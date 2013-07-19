from __future__ import print_function

import httplib
import logging
import Queue
import multiprocessing
import time

import sys
from collections import deque
from ssl import SSLError
from socket import error as SocketError
from urllib2 import HTTPError
from cPickle import UnpickleableError

from twitter.oauth import OAuth
from twitter.stream import TwitterStream
from twitter.api import Twitter, TwitterError, TwitterHTTPError
import tumblpy
import json

import anagramfunctions
import anagramstats as stats
from anagramstream import AnagramStream


# my twitter OAuth key:
from twittercreds import (CONSUMER_KEY, CONSUMER_SECRET,
                          ACCESS_KEY, ACCESS_SECRET)
# my tumblr OAuth key:
from tumblrcreds import (TUMBLR_KEY, TUMBLR_SECRET,
                         TOKEN_KEY, TOKEN_SECRET, TUMBLR_BLOG_URL)

from constants import (ANAGRAM_STREAM_BUFFER_SIZE,
                       ANAGRAM_LOW_CHAR_CUTOFF,
                       ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF)



class StreamHandler(object):
    """
    handles twitter stream connections. Buffers incoming tweets and
    acts as an iter.
    """
    def __init__(self,
                 buffersize=ANAGRAM_STREAM_BUFFER_SIZE,
                 timeout=90,
                 languages=['en']
                 ):
        self.buffersize = buffersize
        self.timeout = timeout
        self.languages = languages
        self.stream_process = None
        self.queue = multiprocessing.Queue()
        self._error_queue = multiprocessing.Queue()
        self._buffer = deque()
        self._should_return = False
        self._iter = self.__iter__()
        self._overflow = multiprocessing.Value('L', 0)
        self._tweets_seen = multiprocessing.Value('L', 0)
        self._passed_filter = multiprocessing.Value('L', 0)
        self._lock = multiprocessing.Lock()
        self._backoff_time = 0

    @property
    def overflow(self):
        return long(self._overflow.value)

    def update_stats(self):
        with self._lock:
            if self._overflow.value:
                stats.overflow(self._overflow.value)
                self._overflow.value = 0
            if self._tweets_seen.value:
                stats.tweets_seen(self._tweets_seen.value)
                self._tweets_seen.value = 0
            if self._passed_filter.value:
                stats.passed_filter(self._passed_filter.value)
                self._passed_filter.value = 0
        stats.set_buffer(self.bufferlength())

    def __iter__(self):
        """
        the connection to twitter is handled in another process
        new tweets are added to self.queue as they arrive.
        on each call to iter we move any tweets in the queue to a fifo buffer
        this makes keeping track of the buffer size a lot cleaner.
        """
        # I think we really want to handle all our various errors and reconection scenarios here
        while 1:
            # first add items from the queue to the buffer
            if self._should_return:
                print('breaking iteration')
                raise StopIteration
            while 1:
                try:
                        self._buffer.append(self.queue.get_nowait())
                except Queue.Empty:
                    break
            try:
                self.update_stats()
                if len(self._buffer):
                    yield self._buffer.popleft()
                    # add elements to buffer from queue:
                else:
                    yield self.queue.get(True, self.timeout)
                    self._backoff_time = 0
                    continue
            except Queue.Empty:
                print('queue timeout, restarting thread')
                # means we've timed out, and should try to reconnect
                self._stream_did_timeout()
        print('exiting iter loop')

    def next(self):
        return self._iter.next()

    def start(self):
        """
        creates a new thread and starts a streaming connection.
        If a thread already exists, it is terminated.
        """
        print('creating new server connection')
        logging.debug('creating new server connection')
        if self.stream_process is not None:
            print('terminating existing server connection')
            logging.debug('terminating existing server connection')
            self.stream_process.terminate()
            if self.stream_process.is_alive():
                pass
            else:
                print('existing thread terminated succesfully')
                logging.debug('thread terminated successfully')

        self.stream_process = multiprocessing.Process(
                                target=self._run,
                                args=(self.queue,
                                      self._error_queue,
                                      self._backoff_time,
                                      self._overflow,
                                      self._tweets_seen,
                                      self._passed_filter,
                                      self._lock,
                                      self.languages))
        self.stream_process.daemon = True
        self.stream_process.start()
        print('created process %i' % self.stream_process.pid)

    def _stream_did_timeout(self):
        """
        check for errors and choose a reconnection strategy.
        see: (https://dev.twitter.com/docs/streaming-apis/connecting#Stalls)
        """
        err = None
        while 1:
            # we could possible have more then one error?
            try:
                err = self._error_queue.get_nowait()
                logging.error('received error from stream process', err)
                print(err, 'backoff time:', self._backoff_time)
            except Queue.Empty:
                break
        if err:
            print(err)
            error_code = err.get('code')

            if error_code == 420:
                if not self._backoff_time:
                    self._backoff_time = 60
                else:
                    self._backoff_time *= 2
            else:
                # a placeholder, for now
                # elif error_code in [400, 401, 403, 404, 405, 406, 407, 408, 410]:
                if not self._backoff_time:
                    self._backoff_time = 5
                else:
                    self._backoff_time *= 2
                if self._backoff_time > 320:
                    self._backoff_time = 320
            # if error_code == 'TCP/IP level network error':
            #     self._backoff_time += 0.25
            #     if self._backoff_time > 16.0:
            #         self._backoff_time = 16.0
        self.start()

    def close(self):
        """
        terminates existing connection and returns
        """
        self._should_return = True
        if self.stream_process:
            self.stream_process.terminate()
        print("\nstream handler closed with overflow %i from buffer size %i" %
              (self.overflow, self.buffersize))
        logging.debug("stream handler closed with overflow %i from buffer size %i" %
              (self.overflow, self.buffersize))

    def bufferlength(self):
        return len(self._buffer)

    def _run(self, queue, errors, backoff_time, overflow, seen, passed, lock, languages):
        """
        handle connection to streaming endpoint.
        adds incoming tweets to queue.
        runs in own process.
        errors is a queue we use to transmit exceptions to parent process.
        """
        # if we've been given a backoff time, sleep
        if backoff_time:
            time.sleep(backoff_time)
        stream = AnagramStream(
            CONSUMER_KEY,
            CONSUMER_SECRET,
            ACCESS_KEY,
            ACCESS_SECRET)

        try:
            stream_iter = stream.stream_iter(languages=languages)
            logging.debug('stream begun')
            for tweet in stream_iter:
                if tweet is not None:
                    try:
                        tweet = json.loads(tweet)
                    except ValueError:
                        pass
                    if tweet.get('warning'):
                        print('\n', tweet)
                        logging.warning(tweet)
                        errors.put(dict(tweet))
                        continue
                    if tweet.get('disconnect'):
                        logging.warning(tweet)
                        errors.put(dict(tweet))
                        continue
                    if tweet.get('text'):
                        with lock:
                            seen.value += 1
                        processed_tweet = anagramfunctions.filter_tweet(tweet)
                        if processed_tweet:
                            with lock:
                                passed.value += 1
                            try:
                                queue.put(processed_tweet, block=False)
                            except Queue.Full:
                                with lock:
                                    overflow.value += 1

        except (HTTPError, SSLError, TwitterHTTPError, SocketError) as err:
            print(type(err))
            print(err)
            error_dict = {'error': str(err), 'code': err.code}
            errors.put(error_dict)


class TwitterHandler(object):
    """
    The TwitterHandler object handles all of the interactions with twitter.
    This includes setting up streams and returning stream iterators, as well
    as handling normal twitter functions such as retrieving specific tweets,
    posting tweets, and sending messages as necessary.
    It also now includes a basic tumblr posting utility function.
    """

    def __init__(self):
        self.stream = TwitterStream(
            auth=OAuth(ACCESS_KEY,
                       ACCESS_SECRET,
                       CONSUMER_KEY,
                       CONSUMER_SECRET),
            api_version='1.1')
        self.twitter = Twitter(
            auth=OAuth(ACCESS_KEY,
                       ACCESS_SECRET,
                       CONSUMER_KEY,
                       CONSUMER_SECRET),
            api_version='1.1')
        self.tmblr = tumblpy.Tumblpy(app_key=TUMBLR_KEY,
                                     app_secret=TUMBLR_SECRET,
                                     oauth_token=TOKEN_KEY,
                                     oauth_token_secret=TOKEN_SECRET
                                     )

    def stream_iter(self):
        """returns a stream iterator."""
        # this is still here because it is ocassionally used for testing.
        # streaming is now handled by StreamHandler.
        return self.stream.statuses.sample(language='en', stall_warnings='true')

    def fetch_tweet(self, tweet_id):
        """
        attempts to retrieve the specified tweet. returns False on failure.
        """
        try:
            tweet = self.twitter.statuses.show(
                id=str(tweet_id),
                include_entities='false')
            return tweet
        except httplib.IncompleteRead as err:
            # print statements for debugging
            logging.debug(err)
            print(err)
            return False
        except TwitterError as err:
            logging.debug(err)
            print(err)
            return False

    def retweet(self, tweet_id):
        try:
            success = self.twitter.statuses.retweet(id=tweet_id)
        except TwitterError as err:
            logging.debug(err)
            return False
        if success:
            return True
        else:
            return False

    def delete_last_tweet(self):
        try:
            tweet = self.twitter.statuses.user_timeline(count="1")[0]
        except TwitterError as err:
            logging.debug(err)
            return False
        try:
            success = self.twitter.statuses.destroy(id=tweet['id_str'])
        except TwitterError as err:
            print(err)
            return False

        if success:
            return True
        else:
            return False

    def url_for_tweet(self, tweet_id):
        tweet = self.fetch_tweet(tweet_id)
        if tweet:
            username = tweet.get('user').get('screen_name')
            return('https://www.twitter.com/%s/status/%s'
                   % (username, str(tweet_id)))
        return False

    def oembed_for_tweet(self, tweet_id):
        return (self.twitter.statuses.oembed(_id=tweet_id))

    def retweet_hit(self, hit):
        """
        handles retweeting a pair of tweets & various possible failures
        """
        if not self.retweet(hit['tweet_one']['tweet_id']):
            return False
        if not self.retweet(hit['tweet_two']['tweet_id']):
            self.delete_last_tweet()
            return False
        return True

    def tumbl_tweets(self, tweetone, tweettwo):
        """
        posts a pair of tweets to tumblr. for url needs real tweet from twitter
        """
        sn1 = tweetone.get('user').get('screen_name')
        sn2 = tweettwo.get('user').get('screen_name')
        oembed1 = self.oembed_for_tweet(tweetone.get('id_str'))
        oembed2 = self.oembed_for_tweet(tweettwo.get('id_str'))
        post_title = "@%s vs @%s" % (sn1, sn2)
        post_content = '<div class="tweet-pair">%s<br /><br />%s</div>' % (oembed1['html'], oembed2['html'])
        post = self.tmblr.post('post',
                               blog_url=TUMBLR_BLOG_URL,
                               params={'type': 'text',
                                       'title': post_title,
                                       'body': post_content
                                       })
        if not post:
            return False
        return True

    def post_hit(self, hit):
        t1 = self.fetch_tweet(hit['tweet_one']['tweet_id'])
        t2 = self.fetch_tweet(hit['tweet_two']['tweet_id'])
        if not t1 or not t2:
            print('failed to fetch tweets')
            # tweet doesn't exist or is unavailable
            # TODO: better error handling here
            return False
        # retewet hits
        if not self.retweet_hit(hit):
            print('failed to retweet hits')
            return False
        if not self.tumbl_tweets(t1, t2):
            # if a tumblr post fails in a forest and nobody etc
            logging.warning('tumblr failed with hit', hit)
        return True


if __name__ == "__main__":
    # listner = AnagramStream()
    # listner._setup_stream()
    count = 0;
    stream = StreamHandler()
    stream.start()

    for t in stream:
        count += 1
        # print(count)

        print(t['tweet_text'], 'buffer length: %i' % len(stream._buffer))
        # if count > 100:
        #     stream.close()
