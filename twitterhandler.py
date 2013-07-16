from __future__ import print_function

import httplib
import logging
import Queue
import multiprocessing
import time
from collections import deque
from ssl import SSLError
from socket import error as SocketError

from twitter.oauth import OAuth
from twitter.stream import TwitterStream
from twitter.api import Twitter, TwitterError, TwitterHTTPError
import tumblpy

import utils
# import anagramstats as stats


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
    def __init__(self, buffersize=ANAGRAM_STREAM_BUFFER_SIZE, timeout=30, languages=['en']):
        self.buffersize = buffersize
        self.timeout = timeout
        self.languages = languages
        self.stream_process = None
        self.queue = multiprocessing.Queue()
        self._buffer = deque()
        self._should_return = False
        self._iter = self.__iter__()
        self._overflow = multiprocessing.Value('L', 0)
        self._lock = multiprocessing.Lock()

    @property
    def overflow(self):
        return long(self._overflow.value)

    # def update_stats(self):
    #     with self._lock:
    #         if self._overflow.value:
    #             stats.overflow(self._overflow.value)
    #             self._overflow.value = 0
    #     stats.set_buffer(self.bufferlength())

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
            # while 1:
                # try:
                #     t = self.queue.get_nowait()
                #     if t.get('text'):
                #         self._buffer.append(t)
                # except Queue.Empty:
                #     break
            if self._should_return:
                raise StopIteration
            try:
                # self.update_stats()
                if len(self._buffer):
                    yield self._buffer.popleft()
                    # add elements to buffer from queue:
                else:
                    yield self.queue.get(True, self.timeout)
                    continue
            except Queue.Empty:
                print('queue timeout, restarting thread')
                # means we've timed out, and should try to reconnect
                self.start()
        print('exiting iter loop')

    def next(self):
        return self._iter.next()

    # def _run(self, queue, stop_flag, seen, passed, overflow, lock):
    def _run(self, queue, overflow, lock, languages):
        stream = TwitterStream(
            auth=OAuth(ACCESS_KEY,
                       ACCESS_SECRET,
                       CONSUMER_KEY,
                       CONSUMER_SECRET),
            api_version='1.1',
            block=True)

        langs = None
        if languages:
            langs = ','.join(languages)

        try:
            if langs:
                streamiter = stream.statuses.sample(language=langs, stall_warnings='true')
            else:
                streamiter = stream.statuses.sample(stall_warnings='true')
            logging.debug('stream begun')
            for tweet in streamiter:
                if tweet is not None:
                    if tweet.get('warning'):
                        print('\n', tweet)
                        logging.warning(tweet)
                        continue
                    if tweet.get('text'):
                        try:
                            queue.put(dict(tweet), block=False)
                        except Queue.Full:
                            with lock:
                                overflow.value += 1
        except SSLError as err:
            print(err)
            logging.error(err)
            return
        except TwitterHTTPError as err:
            print(err)
            logging.error(err)
            return
        except SocketError as err:
            print(err)
            logging.error(err)
            return

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
        # self._process_should_end.clear()
        self.stream_process = multiprocessing.Process(
                                target=self._run,
                                args=(self.queue,
                                      self._overflow,
                                      self._lock,
                                      self.languages))
        self.stream_process.daemon = True
        self.stream_process.start()
        self._should_return = False

        print('created process %i' % self.stream_process.pid)

    def close(self):
        """
        terminates existing connection and returns
        """
        self._should_return = True
        if self.stream_process:
            self.stream_process.terminate()
        print("\nstream handler closing with overflow %i from buffer size %i" %
              (self.overflow, self.buffersize))
        logging.debug("stream handler closing with overflow %i from buffer size %i" %
              (self.overflow, self.buffersize))

    def bufferlength(self):
        return len(self._buffer)


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
        if not self.retweet(hit['tweet_one']['id']):
            return False
        if not self.retweet(hit['tweet_two']['id']):
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
        t1 = self.fetch_tweet(hit['tweet_one']['id'])
        t2 = self.fetch_tweet(hit['tweet_two']['id'])
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
    count = 0;
    stream = StreamHandler()
    stream.start()

    for t in stream:
        count += 1
        print(count)
        if t.get('text'):
            print('buffer length: %i' % len(stream._buffer))
        if count > 100:
            stream.close()
