from __future__ import print_function

import httplib
import logging
from threading import Thread
import Queue
from ssl import SSLError
from socket import error as SocketError
from twitter.oauth import OAuth
from twitter.stream import TwitterStream
from twitter.api import Twitter, TwitterError, TwitterHTTPError
import tumblpy

import utils
import time
import sys # just for debug

# my twitter OAuth key:
from twittercreds import (CONSUMER_KEY, CONSUMER_SECRET,
                          ACCESS_KEY, ACCESS_SECRET)
# my tumblr OAuth key:
from tumblrcreds import (TUMBLR_KEY, TUMBLR_SECRET,
                         TOKEN_KEY, TOKEN_SECRET, TUMBLR_BLOG_URL)

# class StreamConnectionError(Exception):
#     """
#     Base Exception thrown when the stream is closed for some reason
#     """
#     pass


class StreamHandler(object):
    """
    handles twitter stream connections. Buffers incoming tweets and
    acts as an iter.
    """
    def __init__(self, buffersize=10000, timeout=30):
        self.buffersize = buffersize
        self.overflow = 0
        self.timeout = timeout
        self.active_time = None
        self.stream_thread = None
        self.Queue = Queue.Queue(maxsize=buffersize)
        self._iter = self.__iter__()

        self.tweets_seen = 0
        self.passed_filter = 0

    def __iter__(self):
        # I think we really want to handle all our various errors and reconection scenarios here
        try:
            while 1:
                try:
                    yield self.Queue.get(True, self.timeout)
                    self.Queue.task_done()
                    continue
                except Queue.Empty:
                    # means we've timed out, and should try to reconnect
                    self.start()
                except SSLError as err:
                    print(err)
                    logging.error(err)
                except TwitterHTTPError as err:
                    print(err)
                    logging.error(err)
                except SocketError as err:
                    print(err)
                    logging.error(err)
        finally:
            print("\nstream handler closing with overflow %i from buffer size %i" %
                  (self.overflow, self.buffersize))

    def next(self):
        return self._iter.next()

    def _run(self):
        stream = TwitterStream(
            auth=OAuth(ACCESS_KEY,
                       ACCESS_SECRET,
                       CONSUMER_KEY,
                       CONSUMER_SECRET),
            api_version='1.1',
            block=True)

        streamiter = stream.statuses.sample(language='en', stall_warnings='true')
        for tweet in streamiter:
            if tweet is not None:
                self._handle_tweet(tweet)

    def _handle_tweet(self, tweet):
        self.tweets_seen += 1
        self.active_time = time.time()
        if self.filter_tweet(tweet):
            self.passed_filter += 1
            try:
                self.Queue.put(format_tweet(tweet))
            except Queue.Full:
                self.overflow += 1

    def buffersize(self):
        return self.Queue.qsize()

    def start(self):
        self.stream_thread = Thread(target=self._run)
        self.stream_thread.daemon = True
        self.stream_thread.start()
        print('created thread %i' % self.stream_thread.ident)

    def filter_tweet(self, tweet):
        """
        filter out anagram-inappropriate tweets
        """
        LOW_CHAR_CUTOFF = 12
        MIN_UNIQUE_CHARS = 8
        #check for mentions
        if len(tweet.get('entities').get('user_mentions')) is not 0:
            return False
        #check for retweets
        if tweet.get('retweeted_status'):
            return False
        # ignore tweets w/ non-ascii characters
        try:
            tweet['text'].decode('ascii')
        except UnicodeEncodeError:
            return False
        # check for links:
        if len(tweet.get('entities').get('urls')) is not 0:
            return False
        # ignore short tweets
        t = utils.stripped_string(tweet['text'])
        if len(t) <= LOW_CHAR_CUTOFF:
            return False
        # ignore tweets with few characters
        st = set(t)
        if len(st) < MIN_UNIQUE_CHARS:
            return False
        return True

    def format_tweet(self, tweet):
        """
        makes a dict from the JSON properties we need
        """

        tweet_id = long(tweet['id_str'])
        tweet_hash = self.make_hash(tweet['text'])
        tweet_text = str(tweet['text'])
        hashed_tweet = {
            'id': tweet_id,
            'hash': tweet_hash,
            'text': tweet_text,
        }
        return hashed_tweet

    def make_hash(self, text):
        """
        takes a tweet as input. returns a character-unique hash
        from the tweet's text.
        """
        t_text = str(utils.stripped_string(text))
        t_hash = ''.join(sorted(t_text, key=str.lower))
        return t_hash


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
        # implementation may change

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
    logging.basicConfig(
    filename='tests/stream.log',
    format='%(asctime)s - %(levelname)s:%(message)s',
    level=logging.DEBUG)

    teststream = StreamHandler()
    teststream.start()
    for t in teststream:
        print("buffer size = %i" % teststream.Queue.qsize(), t.get('text'))
        # pass
