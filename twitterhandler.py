from __future__ import print_function

import httplib
from urllib2 import URLError
import logging
import Queue
import multiprocessing
import time

from collections import deque

from twitter.oauth import OAuth
from twitter.stream import TwitterStream
from twitter.api import Twitter, TwitterError, TwitterHTTPError
import tumblpy
import json
import requests

import anagramfunctions
import anagramstats as stats
from anagramstream import AnagramStream


# my twitter OAuth key:
from twittercreds import (CONSUMER_KEY, CONSUMER_SECRET,
                          ACCESS_KEY, ACCESS_SECRET, 
                          BOSS_USERNAME, PRIVATE_POST_URL)
# my tumblr OAuth key:
from tumblrcreds import (TUMBLR_KEY, TUMBLR_SECRET,
                         TOKEN_KEY, TOKEN_SECRET, TUMBLR_BLOG_URL)


class TwitterHandler(object):
    """
    The TwitterHandler object handles non-stream interactions with twitter.
    This includes retrieving specific tweets, posting tweets, and sending dms.
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
            logging.debug('error fetching tweet %i' % tweet_id)
            try:
                if err.e.code == 404:
                    # we reraise 404s, and return false on other exceptions.
                    # 404 means we should not use this resource any more.
                    raise
            except AttributeError:
                pass
            return False
        except Exception as err:
            print('unhandled exception suppressed in fetch_tweet', err)

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
        try:
            t1 = self.fetch_tweet(hit['tweet_one']['tweet_id'])
            t2 = self.fetch_tweet(hit['tweet_two']['tweet_id'])
        except TwitterHTTPError as err:
            print('error posting tweet', err)
            return False
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

    # send a DM to a responsible human
    def send_message(self, message):
        if len(message) > 140:
            message = message[:140]

        self.twitter.direct_messages.new(
            user=BOSS_USERNAME,
            text=message
            )

    def handle_directs(self):
        try:
            dms = self.twitter.direct_messages()
            handled_dm = False
            for d in dms:
                sender = d.get('sender_screen_name')
                if sender == BOSS_USERNAME:
                    if not handled_dm:
                        response = self._private_update_function()
                        self.send_message(response)
                        handled_dm = True
                    self.twitter.direct_messages.destroy(id=d.get("id_str"))
        except URLError as err:
            logging.error(str(err))


    # this is a silly way for me to update my ddns server
    def _private_update_function(self):
        response = requests.get(PRIVATE_POST_URL)
        if response.status_code == 200:
            return "update successful"
        return "update returned response %d" % response.status_code


if __name__ == "__main__":

    t = TwitterHandler()
    t.handle_directs()
