from __future__ import print_function

import sys, httplib
from time import sleep

from twitter.oauth import OAuth
from twitter.stream import TwitterStream
from twitter.api import Twitter, TwitterError

# my twitter OAuth key:
from twittercreds import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET;


# username to send alerts to
BOSS_USERNAME = "cmyr"

class TweetUnavailableError(TwitterError):
    """the requested tweet cannot be accessed"""
    pass


class TwitterRateLimitError(TwitterError):
    """
    rate limit exceeded, back off
    """
    pass


class TwitterHandler(object):
    """
    The TwitterHandler object handles all of the interactions with twitter.
    This includes setting up streams and returning stream iterators, as well
    as handling normal twitter functions such as retrieving specific tweets, 
    posting tweets, and sending messages as necessary. 
    """

    def __init__(self):
        self.stream = TwitterStream(
            auth=OAuth(ACCESS_KEY,
                ACCESS_SECRET,
                CONSUMER_KEY,
                CONSUMER_SECRET),
            api_version='1.1',
            secure='False')
        self.twitter = Twitter(
            auth=OAuth(ACCESS_KEY,
                ACCESS_SECRET,
                CONSUMER_KEY,
                CONSUMER_SECRET),
            api_version='1.1')

    def stream_iter(self):
        """returns a stream iterator."""
        # implementation may change

        return self.stream.statuses.sample()

    def fetch_tweet(self, tweet_id):
        """
        attempts to retrieve the specified tweet. returns None on failure.
        """
        unavailable_tweet_codes = [403, 404]
        rate_limit_code = 429
        try:
            tweet = self.twitter.statuses.show(
                id=str(tweet_id),
                include_entities='false')
            return tweet
        except httplib.IncompleteRead, e:
            # print statements for debugging
            print('{0:*^60}'.format('INCOMPLETE READ'))
            print(tweet_id)
            print(e)
            print('{0:*^60}'.format(''))
            return None
        except TwitterError as err:
            if err.e.code:
                if err.e.code in unavailable_tweet_codes:
                    print('unavilable tweet error \n')
                    raise TweetUnavailableError
                elif err.e.code == rate_limit_code:
                    print('twitter rate limit error \n')
                    raise TwitterRateLimitError
                else:
                    print('\n')
                    print('{0:*^60}'.format('TWITTER ERROR'))
                    print(tweet_id)
                    print(err)
                    print('{0:*^60}'.format(''))
                    return None
            else:
                print('\n')
                print('{0:*^60}'.format('TWITTER ERROR'))
                print(tweet_id)
                print(err)
                print('{0:*^60}'.format(''))
                return None

    def retweet(self, tweet_id):
        try:
            success = self.twitter.statuses.retweet(id=tweet_id)
        except TwitterError as err:
            print(err)
            return False

        if success:
            return True
        else:
            return False

    def delete_last_tweet(self):
        try:
            tweet = self.twitter.statuses.user_timeline(count="1")[0]
        except TwitterError as Err:
            print(err)
            print("unable to fetch tweet for deletion")
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

    def send_msg(self, msg):
        """
        attempts to send a DM to a globally defined twitter username.
        returns True on success, False on failure.
        """
        try:
            success = self.twitter.direct_messages.new(
            user=BOSS_USERNAME,
            text=msg)
        except TwitterError as err:
            print(err)
            return False
        if success:
            return True
        else:
            return False

def main():
    return 0

if __name__ == "__main__":
    main()