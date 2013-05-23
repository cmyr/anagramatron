from __future__ import print_function

import sys
import re
import time
import logging

from twitterhandler import TwitterHandler
from datahandler import DataHandler, HIT_STATUS_REVIEW
from twitter.api import TwitterHTTPError
import utils

VERSION_NUMBER = 0.6
LOG_FILE_NAME = 'data/anagramer.log'


class AnagramStats(object):
    """
    keeps track of stats for us
    """

    def __init__(self):
        self.tweets_seen = 0
        self.passed_filter = 0
        self.possible_hits = 0
        self.hits = 0
        self.start_time = 0


BASELINE_SKIP_TARGET = 200


class StallWarningHandler(object):
    """
    handles rate warnings sent from twitter when we're falling behind.
    controls the falling_behind flag on Anagramer & stashes skipped tweets
    """

    def __init__(self, delegate):
        self.delegate = delegate
        self.warning = False
        self.skip_target = BASELINE_SKIP_TARGET
        self.skip_count = 0
        self.skipped_tweets = []
        self.reconnecting = False

        # SIMPLE TEST TO SEE IF DELEGATE IS GETTING SET

    def handle_warning(self, warn):
        """
        receive and handle stall warnings
        """
        if self.warning_active():
            self.skip_target *= 2
        else:
            self.skip_target = BASELINE_SKIP_TARGET
        self.warning = {'time': time.time(), 'percent_full': warn.get('percent_full')}
        self.skip_count = 0
        self.delegate.falling_behind = True

    def handle_reconnection(self):
        """
        when reconnecting to a streaming endpoint we'll often receive duplicates
        of tweets we've already seen. This sets skipping and discarding those tweets.
        """
        self.skip_target = BASELINE_SKIP_TARGET
        self.reconnecting = True
        self.delegate.falling_behind = True

    def warning_active(self):
        """
        checks to see if we have an active warning
        """
        if not self.warning:
            return False
        time_since_warning = time.time() - self.warning.get('time')
        if time_since_warning < (6*60):  # warnings sent every five minutes
            # if we get two warnings in five minutes we need to catch up more
                return True
        return False

    def skipped(self, tweet):
        """
        receives a skip tweet and saves it to process later?
        """
        self.skip_count += 1
        if tweet.get('text') and not self.reconnecting:
            self.skipped_tweets.append(tweet)
        if self.skip_count == self.skip_target:
            self.delegate.falling_behind = False
            self.reconnecting = False


class Anagramer(object):
    """
    Anagramer hunts for anagrams on twitter.
    """

    def __init__(self):
        self.twitter_handler = None
        self.stats = AnagramStats()
        self.data = None  #wait until we get run call to load data
        self.stall_handler = StallWarningHandler(self)
        self.falling_behind = False

    def run(self, source=None):
        """
        starts the program's main run-loop
        """
        self.data = DataHandler()
        if not source:
            while 1:
                try:
                    logging.info('entering run loop')
                    self.twitter_handler = TwitterHandler()
                    self.start_stream()
                except KeyboardInterrupt:
                    break
                except TwitterHTTPError as e:
                    print('\n', e)
                    # handle errors probably?
                finally:
                    self.data.finish()
        else:
            # means we're running from local data
            self.run_with_data(source)

    def start_stream(self):
        """
        main run loop
        """
        self.stats.start_time = time.time()
        stream_iterator = self.twitter_handler.stream_iter()
        for tweet in stream_iterator:
            if tweet.get('warning'):
                print('\n', tweet)
                logging.warning(tweet)
                self.stall_handler.handle_warning(tweet)
            if self.falling_behind:
                self.stall_handler.skipped(tweet)
                continue
            if tweet.get('text'):
                self.stats.tweets_seen += 1
                if self.filter_tweet(tweet):
                    self.stats.passed_filter += 1
                    self.update_console()
                    self.process_input(self.format_tweet(tweet))

    def run_with_data(self, data):
        """
        uses a supplied data source instead of a twitter connection (debug)
        """
        self.stats.start_time = time.time()
        for tweet in data:
            self.process_input(tweet)
            # time.sleep(0.0001)
            self.stats.tweets_seen += 1
            self.stats.passed_filter += 1
            self.update_console()
        logging.debug('hits %g matches %g' % (self.stats.possible_hits, self.stats.hits))

    def filter_tweet(self, tweet):
        """
        filter out anagram-inappropriate tweets
        """
        LOW_CHAR_CUTOFF = 10
        MIN_UNIQUE_CHARS = 7
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
        # uniqueness checking:

    def process_input(self, hashed_tweet):
        if self.data.contains(hashed_tweet['hash']):
            self.process_hit(hashed_tweet)
        else:
            self.add_to_data(hashed_tweet)

    def add_to_data(self, hashed_tweet):
        self.data.add(hashed_tweet)

    def process_hit(self, new_tweet):
        """
        called when a duplicate is found, & does difference checking
        """

        hit_tweet = self.data.get(new_tweet['hash'])
        self.stats.possible_hits += 1
        # logging:
        # logging.info(
        #     'possible hit: \n %s %d \n %s %d',
        #     hit_tweet['text'],
        #     hit_tweet['id'],
        #     new_tweet['text'],
        #     new_tweet['id'])
        if not hit_tweet:
            print('error retrieving hit')
            return

        if self.compare(new_tweet['text'], hit_tweet['text']):
            hit = {
                "id": int(time.time()*1000),
                "status": HIT_STATUS_REVIEW,
                "tweet_one": new_tweet,
                "tweet_two": hit_tweet,
            }
            self.data.remove(hit_tweet['hash'])
            self.data.add_hit(hit)
            self.stats.hits += 1
        else:
            pass

    def compare(self, tweet_one, tweet_two):
        """
        most basic test, finds if tweets are just identical
        """
        if not self.compare_chars(tweet_one, tweet_two):
            return False
        if not self.compare_words(tweet_one, tweet_two):
            return False
        return True

    def compare_chars(self, tweet_one, tweet_two, cutoff=0.5):
        """
        basic test, looks for similarity on a char by char basis
        """
        stripped_one = utils.stripped_string(tweet_one)
        stripped_two = utils.stripped_string(tweet_two)

        total_chars = len(stripped_two)
        same_chars = 0
        for i in range(total_chars):
            if stripped_one[i] == stripped_two[i]:
                same_chars += 1

        if (float(same_chars) / total_chars) < cutoff:
            return True
        return False

    def compare_words(self, tweet_one, tweet_two, cutoff=0.5):
        """
        looks for tweets containing the same words in different orders
        """
        words_one = utils.stripped_string(tweet_one, spaces=True).split()
        words_two = utils.stripped_string(tweet_two, spaces=True).split()

        word_count = len(words_one)
        if len(words_two) < len(words_one):
            word_count = len(words_two)

        same_words = 0
        # compare words to each other:
        for word in words_one:
            if word in words_two:
                same_words += 1
        # if more then $CUTOFF words are the same, fail test
        if (float(same_words) / word_count) < cutoff:
            return True
        else:
            return False

    def make_hash(self, text):
        """
        takes a tweet as input. returns a character-unique hash
        from the tweet's text.
        """
        t_text = str(utils.stripped_string(text))
        t_hash = ''.join(sorted(t_text, key=str.lower))
        return t_hash

# displaying data while we run:
    def update_console(self):
        """
        prints various bits of status information to the console.
        """
        # what all do we want to have, here? let's blueprint:
        # tweets seen: $IN_HAS_TEXT passed filter: $PASSED_F% Hits: $HITS
        seen_percent = int(100*(float(
            self.stats.passed_filter)/self.stats.tweets_seen))
        runtime = int(time.time()-self.stats.start_time)

        status = (
            'tweets seen: ' + str(self.stats.tweets_seen) +
            " passed filter: " + str(self.stats.passed_filter) +
            " ({0}%)".format(seen_percent) +
            " hits " + str(self.stats.possible_hits) +
            " agrams: " + str(self.stats.hits) +
            " runtime: " + utils.format_seconds(runtime)
        )
        sys.stdout.write(status + '\r')
        sys.stdout.flush()

    def print_hits(self):
        hits = self.data.get_all_hits()
        for hit in hits:
            print(hit['tweet_one']['text'], hit['tweet_one']['id'])
            print(hit['tweet_two']['text'], hit['tweet_two']['id'])


def main():
    # set up logging:
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        format='%(asctime)s - %(levelname)s:%(message)s',
        level=logging.DEBUG
    )
    anagramer = Anagramer()
    return anagramer.run()


if __name__ == "__main__":
    main()
