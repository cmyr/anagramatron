from __future__ import print_function

import sys
import re
import time
import string
import cPickle as pickle
import logging

from twitterhandler import TwitterHandler
from datahandler import DataHandler
from twitter.api import TwitterHTTPError

# import comptests

VERSION_NUMBER = 0.54
DATA_FILE_NAME = 'data/data' + str(VERSION_NUMBER) + '.p'
HITS_FILE_NAME = 'data/hits' + str(VERSION_NUMBER) + '.p'
# BLACKLIST_FILE_NAME = 'data/blacklist.p'
LOG_FILE_NAME = 'data/anagramer.log'

# possible database sources:
# http://yserial.sourceforge.net/
# http://buzhug.sourceforge.net/

# TODO: PERSISTENT TRACKING / SCORING OF ANAGRAMS THAT WE'VE ACTUALLY SEEN, PLEASE

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

class Anagramer(object):
    """
    Anagramer hunts for anagrams on twitter. 
    """

    def __init__(self):
        self.twitter_handler = None
        self.stats = AnagramStats()
        self.data = DataHandler()
        # self.hits = []
        self.activity_time = 0

    def run(self, source=None):
        """
        starts the program's main run-loop
        """
        if not source:
            while 1:
                try:
                    logging.info('entering run loop')
                    # self.load()
                    if self.hits:
                        logging.info('printing %g hits', len(self.hits))
                        self.print_hits()
                    self.twitter_handler = TwitterHandler()
                    self.start_stream()
                except KeyboardInterrupt:
                    self.data.finish()
                    self.save()
                    break
                except TwitterHTTPError as e:
                    print('\n', e)
                    # handle errors probably?
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
            self.activity_time = time.time()
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
        for tweet in data:
            self.process_input(tweet)
            time.sleep(0.001)
            self.stats.tweets_seen += 1
            self.stats.passed_filter += 1
            self.update_console()

    def filter_tweet(self, tweet):
        """
        filter out anagram-inappropriate tweets
        """
        LOW_CHAR_CUTOFF = 10
        MIN_UNIQUE_CHARS = 7
        # pass_flag = True
        
        # filter non-english tweets
        if tweet.get('lang') != 'en':
            return False
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
        t = self.stripped_string(tweet['text'])
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
        if self.data.get(hashed_tweet['hash']):
            self.process_hit(hashed_tweet)
        else:
            self.add_to_data(hashed_tweet)

    def add_to_data(self, hashed_tweet):
        self.data.add(hashed_tweet)

    def process_hit(self, new_tweet):
        """
        called when a duplicate is found, & does difference checking
        """

        hit_tweet = self.data.pop(new_tweet['hash'])
        self.stats.possible_hits += 1
        # logging: 
        logging.info(
            'possible hit: \n %s %d \n %s %d',
            hit_tweet['text'],
            hit_tweet['id'],
            new_tweet['text'],
            new_tweet['id'])
        if not hit_tweet:
            print('error retrieving hit')
            return

        if self.compare(new_tweet['text'], hit_tweet['text']):
            hit = {
                "id": int(time.time()*1000),
                "tweet_one": new_tweet,
                "tweet_two": hit_tweet,
            }
            self.data.add_hit(hit)
            self.stats.hits += 1
        else:
            self.add_to_data(new_tweet)

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
        stripped_one = self.stripped_string(tweet_one)
        stripped_two = self.stripped_string(tweet_two)

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
        words_one = self.stripped_string(tweet_one, spaces=True).split()
        words_two = self.stripped_string(tweet_two, spaces=True).split()
           
        word_count = len(words_one)
        if len(words_two) < len(words_one): word_count = len(words_two)

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
        t_text = str(re.sub(r'[^a-zA-Z]', '', text).lower())
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
            # " ({0:.2f}%)".format(seen_percent)
            " ({0}%)".format(seen_percent) +
            " hits " + str(self.stats.possible_hits) +
            " agrams: " + str(self.stats.hits) +
            " runtime: " + self.format_seconds(runtime)
        )
        sys.stdout.write(status + '\r')
        sys.stdout.flush()

    def print_hits(self):
        hits = self.data.get_all_hits()
        for hit in hits:
            print(hit['tweet_one']['text'], hit['tweet_one']['id'])
            print(hit['tweet_two']['text'], hit['tweet_two']['id'])

# helper methods

    def stripped_string(self, text, spaces=False):
        """
        returns lower case string with all non alpha chars removed
        """
        if spaces:
            return re.sub(r'[^a-zA-Z]', ' ', text).lower()
        return re.sub(r'[^a-zA-Z]', '', text).lower()

    def format_seconds(self, seconds):
        """
        yea fine this is bad deal with it
        """
        DAYSECS = 86400
        HOURSECS = 3600
        MINSECS = 60
        dd = hh = mm = ss = 0

        dd = seconds / DAYSECS
        seconds = seconds % DAYSECS
        hh = seconds / HOURSECS
        seconds = seconds % HOURSECS
        mm = seconds / MINSECS
        seconds = seconds % MINSECS
        ss = seconds
        time_string = str(mm)+'m ' + str(ss) + 's'
        if hh or dd:
            time_string = str(hh) + 'h ' + time_string
        if dd:
            time_string = str(dd) + 'd ' + time_string
        return time_string

def main():
    # set up logging:
    logging.basicConfig(
    filename=LOG_FILE_NAME,
    format='%(levelname)s:%(message)s',
    level=logging.DEBUG
    )
    anagramer = Anagramer()
    
    # testdata = pickle.load(open('testdata/archive0.p', 'r'))
    # return anagramer.run(source=testdata)
    return anagramer.run()

if __name__ == "__main__":
    main()
