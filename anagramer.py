from __future__ import print_function

import time
import logging
import cPickle as pickle

from twitterhandler import StreamHandler
from datahandler import DataCoordinator
import anagramstats as stats
from twitterhandler import TwitterHandler, StreamHandler
from datahandler import DataHandler, HIT_STATUS_REVIEW
from constants import ANAGRAM_LOW_CHAR_CUTOFF, ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF
# from twitter.api import TwitterHTTPError
import utils

LOG_FILE_NAME = 'data/anagramer.log'


class NeedsSave(Exception):
    """hacky exception raised when we need to save"""
    # this isn't being used right now, but might be used to implement
    # automated trimming / removal of old tweets from the permanent store
    # when things are getting too slow.
    pass


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
        self.hit_distributions = [0 for x in range(140)]
        self.hash_distributions = [0 for x in range(140)]
        self.hitlist = []

    def new_hash(self, hash_text):
        hashlength = len(hash_text)
        if (hashlength < 140):
            self.hash_distributions[hashlength] += 1

    def new_hit(self, hash_text):
        self.hitlist.append(hash_text)
        hashlength = len(hash_text)
        if (hashlength < 140):
            self.hit_distributions[hashlength] += 1

    def close(self):
        self.end_time = time.time()
        filename = "data/stats/%s.p" % time.strftime("%b%d%H%M")
        pickle.dump(self, open(filename, 'wb'))
        logging.debug('saved stats with %i hits' % len(self.hitlist))


class Anagramer(object):
    """
    Anagramer hunts for anagrams on twitter.
    """

    def __init__(self):
        self.twitter_handler = TwitterHandler()
        self.stream_handler = StreamHandler()
        self.stats = AnagramStats()
        self.data = None  # wait until we get run call to load data
        # self.time_to_save = self.set_save_time()

    def run(self, source=None):
        """
        starts the program's main run-loop
        """
        self.data = DataHandler(delegate=self)
        if not source:
            while 1:
                try:
                    if not self.data:
                        self.data = DataHandler()
                    if not self.stats:
                        self.stats = AnagramStats()
                    if not self.stream_handler:
                        self.stream_handler = StreamHandler()
                    logging.info('entering run loop')
                    self.start_stream()
                except KeyboardInterrupt:
                    break
                except NeedsSave:
                    print('\nclosing stream for scheduled maintenance')
                    # todo: this is where we'd handle pruning etc
                finally:
                    if self.stream_handler:
                        self.stream_handler.close()
                        self.stream_handler = None
                    self.data.finish()
                    self.data = None
                    self.stats.close()
                    self.stats = None
        else:
            # means we're running from local data
            self.run_with_data(source)

    def start_stream(self):
        """
        main run loop
        """
        self.stats.start_time = time.time()
        self.stream_handler.start()
        for tweet in self.stream_handler:
            self._process_input(tweet)
            self.update_console()

    def run_with_data(self, data):
        """
        uses a supplied data source instead of a twitter connection (debug)
        """
        self.stats.start_time = time.time()
        self.stream_handler.start(source=data)
        # for tweet in data:
        #     self.process_input(tweet)
        #     # time.sleep(0.0001)
        #     self.stats.tweets_seen += 1
        #     self.stats.passed_filter += 1
        #     self.update_console()

        logging.debug('hits %g matches %g' % (self.stats.possible_hits, self.stats.hits))
        self.data.finish()

    def _process_input(self, tweet):
        self.stats.tweets_seen += 1
        if self.filter_tweet(tweet):
            self.stats.passed_filter += 1
            hashed_tweet = self.format_tweet(tweet)
            self.stats.new_hash(hashed_tweet['hash'])
            self.data.process_tweet(hashed_tweet)

    def filter_tweet(self, tweet):
        """
        filter out anagram-inappropriate tweets
        """
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
        if len(t) <= ANAGRAM_LOW_CHAR_CUTOFF:
            return False
        # ignore tweets with few characters
        st = set(t)
        if len(st) <= ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF:
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

    def process_hit(self, tweet_one, tweet_two):
        """
        called by datahandler when it has found a match in need of review.
        """
        self.stats.possible_hits += 1
        self.stats.new_hit(tweet_one['hash'])
        if self.compare(tweet_one['text'], tweet_two['text']):
            hit = {
                "id": int(time.time()*1000),
                "status": HIT_STATUS_REVIEW,
                "tweet_one": tweet_one,
                "tweet_two": tweet_two,
            }
            self.data.remove(tweet_one['hash'])
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

    def check_save(self):
        """check if it's time to save and save if necessary"""
        if (time.time() > self.time_to_save):
            self.time_to_save = self.set_save_time()
            raise NeedsSave

# displaying data while we run:
    def update_console(self):
        """
        prints various bits of status information to the console.
        """
        # what all do we want to have, here? let's blueprint:
        # tweets seen: $IN_HAS_TEXT passed filter: $PASSED_F% Hits: $HITS
        seen_percent = int(100*(float(
            self.stats.passed_filter)/self.stats.tweets_seen))
        runtime = time.time()-self.stats.start_time

        status = (
            'tweets seen: ' + str(self.stats.tweets_seen) +
            " passed filter: " + str(self.stats.passed_filter) +
            " ({0}%)".format(seen_percent) +
            " hits " + str(self.stats.possible_hits) +
            " agrams: " + str(self.stats.hits) +
            " buffer: " + str(self.stream_handler.bufferlength()) +
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

    stream_handler = StreamHandler()
    data_coordinator = DataCoordinator()
    server_process = None
    # server_process = subprocess.call('python hit_server.py')

    while 1:
        try:
            logging.info('entering run loop')
            stats.start_time = time.time()
            stream_handler.start()
            for processed_tweet in stream_handler:
                data_coordinator.handle_input(processed_tweet)
                stats.update_console()
        except KeyboardInterrupt:
            break
        finally:
            if server_process:
                server_process.terminate()
            stream_handler.close()
            stream_handler = None
            data_coordinator.close()
            stats.close()


def test(source, raw=True):
    stats._clear_stats()
    data_coordinator = DataCoordinator()
    for tweet in source:
        stats.tweets_seen()
        if raw:
            processed_tweet = filter_tweet(tweet)
            if processed_tweet:
                stats.passed_filter()
                data_coordinator.handle_input(processed_tweet)
        else:
            tweet_text = tweet.get('text') or tweet.get('tweet_text')
            tweet_id = tweet.get('id') or tweet.get('tweet_id')

            tweet_text = _correct_encodings(tweet_text)
            tweet = {'tweet_hash': improved_hash(tweet_text),
                     'tweet_id': tweet_id,
                     'tweet_text': tweet_text
                               }

            stats.passed_filter()
            data_coordinator.handle_input(tweet)

        stats.update_console()
    data_coordinator.close()


if __name__ == "__main__":
    main()
    # source = pickle.load(open('testdata/tst2.p', 'r'))
    # source = pickle.load(open('tstdata/20ktst1.p'))
    # test(source, False)
    # db_conversion_utility()
