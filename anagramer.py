from __future__ import print_function

import sys
import re
import time
import logging
import cPickle as pickle

from twitterhandler import TwitterHandler, StreamHandler
from datahandler import DataHandler, HIT_STATUS_REVIEW
import anagramstats as stats
# from twitter.api import TwitterHTTPError
import utils

from constants import ANAGRAM_LOW_CHAR_CUTOFF, ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF
LOG_FILE_NAME = 'data/anagramer.log'


class NeedsSave(Exception):
    """hacky exception raised when we need to save"""
    # this isn't being used right now, but might be used to implement
    # automated trimming / removal of old tweets from the permanent store
    # when things are getting too slow.
    pass

# class Anagramer(object):
#     """
#     Anagramer hunts for anagrams on twitter.
#     """

#     def __init__(self):
#         self.twitter_handler = TwitterHandler()
#         self.stream_handler = StreamHandler()
#         # self.stats = AnagramStats()
#         self.data = None  # wait until we get run call to load data
#         # self.time_to_save = self.set_save_time()

#     def run(self, source=None):
#         """
#         starts the program's main run-loop
#         """
#         self.data = DataHandler(delegate=self)
#         if not source:
#             while 1:
#                 try:
#                     if not self.data:
#                         self.data = DataHandler()
#                     # if not self.stats:
#                     #     self.stats = AnagramStats()
#                     if not self.stream_handler:
#                         self.stream_handler = StreamHandler()
#                     logging.info('entering run loop')
#                     self.start_stream()
#                 except KeyboardInterrupt:
#                     break
#                 except NeedsSave:
#                     print('\nclosing stream for scheduled maintenance')
#                     # todo: this is where we'd handle pruning etc
#                 finally:
#                     self.stream_handler.close()
#                     self.stream_handler = None
#                     self.data.finish()
#                     self.data = None
#                     # self.stats.close()
#                     # self.stats = None
#         else:
#             # means we're running from local data
#             self.run_with_data(source)

#     def start_stream(self):
#         """
#         main run loop
#         """
#         # self.stats.start_time = time.time()
#         self.stream_handler.start()
#         for tweet in self.stream_handler:
#             self.update_console()
#             self.process_input(tweet)

#     def run_with_data(self, data):
#         """
#         uses a supplied data source instead of a twitter connection (debug)
#         """
#         # self.stats.start_time = time.time()
#         self.stream_handler.start(source=data)
#         # for tweet in data:
#         #     self.process_input(tweet)
#         #     # time.sleep(0.0001)
#         #     self.stats.tweets_seen += 1
#         #     self.stats.passed_filter += 1
#         #     self.update_console()

#         # logging.debug('hits %g matches %g' % (self.stats.possible_hits, self.stats.hits))
#         self.data.finish()

#     def process_input(self, hashed_tweet):
#         anagramstats.new_hash(hashed_tweet['hash'])
#         self.data.process_tweet(hashed_tweet)

#     def process_hit(self, tweet_one, tweet_two):
#         """
#         called by datahandler when it has found a match in need of review.
#         """
#         anagramstats.possible_hits += 1
#         anagramstats.new_hit(tweet_one['hash'])
#         if self.compare(tweet_one['text'], tweet_two['text']):
#             hit = {
#                 "id": int(time.time()*1000),
#                 "status": HIT_STATUS_REVIEW,
#                 "tweet_one": tweet_one,
#                 "tweet_two": tweet_two,
#             }
#             self.data.remove(tweet_one['hash'])
#             self.data.add_hit(hit)
#             anagramstats.hits += 1
#         else:
#             pass

#     def compare(self, tweet_one, tweet_two):
#         """
#         most basic test, finds if tweets are just identical
#         """
#         if not self.compare_chars(tweet_one, tweet_two):
#             return False
#         if not self.compare_words(tweet_one, tweet_two):
#             return False
#         return True

#     def compare_chars(self, tweet_one, tweet_two, cutoff=0.5):
#         """
#         basic test, looks for similarity on a char by char basis
#         """
#         stripped_one = utils.stripped_string(tweet_one)
#         stripped_two = utils.stripped_string(tweet_two)

#         total_chars = len(stripped_two)
#         same_chars = 0
#         for i in range(total_chars):
#             if stripped_one[i] == stripped_two[i]:
#                 same_chars += 1

#         if (float(same_chars) / total_chars) < cutoff:
#             return True
#         return False

#     def compare_words(self, tweet_one, tweet_two, cutoff=0.5):
#         """
#         looks for tweets containing the same words in different orders
#         """
#         words_one = utils.stripped_string(tweet_one, spaces=True).split()
#         words_two = utils.stripped_string(tweet_two, spaces=True).split()

#         word_count = len(words_one)
#         if len(words_two) < len(words_one):
#             word_count = len(words_two)

#         same_words = 0
#         # compare words to each other:
#         for word in words_one:
#             if word in words_two:
#                 same_words += 1
#         # if more then $CUTOFF words are the same, fail test
#         if (float(same_words) / word_count) < cutoff:
#             return True
#         else:
#             return False

#     def check_save(self):
#         """check if it's time to save and save if necessary"""
#         if (time.time() > self.time_to_save):
#             self.time_to_save = self.set_save_time()
#             raise NeedsSave

#     # displaying data while we run:
#     def update_console(self):
#         """
#         prints various bits of status information to the console.
#         """
#     info = anagramstats.stats_dict()
#     # what all do we want to have, here? let's blueprint:
#     # tweets seen: $IN_HAS_TEXT passed filter: $PASSED_F% Hits: $HITS
#     seen_percent = int(100*(float(info['passed_filter'])/info['tweets_seen']))
#     runtime = time.time()-info['start_time']

#     status = (
#         'tweets seen: ' + str(info['tweets_seen']) +
#         " passed filter: " + str(info['passed_filter']) +
#         " ({0}%)".format(seen_percent) +
#         " hits " + str(info['possible_hits']) +
#         " agrams: " + str(info['hits']) +
#         # " buffer: " + str(self.stream_handler.bufferlength()) +
#         " runtime: " + utils.format_seconds(runtime)
#     )
#     sys.stdout.write(status + '\r')
#     sys.stdout.flush()

#         # # what all do we want to have, here? let's blueprint:
#         # # tweets seen: $IN_HAS_TEXT passed filter: $PASSED_F% Hits: $HITS
#         # seen_percent = int(100*(float(
#         #     self.stream_handler.passed_filter)/self.stream_handler.tweets_seen))
#         # runtime = time.time()-self.stats.start_time

#         # status = (
#         #     'tweets seen: ' + str(self.stream_handler.tweets_seen) +
#         #     " passed filter: " + str(self.stream_handler.passed_filter) +
#         #     " ({0}%)".format(seen_percent) +
#         #     " hits " + str(self.stats.possible_hits) +
#         #     " agrams: " + str(self.stats.hits) +
#         #     " buffer: " + str(self.stream_handler.bufferlength()) +
#         #     " runtime: " + utils.format_seconds(runtime)
#         # )
#         # sys.stdout.write(status + '\r')
#         # sys.stdout.flush()

#     # def print_hits(self):
#     #     hits = self.data.get_all_hits()
#     #     for hit in hits:
#     #         print(hit['tweet_one']['text'], hit['tweet_one']['id'])
#     #         print(hit['tweet_two']['text'], hit['tweet_two']['id'])

# def update_console():
#     info = stats.stats_dict()
#     seen_percent = 0
#     if info.get('tweets_seen') > 0:
#         seen_percent = int(100*(float(info['passed_filter'])/info['tweets_seen']))
#     runtime = time.time()-info['start_time']

#     status = (
#         'tweets seen: ' + str(info['tweets_seen']) +
#         " passed filter: " + str(info['passed_filter']) +
#         " ({0}%)".format(seen_percent) +
#         " hits " + str(info['possible_hits']) +
#         " agrams: " + str(info['hits']) +
#         # " buffer: " + str(self.stream_handler.bufferlength()) +
#         " runtime: " + utils.format_seconds(runtime)
#     )
#     sys.stdout.write(status + '\r')
#     sys.stdout.flush()

# def process_hit(self, tweet_one, tweet_two):
#     """
#     called by datahandler when it has found a match in need of review.
#     """
#     anagramstats.possible_hits += 1
#     anagramstats.new_hit(tweet_one['hash'])
#     if self.compare(tweet_one['text'], tweet_two['text']):
#         hit = {
#             "id": int(time.time()*1000),
#             "status": HIT_STATUS_REVIEW,
#             "tweet_one": tweet_one,
#             "tweet_two": tweet_two,
#         }
#         self.data.remove(tweet_one['hash'])
#         self.data.add_hit(hit)
#         anagramstats.hits += 1
#     else:
#         pass


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


def filter_tweet(tweet):
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


def format_tweet(tweet):
    """
    makes a dict from the JSON properties we want
    converts &amp; &lt; etc to &, <.
    """
    text = tweet['text']
    # text = re.sub(r'&amp;', '&', text).lower()
    # this needs testing guy

    tweet_id = long(tweet['id_str'])
    tweet_hash = make_hash(tweet['text'])
    tweet_text = tweet['text']
    hashed_tweet = {
        'id': tweet_id,
        'hash': tweet_hash,
        'text': tweet_text,
    }
    return hashed_tweet


def make_hash(text):
    """
    takes a tweet as input. returns a character-unique hash
    from the tweet's text.
    """
    t_text = str(utils.stripped_string(text))
    t_hash = ''.join(sorted(t_text, key=str.lower))
    return t_hash


def correct_encodings(text):
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)

import unicodedata

def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')

def text_contains_tricky_chars(text):
    if re.search(ur'[\u0080-\u024F]', text):
        return True
    return False

def text_contains_html_entities(text):
    if re.search(r'&amp;', text):
        return True
    if re.search(r'&lt;', text):
        return True
    if re.search(r'&gt;', text):
        return True
    return False


def text_decodes_to_ascii(text):
    try:
        text.decode('ascii')
    except UnicodeEncodeError:
        return False
    return True

def process_input(tweet):
    stats.tweets_seen()
    # so lots of stuff to do here.
# we want to move away from really thinking abou ascii very much.
# we want to respect unicode, and not universally try to decode it.
# basically we want most 'weird' characters to just be ignored
# but maybe critically we would like for this whole 'process input' thing to happen *before*
# we filter? because we want to use it as a basis, in part, for filtering?
# concerns: we don't want to be introducing characters to the hash that aren't in the tweets.
    tweet_text = tweet.get('text')

    # do the basic filters based on entities etc.
    # then substitute amps lts and gts
    # then check if we decode to ascii
    # if we don't, unidecode questionable characters
    # then check how many non standard characters we have, and filter based on that.

    # so maybe we're back to just doing some accent chopping?
    


def main():
    # set up logging:
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        format='%(asctime)s - %(levelname)s:%(message)s',
        level=logging.DEBUG
    )

    stream_handler = StreamHandler()

    while 1:
        try:
            logging.info('entering run loop')
            stats.start_time = time.time()
            stream_handler.start()
            for tweet in stream_handler:
                process_input(tweet)
                stats.update_console()

        except KeyboardInterrupt:
            break
        finally:
            stream_handler.close()
            stream_handler = None
            stats.close()
            # stats = None

if __name__ == "__main__":
    main()
