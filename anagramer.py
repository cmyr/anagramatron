from __future__ import print_function

import sys
import re
import time
import logging
import cPickle as pickle
import unicodedata
import subprocess

from twitterhandler import TwitterHandler, StreamHandler
from datahandler import DataCoordinator
import anagramstats as stats
# from twitter.api import TwitterHTTPError
import utils

from constants import (ANAGRAM_LOW_CHAR_CUTOFF, ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF,
                       ENGLISH_LETTER_FREQUENCIES)

ENGLISH_LETTER_LIST = sorted(ENGLISH_LETTER_FREQUENCIES.keys(),
                             key=lambda t: ENGLISH_LETTER_FREQUENCIES[t])

LOG_FILE_NAME = 'data/anagramer.log'

def filter_tweet_old(tweet):
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
    return format_tweet(tweet)


# def format_tweet(tweet):
#     """
#     makes a dict from the JSON properties we want
#     """
#     text = tweet['text']
#     # text = re.sub(r'&amp;', '&', text).lower()
#     # this needs testing guy

#     tweet_id = long(tweet['id_str'])
#     tweet_hash = make_hash(tweet['text'])
#     tweet_text = tweet['text']
#     hashed_tweet = {
#         'tweet_id': tweet_id,
#         'tweet_hash': tweet_hash,
#         'tweet_text': tweet_text,
#     }
#     return hashed_tweet


# def make_hash(text):
#     """
#     takes a tweet as input. returns a character-unique hash
#     from the tweet's text.
#     """
#     t_text = str(utils.stripped_string(text))
#     t_hash = ''.join(sorted(t_text, key=str.lower))
#     return t_hash

freqsort = ENGLISH_LETTER_FREQUENCIES


def improved_hash(text, debug=False):
    """
    only very *minorly* improved. sorts based on letter frequencies.
    """
    CHR_COUNT_START = 64  # we convert to chars; char 65 is A
    if debug: print(text)
    t_text = utils.stripped_string(text)
    if debug: print(t_text)
    t_hash = ''.join(sorted(t_text, key=lambda t: freqsort[t]))
    if debug: print(t_hash)
    letset = set(t_hash)
    if debug: print(letset)
    break_letter = t_hash[-1:]
    if break_letter not in ENGLISH_LETTER_LIST:
        break_letter = ENGLISH_LETTER_LIST[-1]
    if debug: print('breaking on: %s' % break_letter)
    compressed_hash = ''
    for letter in ENGLISH_LETTER_LIST:
        if letter in letset:
            count = len(re.findall(letter, t_hash))
            count = (count if count < 48 else 48)
            # this is a hacky way of sanity checking our values.
            # if this shows up as a match we'll ignore it
            if debug: print('%s in letset %i times' % (letter, count))
            compressed_hash += chr(count + CHR_COUNT_START)
        else:
            if freqsort[letter] > freqsort[break_letter]:
                if debug: print('broke on: %s' % letter)
                if len(compressed_hash) % 2:
                    # an uneven number of bytes will cause unicode errors
                    compressed_hash += chr(64)
                break
            compressed_hash += chr(64)

    if len(compressed_hash) == 0:
        print('hash length is zero?')
        return '@@'
    return compressed_hash
    # return t_hash


def _correct_encodings(text):
    """
    twitter auto converts &, <, > to &amp; &lt; &gt;
    """
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    return text


def _strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def _text_contains_tricky_chars(text):
    if re.search(ur'[\u0080-\u024F]', text):
        return True
    return False


def _text_decodes_to_ascii(text):
    try:
        text.decode('ascii')
    except UnicodeEncodeError:
        return False
    return True


def _basic_filters(tweet):
    if len(tweet.get('entities').get('user_mentions')) is not 0:
        return False
    #check for retweets
    if tweet.get('retweeted_status'):
        return False
    # check for links:
    if len(tweet.get('entities').get('urls')) is not 0:
        return False
    t = utils.stripped_string(tweet['text'])
    if len(t) <= ANAGRAM_LOW_CHAR_CUTOFF:
        return False
    # ignore tweets with few characters
    st = set(t)
    if len(st) <= ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF:
        return False
    return True


def _low_letter_ratio(text, cutoff=0.8):
    t = re.sub(r'[^a-zA-Z .,!?"\']', '', text)
    if (float(len(t)) / len(text)) < cutoff:
        return True
    return False


# def print_low_letter_ratio(text, cutoff=0.8):
#     t = re.sub(r'[^a-zA-Z .,!?"\']', '', text)
#     return (float(len(t)) / len(text))


def filter_tweet(tweet):
    """
    filters out anagram-inappropriate tweets.
    Returns the original tweet object and cleaned tweet text on success.
    """
    if not _basic_filters(tweet):
        return False

    tweet_text = _correct_encodings(tweet.get('text'))
    if not _text_decodes_to_ascii(tweet_text):
        # check for latin chars:
        if _text_contains_tricky_chars(tweet_text):
            tweet_text = _strip_accents(tweet_text)

    if _low_letter_ratio(tweet_text):
        return False

    return {'tweet_hash': improved_hash(tweet_text),
            'tweet_id': long(tweet['id_str']),
            'tweet_text': tweet_text
            }


def main():
    # set up logging:
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        format='%(asctime)s - %(levelname)s:%(message)s',
        level=logging.DEBUG
    )

    stream_handler = StreamHandler()
    data_coordinator = DataCoordinator()
    # server_process = subprocess.call('python hit_server.py')

    while 1:
        try:
            logging.info('entering run loop')
            stats.start_time = time.time()
            stream_handler.start()
            for tweet in stream_handler:
                stats.tweets_seen()
                processed_tweet = filter_tweet(tweet)
                if processed_tweet:
                    # print(processed_tweet)
                    stats.passed_filter()
                    data_coordinator.handle_input(processed_tweet)
                stats.update_console()
        except KeyboardInterrupt:
            break
        finally:
            if server_process:
                server_process.terminate()
            stream_handler.close()
            stream_handler = None
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

def db_conversion_utility():
    import sqlite3 as lite
    olddb = lite.connect('data/tweets.db.bak')
    newdb = lite.connect('data/anagramdataen.db')
    newcurs = newdb.cursor()
    newcurs.execute(
        "CREATE TABLE tweets(tweet_hash TEXT PRIMARY KEY ON CONFLICT REPLACE, tweet_id INTEGER, tweet_text TEXT)"
            )

    print('converting dbs')
    operation_start_time = time.time()
    oldcurs = olddb.cursor()
    oldcurs.execute('SELECT * FROM tweets')
    blocks_converted = 0
    print('selected tweets')
    has_debug = False

    while True:
        print('starting block %i' % blocks_converted)
        results = oldcurs.fetchmany(100000)
        print('fetched block, processing')
        if not results:
            break
        to_write = []
        for result in results:
            tweet_text = _correct_encodings(result[2])
            if not _text_decodes_to_ascii(tweet_text):
                continue

            formatted_tweet = (improved_hash(tweet_text),
                                result[0],
                                tweet_text)
            to_write.append(formatted_tweet)
            if not has_debug:
                # only need to see debug info once
                print('result: ', result, 'formatted: ', formatted_tweet)
                has_debug = True
        print('converted block, saving to disk')
        newcurs = newdb.cursor()
        newcurs.executemany("INSERT INTO tweets VALUES (?, ?, ?)", to_write)
        newdb.commit()
        blocks_converted += 1
        print('block converted %i, runtime %s' %
            (blocks_converted, time.time() - operation_start_time))



if __name__ == "__main__":
    main()
    # source = pickle.load(open('testdata/tst2.p', 'r'))
    # source = pickle.load(open('tstdata/20ktst1.p'))
    # test(source, False)
    # db_conversion_utility()
