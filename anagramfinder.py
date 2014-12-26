# coding: utf-8

from __future__ import print_function
import gdbm
import multidbm
import os
import re
import sys
import logging
import time
import cPickle as pickle
import multiprocessing
from operator import itemgetter


import anagramfunctions
import hitmanager
import anagramstats as stats

from constants import (ANAGRAM_CACHE_SIZE, STORAGE_DIRECTORY_PATH,
 ANAGRAM_STREAM_BUFFER_SIZE)


DATA_PATH_COMPONENT = 'anagrammdbm'
CACHE_PATH_COMPONENT = 'cachedump'


class NeedsMaintenance(Exception):
    """
    hacky exception raised when AnagramFinder is no longer able to keep up.
    use this to signal that we should shutdown and perform maintenance.
    """
    pass


class AnagramFinder(object):
    """
    AnagramFinder handles the storage, retrieval and comparisons
    of anagram candidates.
    It caches newly returned or requested candidates to memory,
    and maintains & manages a persistent database of older candidates.
    """
    def __init__(self, languages=['en'],
        noload=False,
        storage_location=STORAGE_DIRECTORY_PATH,
        hit_handler=hitmanager.new_hit,
        anagram_test=anagramfunctions.test_anagram):
        """
        language selection is not currently implemented
        """
        self.languages = languages
        self.cache = dict()
        self.datastore = None
        self._should_trim_cache = False
        self._write_process = None
        self._lock = multiprocessing.Lock()
        self._is_writing = multiprocessing.Event()
        self.dbpath = (storage_location +
                       DATA_PATH_COMPONENT +
                       '_'.join(self.languages) + '.db')
        self.cachepath = (storage_location +
                          CACHE_PATH_COMPONENT +
                          '_'.join(self.languages) + '.p')


        self.hit_handler = hit_handler
        self.anagram_test = anagram_test

        if not noload:
            self._setup()

    def _setup(self):
        """
        - unpickle previous session's cache
        - load / init database
        - extract hashes
        """
        self.cache = self._load_cache()
        self.datastore = multidbm.MultiDBM(self.dbpath)
        # hitmanager._setup(self.languages)

    def handle_input(self, inp, text_key="text"):
        """
        takes either a string or a dict, and compares it against
        all previous input. if an anagram is found, runs self.anagram_test
        and then self.hit_handler if test passes.
        """
        text = self._text_from_input(inp, text_key)
        key = anagramfunctions.improved_hash(text)
        if key in self.cache:
            stats.cache_hit()
            match = self.cache[key]['tweet']
            match_text = self._text_from_input(match, key)
            if self.anagram_test(text, match_text):
                del self.cache[key]
                self.hit_handler(inp, match)
            else:
                self.cache[key]['tweet'] = inp
                self.cache[key]['hit_count'] += 1
        else:
            # not in cache. in datastore?
            if key in self.datastore:
                self._process_hit(inp, key, text_key)
            else:
                # not in datastore. add to cache
                self.cache[key] = {'tweet': inp,
                                   'hit_count': 0}
                stats.set_cache_size(len(self.cache))

                if len(self.cache) > ANAGRAM_CACHE_SIZE:
                    self._trim_cache()

    def _process_hit(self, inp, key, text_key):
        try:
            hit = _tweet_from_dbm(self.datastore[key])
            hit_text = self._text_from_input(hit, text_key)
            text = self._text_from_input(inp, text_key)
        except UnicodeDecodeError as err:
            print('error decoding hit for key %s' % key)
            self.cache[key] = {'tweet': inp, 'hit_count': 1}
            return
        stats.possible_hit()
        if self.anagram_test(text, hit_text):
            self.hit_handler(inp, hit)
        else:
            self.cache[key] = {'tweet': inp, 'hit_count': 1}

    def _text_from_input(self, inp, key=None):
        LEGACY_KEY = 'tweet_text'
        if isinstance(inp, unicode):
            return inp
        else:
            text = inp.get(key) or inp.get(LEGACY_KEY)
            if not text:
                raise TypeError('expected string or dict')
            return text

    def _trim_cache(self, to_trim=None):
        """
        takes least frequently hit tweets from cache and writes to datastore
        """

        self._should_trim_cache = False
        # first just grab hashes with zero hits. If that's less then 1/2 total
        # do a more complex filter
            # find the oldest, least frequently hit items in cache:
        cache_list = self.cache.values()
        cache_list = [(x['tweet']['tweet_hash'],
                       x['tweet']['tweet_id'],
                       x['hit_count']) for x in cache_list]
        s = sorted(cache_list, key=itemgetter(1))
        cache_list = sorted(s, key=itemgetter(2))
        if not to_trim:
            to_trim = min(10000, (ANAGRAM_CACHE_SIZE/10))
        hashes_to_save = [x for (x, y, z) in cache_list[:to_trim]]

        # write those caches to disk, delete from cache, add to hashes
        for x in hashes_to_save:

            self.datastore[x] = _dbm_from_tweet(self.cache[x]['tweet'])
            del self.cache[x]

        buffer_size = stats.buffer_size()
        if buffer_size > ANAGRAM_STREAM_BUFFER_SIZE:
            # self.perform_maintenance()
            print('raised needs maintenance')
            raise NeedsMaintenance

    def _save_cache(self):
        """
        pickles the tweets currently in the cache.
        doesn't save hit_count. we don't want to keep briefly popular
        tweets in cache indefinitely
        """
        tweets_to_save = [self.cache[t]['tweet'] for t in self.cache]
        try:
            pickle.dump(tweets_to_save, open(self.cachepath, 'wb'))
            print('saved cache to disk with %i tweets' % len(tweets_to_save))
        except:
            logging.error('unable to save cache, writing')
            self._trim_cache(len(self.cache))

    def _load_cache(self):
        print('loading cache')
        cache = dict()
        try:
            loaded_tweets = pickle.load(open(self.cachepath, 'r'))
            # print(loaded_tweets)
            for t in loaded_tweets:
                cache[t['tweet_hash']] = {'tweet': t, 'hit_count': 0}
            print('loaded %i tweets to cache' % len(cache))
            return cache
        except IOError:
            logging.error('error loading cache :(')
            return cache
            # really not tons we can do ehre


    def perform_maintenance(self):
        """
        called when we're not keeping up with input.
        moves current database elsewhere and starts again with new db
        """
        print("perform maintenance called")
        # save our current cache to be restored after we run _setup (hacky)
        moveddb = self.datastore.archive()
        print('moved mdbm chunk: %s' % moveddb)
        print('mdbm contains %s chunks' % self.datastore.section_count())


    def close(self):
        if self._write_process and self._write_process.is_alive():
            print('write process active. waiting.')
            self._write_process.join()

        self._save_cache()
        self.datastore.close()


def _tweet_from_dbm(dbm_tweet):
    tweet_values = re.split(unichr(0017), dbm_tweet.decode('utf-8'))
    t = dict()
    t['tweet_id'] = int(tweet_values[0])
    t['tweet_hash'] = tweet_values[1]
    t['tweet_text'] = tweet_values[2]
    return t


# I turn my tweet objects into strings by joining them with the u0017 char,
# because when I wrote this I didn't know about __repr__. o_Ô

def _dbm_from_tweet(tweet):
    dbm_string = unichr(0017).join([unicode(i) for i in tweet.values()])
    return dbm_string.encode('utf-8')

def dbm_iter(dbm_path):
    db = gdbm.open(dbm_path)
    key = db.firstkey()
    while key is not None:
        try:
            yield _tweet_from_dbm(db[key])
        # value isn't a tweet; should be metadata (filepath)
        except ValueError:
            pass
        key = db.nextkey(key)
    raise StopIteration()


def repair_database():
    db = AnagramFinder()
    db.datastore.perform_maintenance()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--repair', help='repair target database', action="store_true")
    parser.add_argument('db', type=str, help="source database file")
    parser.add_argument('-t', '--trim', type=int, help="trim low length values")
    parser.add_argument('-d', '--destination', type=str, help="destination database file")
    parser.add_argument('-s', '--start', type=int, help='skip-to position')
    args = parser.parse_args()

    
    if args.repair:
        return repair_database()


    if not args.db:
        print('please specify a target database.')

    outargs = dict()
    outargs['srcdb'] = args.db

    if args.trim:
        print('trim requested %i' % args.trim)
        outargs['cutoff'] = args.trim

    if args.start:
        outargs['start'] = args.start

    if args.destination:
        print('destination: %s' %args.destination)
        outargs['destdb'] = args.destination
        combine_databases(**outargs)
    else:
        delete_short_entries(**outargs)


    

if __name__ == "__main__":
    main()