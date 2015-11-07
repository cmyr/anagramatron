# coding: utf-8

from __future__ import print_function
import os
import re
import sys
# import logging
import time
import cPickle as pickle
import multiprocessing
from operator import itemgetter


from . import multidbm, anagramfunctions, hitmanager, constants
from simpledatastore import AnagramSimpleStore
# import anagramstats as stats

# from .constants import (ANAGRAM_CACHE_SIZE, STORAGE_DIRECTORY_PATH,
                       # ANAGRAM_STREAM_BUFFER_SIZE)


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

    :languages: a list of language identifiers. In future, multiple languages
    might be supported. NOT IMPLEMENTED.
    :storage: type of backing store. currently accepts None or 'mdbm'.
    :hit_callback: a function to be called when an anagram is found.
    :test_func: a function called when an anagram is found. 
    Should implement some heuristic and return True if the passed anagram is 'interesting'.
    """

    def __init__(self, languages=['en'],
                 storage=None,
                 hit_callback=hitmanager.new_hit,
                 test_func=anagramfunctions.test_anagram):
        """
        language selection is not currently implemented
        """
        if languages != ['en']:
            raise NotImplementedError(
                'languages other then \'en\' are not currently expected')
        self.languages = languages
        self._should_trim_cache = False
        self._write_process = None
        self._lock = multiprocessing.Lock()
        self._is_writing = multiprocessing.Event()
        self.dbpath = os.path.join(
            constants.ANAGRAM_DATA_DIR, 
            '%s_%s.db' % (DATA_PATH_COMPONENT, '_'.join(languages)))
        self.cachepath = os.path.join(
            constants.ANAGRAM_DATA_DIR, 
            '%s_%s.db' % (CACHE_PATH_COMPONENT, '_'.join(languages)))

        self.hit_callback = hit_callback
        self.test_func = test_func

        if noload:
            self.cache = AnagramSimpleStore()
            self.datastore = None
        else:
            self.cache = AnagramSimpleStore(self.cachepath, ANAGRAM_CACHE_SIZE)
            self.datastore = multidbm.MultiDBM(self.dbpath)

    def handle_input(self, inp, text_key="text"):
        """
        takes either a string or a dict, and compares it against
        all previous input. if an anagram is found, runs self.test_func
        and then self.hit_callback if test passes.
        """
        text = self._text_from_input(inp, text_key)
        key = anagramfunctions.improved_hash(text)
        if key in self.cache:
            # stats.cache_hit()
            match = self.cache[key]
            match_text = self._text_from_input(match, key)
            if self.test_func(text, match_text):
                del self.cache[key]
                self.hit_callback(inp, match)
            else:
                # anagram, but fails tests (too similar)
                self.cache[key] = inp
        else:
            # not in cache. in datastore?
            if key in self.datastore:
                self._process_hit(inp, key, text_key)
            else:
                # not in datastore. add to cache
                self.cache[key] = inp
                # stats.set_cache_size(len(self.cache))

                if len(self.cache) > ANAGRAM_CACHE_SIZE:
                    self._trim_cache()

    def _process_hit(self, inp, key, text_key):
        try:
            hit = anagramfunctions.decode_tweet(self.datastore[key])
            hit_text = self._text_from_input(hit, text_key)
            text = self._text_from_input(inp, text_key)
        except (UnicodeDecodeError, ValueError) as err:
            print('error decoding hit for key %s' % key)
            self.cache[key] = inp
            return
        stats.possible_hit()
        if self.test_func(text, hit_text):
            self.hit_callback(inp, hit)
        else:
            self.cache[key] = inp

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

        if not to_trim:
            to_trim = min(10000, (ANAGRAM_CACHE_SIZE / 10))

        to_store = self.cache.least_used(to_trim)
        # write those caches to disk, delete from cache, add to hashes
        for x in to_store:
            self.datastore[x] = _dbm_from_tweet(self.cache[x])
            del self.cache[x]

        buffer_size = stats.buffer_size()
        if buffer_size > ANAGRAM_STREAM_BUFFER_SIZE:
            print('raised needs maintenance')
            raise NeedsMaintenance

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

        self.cache.save()
        self.datastore.close()


# def _tweet_from_dbm(dbm_tweet):
#     tweet_values = re.split(unichr(0017), dbm_tweet.decode('utf-8'))
#     t = dict()
#     t['tweet_id'] = int(tweet_values[0])
#     t['tweet_hash'] = tweet_values[1]
#     t['tweet_text'] = tweet_values[2]
#     return t


# # I turn my tweet objects into strings by joining them with the u0017 char,
# # because when I wrote this I didn't know about __repr__. o_Ô

# def _dbm_from_tweet(tweet):
#     dbm_string = unichr(0017).join([unicode(i) for i in tweet.values()])
#     return dbm_string.encode('utf-8')


def dbm_iter(dbm_path):
    import gdbm
    db = gdbm.open(dbm_path)
    key = db.firstkey()
    while key is not None:
        try:
            yield anagramfunctions.decode_tweet(db[key])
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
    parser.add_argument(
        '-r', '--repair', help='repair target database', action="store_true")
    parser.add_argument('db', type=str, help="source database file")
    parser.add_argument(
        '-t', '--trim', type=int, help="trim low length values")
    parser.add_argument(
        '-d', '--destination', type=str, help="destination database file")
    parser.add_argument('-s', '--start', type=int, help='skip-to position')
    args = parser.parse_args()

    if args.repair:
        return repair_database()


if __name__ == "__main__":
    main()
