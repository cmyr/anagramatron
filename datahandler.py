from __future__ import print_function
import sqlite3 as lite
import anydbm
import os
import re
import sys
import shutil
import logging
import time
import cPickle as pickle
import multiprocessing
from operator import itemgetter


import anagramfunctions
import hitmanager
import anagramstats as stats

from constants import (ANAGRAM_FETCH_POOL_SIZE, ANAGRAM_CACHE_SIZE,
                       STORAGE_DIRECTORY_PATH, ANAGRAM_STREAM_BUFFER_SIZE)


DATA_PATH_COMPONENT = 'anagramdbm2'
CACHE_PATH_COMPONENT = 'cachedump'

from hitmanager import (HIT_STATUS_SEEN, HIT_STATUS_REVIEW, HIT_STATUS_POSTED,
        HIT_STATUS_REJECTED, HIT_STATUS_APPROVED, HIT_STATUS_MISC,
        HIT_STATUS_FAILED)



class NeedsMaintenance(Exception):
    """
    hacky exception raised when DataCoordinator is no longer able to keep up.
    use this to signal that we should shutdown and perform maintenance.
    """
    pass


class DataCoordinator(object):
    """
    DataCoordinator handles the storage, retrieval and comparisons
    of anagram candidates.
    It caches newly returned or requested candidates to memory,
    and maintains & manages a persistent database of older candidates.
    """
    def __init__(self, languages=['en'], noload=False):
        """
        language selection is not currently implemented
        """
        self.languages = languages
        self.cache = dict()
        self.fetch_pool = dict()
        self.hashes = set()
        self.datastore = None
        self._should_trim_cache = False
        self._write_process = None
        self._lock = multiprocessing.Lock()
        self._is_writing = multiprocessing.Event()
        self.dbpath = (STORAGE_DIRECTORY_PATH +
                       DATA_PATH_COMPONENT +
                       '_'.join(self.languages) + '.db')
        self.cachepath = (STORAGE_DIRECTORY_PATH +
                          CACHE_PATH_COMPONENT +
                          '_'.join(self.languages) + '.p')
        if not noload:
            self._setup()

    def _setup(self):
        """
        - unpickle previous session's cache
        - load / init database
        - extract hashes
        """
        self.cache = self._load_cache()
        self.datastore = anydbm.open(self.dbpath, 'c')
        # extract hashes
        print('extracting hashes')
        operation_start_time = time.time()
        self.hashes.update(set(self.datastore.keys()))
        print('extracted %i hashes in %s' %
              (len(self.hashes), anagramfunctions.format_seconds(time.time()-operation_start_time)))
        # setup hit manager:
        hitmanager._setup(self.languages)

    def handle_input(self, tweet):
        """
        recieves a filtered tweet.
        - checks if it exists in cache
        - checks if in database
        - if yes adds to fetch queue(checks if in fetch queue)
        """

        key = tweet['tweet_hash']
        if key in self.cache:
            stats.cache_hit()
            hit_tweet = self.cache[key]['tweet']
            if anagramfunctions.test_anagram(tweet['tweet_text'], hit_tweet['tweet_text']):
                del self.cache[key]
                hitmanager.new_hit(tweet, hit_tweet)
            else:
                self.cache[key]['tweet'] = tweet
                self.cache[key]['hit_count'] += 1
        else:
            # not in cache. in datastore?
            if key in self.hashes:
            # add to fetch_pool
                self._add_to_fetch_pool(tweet)
            else:
                # not in datastore. add to cache
                self.cache[key] = {'tweet': tweet,
                                   'hit_count': 0}
                stats.set_cache_size(len(self.cache))

                if len(self.cache) > ANAGRAM_CACHE_SIZE:
                    # we imagine a future in which trimming isn't based on a constant
                    self._should_trim_cache = True

                if self._should_trim_cache:
                    if self._is_writing.is_set():
                        # means we're trying to write before previous write op is done
                        if len(self.cache) > 4*ANAGRAM_CACHE_SIZE:
                            raise NeedsMaintenance
                    else:
                        self._trim_cache()

    def _add_to_fetch_pool(self, tweet):
        key = tweet['tweet_hash']
        if self.fetch_pool.get(key):
            # exists in fetch pool, run comps
            hit_tweet = self.fetch_pool[key]
            if anagramfunctions.test_anagram(tweet['tweet_text'], hit_tweet['tweet_text']):
                del self.fetch_pool[key]
                hitmanager.new_hit(tweet, hit_tweet)
            else:
                pass
        else:
            self.fetch_pool[key] = tweet
            if len(self.fetch_pool) > ANAGRAM_FETCH_POOL_SIZE:
                self._batch_fetch()

        stats.set_fetch_pool_size(len(self.fetch_pool))

    def _batch_fetch(self):
        """
        fetches all the tweets in our fetch pool and runs comparisons
        deleting from
        """
                # when we're done writing, check to see how long our buffer is.
        # if it's gotten too long, we raise our NeedsMaintenance exception.
        buffer_size = stats.buffer_size()
        should_raise_maintenance_flag = False
        if buffer_size:
            print('\nfinished with buffer size: %i\n' % buffer_size)
            logging.debug('finished with buffer size: %i\n' % buffer_size)
        if buffer_size > ANAGRAM_STREAM_BUFFER_SIZE:
            should_raise_maintenance_flag = True
            # putting this here because I forget how our processes work
            # and am unsure if we'll still have a buffer when we exit this function?
            # but if we perform maintenance now we'll have a crash on restart
            # b/c there are things in the fetchpool that no longer exist in db

        load_time = time.time()
        fetch_count = len(self.fetch_pool)
        hashes = [self.fetch_pool[i]['tweet_hash'] for i in self.fetch_pool]
        results = []
        for h in hashes:
            results.append(self.datastore[h])
        self.hashes -= set(hashes)
        # self._lock.release()
        for result in results:
            fetched_tweet = _tweet_from_dbm(result)
            new_tweet = self.fetch_pool[fetched_tweet['tweet_hash']]
            if anagramfunctions.test_anagram(fetched_tweet['tweet_text'],
                                             new_tweet['tweet_text']):
                hitmanager.new_hit(fetched_tweet, new_tweet)
            else:
                self.cache[new_tweet['tweet_hash']] = {'tweet': new_tweet,
                                                       'hit_count': 1}
        # reset our fetch_pool
        self.fetch_pool = dict()
        logging.debug('fetched %i from %i in %s' % 
            (fetch_count,
                len(self.hashes),
                anagramfunctions.format_seconds(time.time()-load_time)))

        if buffer_size > ANAGRAM_STREAM_BUFFER_SIZE:
            logging.debug('buffer size after fetch: %i' % stats.buffer_size())

        if should_raise_maintenance_flag:
            raise NeedsMaintenance


    def _trim_cache(self, to_trim=None):
        """
        takes least frequently hit tweets from cache and writes to datastore
        """
        # self._is_writing.set()
        # perform fetch before trimming cache:
        if len(self.fetch_pool):
            self._batch_fetch()
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
            to_trim = 10000
        hashes_to_save = [x for (x, y, z) in cache_list[:to_trim]]

        # write those caches to disk, delete from cache, add to hashes
        for x in hashes_to_save:

            self.datastore[x] = _dbm_from_tweet(self.cache[x]['tweet'])
            del self.cache[x]
        self.hashes |= set(hashes_to_save)

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
        oldcache = self.cache
        print('stashing cache with %i items' % len(oldcache))
        self.close()
        # move current db out of the way
        newpath = (STORAGE_DIRECTORY_PATH +
                    DATA_PATH_COMPONENT +
                    '_'.join(self.languages) +
                    time.strftime("%b%d%H%M") + '.db')
        os.rename(self.dbpath, newpath)
        self._setup()
        print('restoring cache with %i items' % len(oldcache))
        self.cache = oldcache


    def close(self):
        self.hashes = set()
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


def _dbm_from_tweet(tweet):
    dbm_string = unichr(0017).join([unicode(i) for i in tweet.values()])
    return dbm_string.encode('utf-8')


def combine_databases(path1, path2, minlen=20):
    try:
        import gdbm
    except ImportError:
        print('combining databases requires the gdbm module. :(')
    print('adding tweets from %s to %s' % (path2, path1))

    db1 = gdbm.open(path1, 'w')
    db2 = gdbm.open(path2, 'w')
    start_time = time.time()

    k = db2.firstkey()
    temp_k = None
    try:
        while k is not None:
            tweet = _tweet_from_dbm(db2[k])
            # print(k, tweet)
            stats.tweets_seen()
            if len(anagramfunctions.stripped_string(tweet['tweet_text'])) < minlen:
                k = db2.nextkey(k)
                continue
            stats.passed_filter()
            if k in db1:
                stats.possible_hit()
                tweet2 = _tweet_from_dbm(db1[k])
                if anagramfunctions.test_anagram(
                    tweet['tweet_text'],
                    tweet2['tweet_text']
                    ):
                    temp_k = db2.nextkey(k)
                    del db2[k]
                    hitmanager.new_hit(tweet, tweet2)
                else:
                    pass
            else:
                db1[k] = _dbm_from_tweet(tweet)
            stats.update_console()
            k = db2.nextkey(k)
            if not k and temp_k:
                k = temp_k
                temp_k = None
    finally:
        db1.close()
        db2.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) is not 2:
        print('please select exactly two target databases')

    combine_databases(args[0], args[1])
    # dc = DataCoordinator()
    # sys.exit(1)
    pass
