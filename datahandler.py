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


DATA_PATH_COMPONENT = 'anagramdbm'
CACHE_PATH_COMPONENT = 'cachedump'

HIT_STATUS_REVIEW = 'review'
HIT_STATUS_REJECTED = 'rejected'
HIT_STATUS_POSTED = 'posted'
HIT_STATUS_APPROVED = 'approved'
HIT_STATUS_MISC = 'misc'
HIT_STATUS_FAILED = 'failed'


class NeedsMaintenance(Exception):
    """
    hacky exception raised when DataCoordinator is no longer able to keep up.
    use this to signal that we should shutdown and perform maintenance.
    """
    # this isn't being used right now, but might be used to implement
    # automated trimming / removal of old tweets from the permanent store
    # when things are getting too slow.
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
            # print('\ntweet in fetchpool')
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
        load_time = time.time()
        fetch_count = len(self.fetch_pool)
        hashes = [self.fetch_pool[i]['tweet_hash'] for i in self.fetch_pool]
        results = []
        for h in hashes:
            results.append(self.datastore[h])
        self.hashes -= set(hashes)
        # self._lock.release()
        for result in results:
            fetched_tweet = self._tweet_from_dbm(result)
            new_tweet = self.fetch_pool[fetched_tweet['tweet_hash']]
            if anagramfunctions.test_anagram(fetched_tweet['tweet_text'],
                                             new_tweet['tweet_text']):
                hitmanager.new_hit(fetched_tweet, new_tweet)
            else:
                self.cache[new_tweet['tweet_hash']] = {'tweet': new_tweet,
                                                       'hit_count': 1}
        # reset our fetch_pool
        self.fetch_pool = dict()
        logging.debug('fetched %i from %i in %s' % (fetch_count, len(self.hashes), anagramfunctions.format_seconds(time.time()-load_time)))
        # else:
        #     pass
            # if we can't acquire lock we'll just try again


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
        # hashes_to_save = [x for x in self.cache if not self.cache[x]['hit_count']]
        # if len(hashes_to_save) < len(self.cache)/2:
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
        # to_write = [self.cache[x] for x in hashes_to_save]
        for x in hashes_to_save:

            self.datastore[x] = self._dbm_from_tweet(self.cache[x]['tweet'])
            del self.cache[x]
        self.hashes |= set(hashes_to_save)
        # when we're done writing, check to see how long our buffer is.
        # if it's gotten too long, we raise our NeedsMaintenance exception.
        buffer_size = stats.buffer_size()
        print('finished with buffer size: %i' % buffer_size)
        if buffer_size > ANAGRAM_STREAM_BUFFER_SIZE:
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

    def _tweet_from_sql(self, sql_tweet):
        return {
            'tweet_hash': sql_tweet[0],
            'tweet_id': long(sql_tweet[1]),
            'tweet_text': sql_tweet[2]
        }

    def _tweet_from_dbm(self, dbm_tweet):
        tweet_values = re.split(unichr(0017), dbm_tweet.decode('utf-8'))
        t = dict()
        t['tweet_id'] = int(tweet_values[0])
        t['tweet_hash'] = tweet_values[1]
        t['tweet_text'] = tweet_values[2]
        return t

    def _dbm_from_tweet(self, tweet):
        dbm_string = unichr(0017).join([unicode(i) for i in tweet.values()])
        return dbm_string.encode('utf-8')

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

        # we want to free up memory, batch_fetch performs set arithmetic
        # if len(self.cache) > ANAGRAM_CACHE_SIZE:\
        # self._trim_cache()
        # self._write_process.join()
        self._save_cache()
        self.datastore.close()


def archive_dbm_tweets(dbmpath, cutoff=0.2):
    load_time = time.time()
    db = anydbm.open(dbmpath, 'w')
    archive_path = "data/culled_%s.db" % time.strftime("%b%d%H%M")
    archive = anydbm.open(archive_path, 'c')
    max_id = max(db.keys())
    min_id = min(db.keys())
    cutoff_tweet = min_id + ((max_id-min_id) * cutoff)
    print('found cutoff tweet in %s' % anagramfunctions.format_seconds(time.time()-load_time))

    for t in db:
        if t < cutoff_tweet:
            archive[t] = db[t]
            del db[t]

    print('deleted %i tweets in %s' % (len(archive), anagramfunctions.format_seconds(time.time()-load_time)))


def archive_old_tweets(dbpath, cutoff=0.2):
    """cutoff represents the rough fraction of tweets to be archived"""
    load_time = time.time()
    db = lite.connect(dbpath)
    cursor = db.cursor()
    tweet_ids = list()
    cursor.execute("SELECT tweet_id FROM tweets")
    while True:
        results = cursor.fetchmany(1000000)
        if not results:
            break
        for result in results:
            tweet_ids.append(result[0])
    print('extracted %i tweets in %s' % (len(tweet_ids), anagramfunctions.format_seconds(time.time()-load_time)))

    load_time = time.time()
    max_id = max(tweet_ids)
    min_id = min(tweet_ids)
    cutoff_tweet = min_id + ((max_id-min_id) * cutoff)
    cursor.execute("CREATE TABLE tmp AS SELECT * FROM tweets WHERE tweet_id > (?)", (cutoff_tweet,))
    cursor.execute("DROP TABLE tweets")
    cursor.execute("ALTER TABLE tmp RENAME to tweets")
    db.commit()
    db.close()


def trim_short_tweets(cutoff=20):
    """
    utility function for deleting short tweets from our database
    cutoff represents the rough percentage of tweets to be deleted
    """
    load_time = time.time()
    db = lite.connect(TWEET_DB_PATH)
    cursor = db.cursor()
    cursor.execute("SELECT hash FROM tweets")
    hashes = cursor.fetchall()
    hashes = set([str(h) for (h,) in hashes])
    print('extracted %i hashes in %s' % (len(hashes), anagramfunctions.format_seconds(time.time()-load_time)))
    short_hashes = [h for h in hashes if len(h) < cutoff]
    print("found %i of %i hashes below %i character cutoff" % (len(short_hashes), len(hashes), cutoff))
    load_time = time.time()
    hashvals = ["'%s'" % h for h in short_hashes]
    db.execute("DELETE FROM tweets WHERE hash IN (%s)" % ",".join(hashvals))
    # self.cache.executemany("DELETE FROM tweets WHERE hash=(?)", iter(short_hashes))
    db.commit()
    print('deleted %i hashes in %s' % (len(short_hashes), anagramfunctions.format_seconds(time.time()-load_time)))
    # short_hashes = set(short_hashes)
    # self.hashes = self.hashes.difference(short_hashes)


# def combine_databases(path1, path2, rate, debug=True):
#     print('adding tweets from %s to %s' % (db2, db1))
#     db1 = lite.connect(path1)
#     db2 = lite.connect(path2)

#     # so what's the plan, here? 
#     #   1) fetch from 2 at $RATE
#     #   2) select $FETCHED from 1
#     #   3) compare results
#     #   4) if there's a hit, just print it or something?

#     cursor = db2.cursor()
#     cursor.execute("SELECT tweet_id FROM tweets")
#     while True:
#         results = cursor.fetchmany(rate)
#         if not results:
#             break
#         for result in results:



if __name__ == "__main__":
    # dc = DataCoordinator()
    # sys.exit(1)
    pass

