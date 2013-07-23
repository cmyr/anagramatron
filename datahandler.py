from __future__ import print_function
import sqlite3 as lite
import os
import logging
import time
import cPickle as pickle
import multiprocessing
from operator import itemgetter


import anagramfunctions
import hitmanager
import anagramstats as stats

from constants import (ANAGRAM_FETCH_POOL_SIZE, ANAGRAM_CACHE_SIZE,
                       STORAGE_DIRECTORY_PATH)


DATA_PATH_COMPONENT = 'anagramdata'
CACHE_PATH_COMPONENT = 'cachedump'

HIT_STATUS_REVIEW = 'review'
HIT_STATUS_REJECTED = 'rejected'
HIT_STATUS_POSTED = 'posted'
HIT_STATUS_APPROVED = 'approved'
HIT_STATUS_MISC = 'misc'
HIT_STATUS_FAILED = 'failed'


class DataCoordinator(object):
    """
    DataCoordinator handles the storage, retrieval and comparisons
    of anagram candidates.
    It caches newly returned or requested candidates to memory,
    and maintains & manages a persistent database of older candidates.
    """
    def __init__(self, languages=['en']):
        """
        language selection is not currently implemented
        """
        self.languages = languages
        self.cache = dict()
        self.fetch_pool = dict()
        self.hashes = set()
        self.datastore = None
        self._should_trim_cache = False
        self._lock = multiprocessing.Lock()
        self.dbpath = (STORAGE_DIRECTORY_PATH +
                       DATA_PATH_COMPONENT +
                       '_'.join(self.languages) + '.db')
        self.cachepath = (STORAGE_DIRECTORY_PATH +
                          CACHE_PATH_COMPONENT +
                          '_'.join(self.languages) + '.p')
        self.setup()

    def setup(self):
        """
        - unpickle previous session's cache
        - load / init database
        - extract hashes
        """
        self.cache = self._load_cache()
        if not os.path.exists(self.dbpath):
            self.datastore = lite.connect(self.dbpath)
            cursor = self.datastore.cursor()
            print('data not found, creating new database')
            cursor.execute(
                "CREATE TABLE tweets(tweet_hash TEXT PRIMARY KEY ON CONFLICT REPLACE, tweet_id INTEGER, tweet_text TEXT)"
            )
        else:
            self.datastore = lite.connect(self.dbpath)
        # extract hashes
        print('extracting hashes')
        operation_start_time = time.time()
        cursor = self.datastore.cursor()
        cursor.execute('SELECT tweet_hash FROM tweets')
        while True:
            results = cursor.fetchmany(100000)
            if not results:
                break
            for result in results:
                self.hashes.add(str(result[0]))
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
                # debug cache writing
                if len(self.cache) > ANAGRAM_CACHE_SIZE:
                    self._should_trim_cache = True
                if self._should_trim_cache:
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
        print('performing fetch')
        load_time = time.time()
        cursor = self.datastore.cursor()
        hashes = ['"%s"' % self.fetch_pool[i]['tweet_hash'] for i in self.fetch_pool]
        hashes = ",".join(hashes)
        with self._lock:
            cursor.execute("SELECT * FROM tweets WHERE tweet_hash IN (%s)" % hashes)
            results = cursor.fetchall()
            self.hashes -= set(hashes)

        for result in results:
            fetched_tweet = self._tweet_from_sql(result)
            new_tweet = self.fetch_pool[fetched_tweet['tweet_hash']]
            if anagramfunctions.test_anagram(fetched_tweet['tweet_text'],
                                             new_tweet['tweet_text']):
                hitmanager.new_hit(fetched_tweet, new_tweet)
            else:
                self.cache[new_tweet['tweet_hash']] = {'tweet': new_tweet,
                                                       'hit_count': 1}
        # reset our fetch_pool
        self.fetch_pool = dict()
        print('fetch finished in %s' % anagramfunctions.format_seconds(time.time()-load_time))

    def _trim_cache(self, to_trim=None):
        """
        takes least frequently hit tweets from cache and writes to datastore
        """
        self._should_trim_cache = False
        # first just grab hashes with zero hits. If that's less then 1/2 total
        # do a more complex filter
        hashes_to_save = [x for x in self.cache if not self.cache[x]['hit_count']]
        if len(hashes_to_save) < len(self.cache)/2:
            # find the oldest, least frequently hit items in cache:
            cache_list = self.cache.values()
            cache_list = [(x['tweet']['tweet_hash'],
                           x['tweet']['tweet_id'],
                           x['hit_count']) for x in cache_list]
            s = sorted(cache_list, key=itemgetter(1))
            cache_list = sorted(s, key=itemgetter(2))
            if not to_trim:
                to_trim = len(cache_list)/2
            hashes_to_save = [x for (x, y, z) in cache_list[:to_trim]]

        # write those caches to disk, delete from cache, add to hashes
        to_write = [(self.cache[x]['tweet']['tweet_hash'],
                     self.cache[x]['tweet']['tweet_id'],
                     self.cache[x]['tweet']['tweet_text']) for x in hashes_to_save]
        for x in hashes_to_save:
            del self.cache[x]
        self.hashes |= set(hashes_to_save)
        _write_process = multiprocessing.Process(
            target=self._perform_write,
            args=(self._lock,
                  to_write,
                  self.dbpath))
        _write_process.start()

    def _perform_write(self, lock, to_write, db_path):
        with lock:
            database = lite.connect(db_path)
            cursor = database.cursor()
            cursor.executemany("INSERT INTO tweets VALUES (?, ?, ?)", to_write)
            database.commit()
            database.close()

    def _perform_fetch(self, to_fetch):
        pass

    def _save_cache(self):
        """
        pickles the tweets currently in the cache.
        doesn't save hit_count. we don't want to keep briefly popular
        tweets in cache indefinitely
        """
        tweets_to_save = [self.cache[t]['tweet'] for t in self.cache]
        try:
            pickle.dump(tweets_to_save, open(self.cachepath, 'wb'))
            print('saved %i tweets to cache' % len(tweets_to_save))
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

    def close(self):
        self.hashes = set()
        # we want to free up memory, batch_fetch performs set arithmetic
        if len(self.fetch_pool):
            print('running batch fetch with %i tweets' % len(self.fetch_pool))
            self._batch_fetch()
        print('saving cache')
        self._save_cache()
        stats.update_console()
        self.datastore.close()


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


def archive_old_tweets(cutoff=0.2):
    """cutoff represents the rough fraction of tweets to be archived"""
    load_time = time.time()
    db = lite.connect(TWEET_DB_PATH)
    cursor = db.cursor()
    cursor.execute("SELECT id FROM tweets")
    ids = cursor.fetchall()
    ids = [str(h) for (h,) in ids]
    print('extracted %i ids in %s' % (len(ids), anagramfunctions.format_seconds(time.time()-load_time)))
    ids = sorted(ids)
    tocull = int(len(ids) * cutoff)
    ids = ids[:tocull]
    print('found %i old tweets' % len(ids))
    load_time = time.time()
    ids = ["'%s'" % i for i in ids]
    # todo we actually want to archive this stuff tho
    cursor.execute("SELECT * FROM tweets WHERE id IN (%s)" % ",".join(ids))
    results = cursor.fetchall()
    db.execute("DELETE FROM tweets WHERE id IN (%s)" % ",".join(ids))
    db.commit()
    filename = "data/culled_%s.p" % time.strftime("%b%d%H%M")
    pickle.dump(results, open(filename, 'wb'))
    print('archived %i hashes in %s' % (len(ids), anagramfunctions.format_seconds(time.time()-load_time)))


if __name__ == "__main__":
    # dc = DataCoordinator()
    # sys.exit(1)

    print(
        "Anagrams Data Utilities, Please Select From the Following Options:",
        "\n (R)eview Hits",
        "\n (T)rim Short Tweets",
        "\n (A)rchive Old Tweets",
        "\n (D)ump sqlite to file"
        "\n (Q)uit")

    inp = raw_input(':')
    while 1:
        if inp in ['r', 'R']:
            dh = DataHandler(just_the_hits=True)
            dh.review_hits()
            dh.finish()
            break
        elif inp in ['t', 'T']:
            # print('not implemented')
            while 1:
                print('enter the character count cutoff below which tweets will be culled')
                cutoff = raw_input('cutoff:')
                try:
                    cutoff = int(cutoff)
                except ValueError:
                    print('enter a number between 50-100')
                    continue
                if (cutoff < 15) or (cutoff > 100):
                    print('enter a number between 50-100')
                    continue
                trim_short_tweets(cutoff=cutoff)
                break
            break
        elif inp in ['a', 'A']:
            while 1:
                print('enter the number of tweets to archive as a decimal')
                cutoff = raw_input('cutoff:')
                try:
                    cutoff = float(cutoff)
                except ValueError:
                    print('enter a number between 0.01-0.9')
                    continue
                if (cutoff < 0.01) or (cutoff > 0.9):
                    print('enter a number between 0.01-0.9')
                    continue
                archive_old_tweets(cutoff=cutoff)
                break
            break
        elif inp in ['d', 'D']:
            dh = DataHandler()
            dh.dump()
            dh.finish()
            break
        elif inp in ['q', 'Q']:
            break
    # dh = DataHandler(just_the_hits=True)
    # dh.review_hits()
    # dh.finish()
