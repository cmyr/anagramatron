from __future__ import print_function
import sqlite3 as lite
import os
import sys
import logging
import time
import cPickle as pickle
import multiprocessing
import utils
import twitterhandler
import anagramconfig
import anagramer

from constants import ANAGRAM_WRITE_CACHE_SIZE, ANAGRAM_FETCH_POOL_SIZE

# TWEET_DB_PATH = 'data/tweets.db'
# HITS_DB_PATH = 'data/hits.db'
# CACHE_STORE_PATH = 'data/cachedump.p'

DATA_PATH_COMPONENT = 'anagramdata'
CACHE_PATH_COMPONENT = 'cachedump'
HIT_PATH_COMPONENT = 'hitdata'

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
        self.cache = None
        self.fetch_pool = dict()
        self.hashes = set()
        self.datastore = None
        self.dbpath = (anagramconfig.STORAGE_DIRECTORY_PATH +
                       DATA_PATH_COMPONENT +
                       '_'.join(self.languages) + '.db')
        self.cachepath = (anagramconfig.STORAGE_DIRECTORY_PATH +
                          CACHE_PATH_COMPONENT +
                          '_'.join(self.languages) + '.p')
        self.hit_manager = HitManager(self.languages)
        self.setup()

    def setup(self):
        """
        - unpickle previous session's cache
        - load / init database
        - extract hashes
        """
        try:
            self.cache = pickle.load(open('CACHE_STORE_PATH', 'r'))
            ('cache loaded')
        except IOError as e:
            print('no loadable cache found')
            self.cache = dict()
        if not os.path.exists(self.dbpath):
            self.datastore = lite.connect(self.dbpath)
            cursor = self.datastore.cursor()
            print('data not found, creating new database')
            cursor.execute(
                "CREATE TABLE tweets(tweet_hash TEXT PRIMARY KEY, tweet_id INTEGER, tweet_text TEXT)"
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
                self.hashes.add(str(result))
        print('extracted %i hashes in %s' %
              (len(self.hashes), utils.format_seconds(time.time()-operation_start_time)))


    def handle_input(self, tweet):
        """recieves a filtered tweet.
        - checks if it exists in cache
        - checks if in database
        - if yes adds to fetch queue(checks if in fetch queue)
        """
        key = tweet['tweet_hash']
        if self.cache.get(key):
            hit_tweet = self.cache[key]['tweet']
            if anagramer.test_anagram(tweet['tweet_text'], hit_tweet['tweet_text']):
                self.cache['tweet_hash'] = None
                self.hit_manager.new_hit(tweet, hit_tweet)
            else:
                self.cache[key]['tweet'] = tweet
                self.cache[key]['hit_count'] += 1
        elif key in self.hashes:
            # add to fetch_pool
            if self.fetch_pool.get(key):
                # exists in fetch pool, run comps
                hit_tweet = self.fetch_pool(key)
                if anagramer.test_anagram(tweet['tweet_text'], hit_tweet['tweet_text']):
                    # remove from fetch pool
                    self.fetch_pool[key] = None
                    self.hit_manager.new_hit(tweet, hit_tweet)
            else:
                self.fetch_pool[key] = tweet
        else:
            self.cache[key] = {'tweet': tweet,
                                 'hit_count': 0}
            # check if we need to write cache


    def close(self):
        self.datastore.close()
        self.hit_manager.close()


class HitManager(object):
    """
    handles storage of hits. runs webserver for remote review
    """
    def __init__(self, languages):
        self.dbpath = (anagramconfig.STORAGE_DIRECTORY_PATH +
                       HIT_PATH_COMPONENT +
                       '_'.join(languages) + '.db')
        self.debug_hits = []

    def new_hit(self, first, second):
        hit = {
           "id": int(time.time()*1000),
           "status": HIT_STATUS_REVIEW,
           "tweet_one": first,
           "tweet_two": second
        }
        self.debug_hits.append(hit)

    def close(self):
        print("debug found %i hits" % len(self.debug_hits))
        for hit in self.debug_hits:
            print(hit['tweet_one']['tweet_text'])
            print(hit['tweet_two']['tweet_text'])


class DataHandler(object):
    """
    handles storage and retrieval of tweets
    """
    def __init__(self, just_the_hits=False, delegate=None):
        self.just_the_hits = just_the_hits
        self.twitterhandler = twitterhandler.TwitterHandler()
        self.write_cache = dict()
        self.write_cache_hashes = set()
        self.data = None
        self.fetch_pool = dict()
        self.delegate = delegate  # for sending hits
        self.hitsdb = None
        self.hashes = None
        self.debug_used_cache_count = 0
        self.setup()

    def setup(self):
        """
        creates database if it doesn't already exist
        populates hash table
        """
        if self.just_the_hits:
            # don't bother initing the cache etc
            self.hitsdb = lite.connect(HITS_DB_PATH)
            return
        if not os.path.exists(TWEET_DB_PATH):
            self.data = lite.connect(TWEET_DB_PATH)
            cursor = self.data.cursor()
            print('data not found, creating new database')
            cursor.execute("CREATE TABLE tweets(id integer, hash text, text text)")
            self.data.commit()
        else:
            self.data = lite.connect(TWEET_DB_PATH)
        if not os.path.exists(HITS_DB_PATH):
            self.hitsdb = lite.connect(HITS_DB_PATH)
            cursor = self.hitsdb.cursor()
            print('hits db not found, creating')
            cursor.execute("""CREATE TABLE hits
                (hit_id integer, hit_status text, one_id text, two_id text, one_text text, two_text text)""")
            self.hitsdb.commit()
        else:
            self.hitsdb = lite.connect(HITS_DB_PATH)
        # setup the hashtable
        print('extracting hashes')
        cursor = self.data.cursor()
        cursor.execute("SELECT hash FROM tweets")
        hashes = cursor.fetchall()
        self.hashes = set([str(h) for (h,) in hashes])
        print('loaded %d hashes' % (len(hashes)))

    def process_tweet(self, new_tweet):
        """
        called when a new tweet arrives.
        if the tweet matches a hash saves it for the next db fetch
        """
        if (self.fetch_pool.get(new_tweet['hash'])):
            # if there's a match in our hit pool do a quick diff check
            if (self.delegate.compare(new_tweet['text'], self.fetch_pool[new_tweet['hash']]['text'])):
                print('HIT IN FETCH POOL?', new_tweet, self.fetch_pool[new_tweet['hash']])
                logging.debug('HIT IN FETCH POOL? \n %s \n %s' % (new_tweet, self.fetch_pool[new_tweet['hash']]))
            return
        if (new_tweet['hash'] in self.write_cache_hashes):
            # if it's in the write cache return them both for checking
            self.debug_used_cache_count += 1
            self.delegate.process_hit(new_tweet, self.write_cache[new_tweet['hash']])
        if (self.contains(new_tweet['hash'])):
            # stored_tweet = self.get(new_tweet['hash'])
            self.fetch_pool[new_tweet['hash']] = new_tweet
            if (len(self.fetch_pool) > ANAGRAM_FETCH_POOL_SIZE):
                self.batch_fetch()
        else:
            self.add(new_tweet)

    def batch_fetch(self):
        """
        fetches all of the tweets in our fetch pool and returns them to delegate
        """
        # logging.debug("batch_fetch called, batch size: %i" % len(self.fetch_pool))
        cursor = self.data.cursor()
        hashes = ['"%s"' % self.fetch_pool[i]['hash'] for i in self.fetch_pool]
        cursor.execute("SELECT * FROM tweets WHERE hash IN (%s)" % ",".join(hashes))
        results = cursor.fetchall()
        for result in results:
            result = self.tweet_from_sql(result)
            new_tweet = self.fetch_pool[result['hash']]
            self.delegate.process_hit(result, new_tweet)

        self.fetch_pool = dict()

    def contains(self, tweet_hash):
        if tweet_hash in self.hashes or tweet_hash in self.write_cache_hashes:
            return True
        else:
            return False

    def count_hashes(self):
        return len(self.hashes)

    def add(self, tweet):
        self.write_cache[tweet['hash']] = tweet
        self.write_cache_hashes.add(tweet['hash'])
        if (len(self.write_cache_hashes) > ANAGRAM_WRITE_CACHE_SIZE):
            self.write_cached_tweets

    def write_cached_tweets(self):
        towrite = [(self.write_cache[d]['id'], self.write_cache[d]['hash'], self.write_cache[d]['text']) for d in self.write_cache]
        self.data.executemany("INSERT INTO tweets VALUES (?, ?, ?)", towrite)
        self.data.commit()
        self.hashes |= self.write_cache_hashes
        self.write_cache = dict()
        self.write_cache_hashes = set()

    def get(self, tweet_hash):
        # if hit isn't in data, check if it's still in the cache
        tweet = None
        if (tweet_hash in self.write_cache_hashes):
            self.debug_used_cache_count += 1
            tweet = self.write_cache[tweet_hash]
        else:
            cursor = self.data.cursor()
            cursor.execute("SELECT id, hash, text FROM tweets WHERE hash=:hash",
                                 {"hash": tweet_hash})
            result = cursor.fetchone()
            if result:
                tweet = self.tweet_from_sql(result)
        if not tweet:
            logging.debug('failed to retreive tweet')
        return tweet

    def pop(self, tweet_hash):
        result = self.get(tweet_hash)
        self.remove(tweet_hash)
        return result

    def remove(self, tweet_hash):
        if tweet_hash in self.write_cache:
            # if this hit is still in the cache, write the cache before continuing
            self.write_cached_tweets()
        cursor = self.data.cursor()
        cursor.execute("DELETE FROM tweets WHERE hash=:hash",
                       {"hash": tweet_hash})
        self.data.commit()
        # delete from hashes
        self.hashes.remove(tweet_hash)

    def add_hit(self, hit):
        cursor = self.hitsdb.cursor()
        cursor.execute("INSERT INTO hits VALUES (?,?,?,?,?,?)",
                      (str(hit['id']), hit['status'],
                       str(hit['tweet_one']['id']),
                       str(hit['tweet_two']['id']),
                       hit['tweet_one']['text'],
                       hit['tweet_two']['text'])
                       )
        self.hitsdb.commit()

    def get_hit(self, hit_id):
        cursor = self.hitsdb.cursor()
        cursor.execute("SELECT * FROM hits WHERE hit_id=:id",
                       {"id": str(hit_id)})
        result = cursor.fetchone()
        return self.hit_from_sql(result)

    def remove_hit(self, hit_id):
        cursor = self.hitsdb.cursor()
        cursor.execute("DELETE FROM hits WHERE hit_id=:id",
                       {"id": str(hit_id)})
        self.hitsdb.commit()

    def set_hit_status(self, hit_id, status):
        if status not in [HIT_STATUS_REVIEW, HIT_STATUS_MISC,
                          HIT_STATUS_APPROVED, HIT_STATUS_POSTED,
                          HIT_STATUS_REJECTED, HIT_STATUS_FAILED]:
            return False
        # get the hit, delete the hit, add it again with new status.
        hit = self.get_hit(hit_id)
        hit['status'] = status
        self.remove_hit(hit_id)
        self.add_hit(hit)

    def get_all_hits(self):
        cursor = self.hitsdb.cursor()
        cursor.execute("SELECT * FROM hits")
        results = cursor.fetchall()
        hits = []
        for item in results:
            hits.append(self.hit_from_sql(item))
        return hits

    def hit_from_sql(self, item):
        """
        convenience method for converting the result of an sql query
        into a python dictionary compatable with anagramer
        """
        return {'id': long(item[0]),
                'status': str(item[1]),
                'tweet_one': {'id': long(item[2]), 'text': str(item[4])},
                'tweet_two': {'id': long(item[3]), 'text': str(item[5])}
                }

    def tweet_from_sql(self, sql_tweet):
        return {'id': long(sql_tweet[0]), 'hash': str(sql_tweet[1]), 'text': str(sql_tweet[2])}

    def finish(self):
        if not self.just_the_hits:
            self.write_cached_tweets()
            print('datahandler closing with %i tweets' % (len(self.hashes)))
            print('write cache hit %i times' % self.debug_used_cache_count)
        if (len(self.fetch_pool)):
            self.batch_fetch()
        if self.data:
            self.data.close()
        if self.hitsdb:
            self.hitsdb.close()

    # functions for handling hit processing

    def reject_hit(self, hit_id):
        self.set_hit_status(hit_id, HIT_STATUS_REJECTED)
        return True

    def post_hit(self, hit_id):
        if self.twitterhandler.post_hit(self.get_hit(hit_id)):
            self.set_hit_status(hit_id, HIT_STATUS_POSTED)
            return True
        else:
            self.set_hit_status(hit_id, HIT_STATUS_FAILED)
            return False

    def approve_hit(self, hit_id):
        self.set_hit_status(hit_id, HIT_STATUS_APPROVED)
        return True

    def review_hits(self):
        """
        this is a simple command line tool for reviewing and categorizing
        potential anagrams as they come in. It's intended as a stopgap/
        fallback pending my adding a more elegent solution.
        """
        # should only be run with the just_the_hits flag
        if not self.just_the_hits:
            return
        allhits = self.get_all_hits()
        hits = [h for h in allhits if h['status'] in [HIT_STATUS_REVIEW]]
        for hit in allhits:
            print(hit['tweet_one']['text'], hit['tweet_one']['id'], hit['status'])
            print(hit['tweet_two']['text'], hit['tweet_two']['id'])
        print('db contains %i hits' % len(allhits))
        print('recorded %i hits in need of review' % len(hits))
        # show hit and input prompt
        for hit in hits:
            print(hit['tweet_one']['id'], hit['tweet_two']['id']), hit['status']
            print(hit['tweet_one']['text'])
            print(hit['tweet_two']['text'])
            while 1:
                inp = raw_input("(a)ccept, (r)eject, (s)kip, (i)llustrate, (q)uit:")
                if inp == 'i':
                    utils.show_anagram(hit['tweet_one']['text'], hit['tweet_two']['text'])
                    continue
                if inp not in ['a', 'r', 's', 'q', 'i']:
                    print("invalid input. Please enter 'a', 'r', 's', 'i' or 'q'.")
                else:
                    break
            if inp == 'a':
                if not self.post_hit(hit['id']):
                    print('retweet failed, sorry bud')
                else:
                    print('post successful')
            if inp == 'r':
                # remove from list of hits
                self.reject_hit(hit['id'])
            if inp == 's':
                self.approve_hit(hit['id'])
            if inp == 'q':
                break


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
    print('extracted %i hashes in %s' % (len(hashes), utils.format_seconds(time.time()-load_time)))
    short_hashes = [h for h in hashes if len(h) < cutoff]
    print("found %i of %i hashes below %i character cutoff" % (len(short_hashes), len(hashes), cutoff))
    load_time = time.time()
    hashvals = ["'%s'" % h for h in short_hashes]
    db.execute("DELETE FROM tweets WHERE hash IN (%s)" % ",".join(hashvals))
    # self.cache.executemany("DELETE FROM tweets WHERE hash=(?)", iter(short_hashes))
    db.commit()
    print('deleted %i hashes in %s' % (len(short_hashes), utils.format_seconds(time.time()-load_time)))
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
    print('extracted %i ids in %s' % (len(ids), utils.format_seconds(time.time()-load_time)))
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
    print('archived %i hashes in %s' % (len(ids), utils.format_seconds(time.time()-load_time)))


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
