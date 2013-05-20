from __future__ import print_function
import sqlite3 as lite
import os
import logging

TWEET_DB_PATH = 'data/tweetcache.db'
# TWEET_DB_PATH = 'data/testdb.db'
# TWEET_DB_PATH = 'data/blankdb.db'


class DataHandler(object):
    """
    handles storage and retrieval of tweets
    """
    def __init__(self):
        self.data = None
        self.cache = None
        self.hashes = set()
        self.setup()

    def setup(self):
        """
        creates database if it doesn't already exist
        populates hash table
        """
        if not os.path.exists(TWEET_DB_PATH):
            self.data = lite.connect(TWEET_DB_PATH)
            cursor = self.data.cursor()
            print('db not found, creating')
            cursor.execute("CREATE TABLE tweets(id_str text, hash text, text text)")
            cursor.execute("""CREATE TABLE hits
                (hit_id_str text, one_id text, two_id text, one_text text, two_text text)""")
            self.data.commit()
        else:
            self.data = lite.connect(TWEET_DB_PATH)
        # extract hashes for adding to the lookup table
        cursor = self.data.cursor()
        cursor.execute("SELECT hash FROM tweets")
        hashes = cursor.fetchall()
        logging.debug('loaded %d hashes' % (len(hashes)))
        # setup the lookup table;
        self.cache = lite.connect(':memory:')
        cache_cursor = self.cache.cursor()
        self.hashes = set([str(h) for (h,) in hashes])
        # setup the cache
        cache_cursor.execute("CREATE TABLE cache(id_str text, hash text, text text)")
        self.cache.commit()
        self.load_cache()

    def contains(self, tweet_hash):
        if tweet_hash in self.hashes:
            return True
        else:
            return False

    def count(self):
        cursor = self.data.cursor()
        cursor.execute("SELECT Count() FROM tweets")
        diskcount = cursor.fetchone()
        cache_cursor = self.cache.cursor()
        cache_cursor.execute("SELECT Count() FROM cache")
        cachecount = cache_cursor.fetchone()
        return (diskcount, cachecount)

    def count_hashes(self):
        return len(self.hashes)

    def add(self, tweet):
        cursor = self.cache.cursor()
        cursor.execute("INSERT INTO cache VALUES (?,?,?)", (str(tweet['id']), tweet['hash'], tweet['text']))
        self.hashes.add(tweet['hash'])
        self.cache.commit()

    def get(self, tweet_hash):
        # if hit isn't in data, check if it's still in the cache
        cache_cursor = self.cache.cursor()
        cache_cursor.execute("SELECT id_str, hash, text FROM cache WHERE hash=:hash",
        {"hash": tweet_hash})
        result = cache_cursor.fetchone()
        if result:
            return {'id': long(result[0]), 'hash': str(result[1]), 'text': str(result[2])}
        return None

    def pop(self, tweet_hash):
        result = self.get(tweet_hash)
        self.remove(tweet_hash)
        return result

    def remove(self, tweet_hash):
        # delete any entries in data & cache
        cache_cursor = self.cache.cursor()
        cache_cursor.execute("DELETE FROM cache WHERE hash=:hash",
            {"hash": tweet_hash})
        self.cache.commit()
        # delete from hashes
        self.hashes.remove(tweet_hash)

    def add_hit(self, hit):
        cursor = self.data.cursor()
        cursor.execute("INSERT INTO hits VALUES (?,?,?,?,?)",
            (str(hit['id']), str(hit['tweet_one']['id']), str(hit['tweet_two']['id']),
            hit['tweet_one']['text'], hit['tweet_two']['text'])
            )
        self.data.commit()

    def get_hit(self, hit_id):
        cursor = self.data.cursor()
        cursor.execute("SELECT hit_id_str one_id, two_id, one_text, two_text FROM hits WHERE hit_id_str=:id",
            {"id": hit_id})
        result = cursor.fetchone()
        return self.hit_from_sql(result)

    def get_all_hits(self):
        cursor = self.data.cursor()
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
            'tweet_one': {'id': long(item[1]), 'text': str(item[3])},
            'tweet_two': {'id': long(item[2]), 'text': str(item[4])}}

    def add_from_file(self, filename):
        """
        utility function for loading archived tweets
        """
        import cPickle as pickle
        data = pickle.load(open(filename, 'r'))
        print("loaded data of type:", type(data), "size: ", len(data))
        dlist = [data[d] for d in data]
        tlist = [(str(d['id']), d['hash'], d['text']) for d in dlist]
        cursor = self.data.cursor()
        cursor.executemany("INSERT INTO tweets VALUES (?, ?, ?)", tlist)
        self.data.commit()

    def remove_hit(self, hit):
        pass

    def load_cache(self):
        cursor = self.data.cursor()
        cursor.execute("SELECT * FROM tweets")
        results = cursor.fetchall()
        cache_cursor = self.cache.cursor()
        cache_cursor.executemany("INSERT INTO cache VALUES (?, ?, ?)", results)
        self.cache.commit()
        logging.debug('loaded %g tweets to cache' % (self.count()[1]))

    def write_cache(self):
        """
        write the cache to disk
        """
        cache_cursor = self.cache.cursor()
        cache_cursor.execute("SELECT * FROM cache")
        results = cache_cursor.fetchall()
        cursor = self.data.cursor()
        cursor.execute("DROP TABLE IF EXISTS tweets")
        cursor.execute("CREATE TABLE tweets(id_str text, hash text, text text)")
        cursor.executemany("INSERT INTO tweets VALUES (?, ?, ?)", results)
        self.data.commit()
        cache_cursor.execute("DELETE FROM cache")
        self.cache.commit()

    def finish(self):
        self.write_cache()
        print('datahandler closing with %g tweets' % (self.count()[0]))
        if self.data:
            self.data.close()
        if self.cache:
            self.cache.close()

if __name__ == "__main__":
    dh = DataHandler()
