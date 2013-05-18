from __future__ import print_function
import sqlite3 as lite
import os
import shutil

TWEET_DB_PATH = 'data/tweetcache.db'

class DataHandler(object):
    """
    handles storage and retrieval of tweets
    """
    def __init__(self):
        self.data = None
        self.lookup_table = None
        self.setup()

    def setup(self):
        """
        creates database if it doesn't already exist
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
        print(hashes)

        # setup the lookup table;
        self.lookup_table = lite.connect(:memory:)
        lookup_cursor = self.lookup_table.cursor()
        lookup_cursor.execute("CREATE TABLE hashes(hash text)")
        lookup_cursor.cursor.executemany("INSERT INTO hashes VALUES (?)", hashes)

    def contains(self, tweet_hash):
        cursor = self.lookup_table.cursor()
        cursor.execute("SELECT hash FROM hashes WHERE hash=:hash",
            {"hash":tweet_hash})
        if cursor.fetchone():
            return True
        else:
            return False

    def add(self, tweet):
        cursor = self.data.cursor()
        cursor.execute("INSERT INTO tweets VALUES (?,?,?)", (str(tweet['id']), tweet['hash'], tweet['text']))
        self.data.commit()

    def get(self, tweet_hash):
        cursor = self.data.cursor()
        cursor.execute("SELECT id_str, hash, text FROM tweets WHERE hash=:hash",
            {"hash":tweet_hash})
        result = cursor.fetchone()
        if result:
            return {'id': long(result[0]), 'hash': str(result[1]), 'text': str(result[2])}
        return None

    def pop(self, tweet_hash):
        result = self.get(tweet_hash)
        cursor = self.data.cursor()
        cursor.execute("DELETE FROM tweets WHERE hash=:hash",
        {"hash":tweet_hash})
        self.data.commit()
        if result:
            return result
        return None

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
            'tweet_one':{'id': long(item[1]), 'text': str(item[3])},
            'tweet_two':{'id': long(item[2]), 'text': str(item[4])}}

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

    def finish(self):
        if self.data:
            self.data.close()
        if self.lookup_table:
            self.lookup_table.close()

if __name__ == "__main__":
    dh = DataHandler()
    dh.add_from_file('testdata/tst100.p')
