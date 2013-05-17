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
        self.setup()

    def setup(self):
        """
        creates database if it doesn't already exist
        """
        if not os.path.exists(TWEET_DB_PATH):
            self.data = sqlite3.connect(TWEET_DB_PATH)
            cursor = self.data.cursor()
            print('db not found, creating')
            cursor.execute("CREATE TABLE tweets(id_str text, hash text, text text)")
            cursor.execute("""CREATE TABLE hits
                (hit_id_str text, one_id text, two_id text, one_text text, two_text text)""")
            self.data.commit()
        else:
            self.data = sqlite3.connect(TWEET_DB_PATH)

    def add(self, tweet):
        cursor = self.data.cursor()
        cursor.execute("INSERT INTO tweets VALUES (?,?,?)", (str(tweet['id']), tweet['hash'], tweet['text']))
        self.data.commit()

    def get(self, tweet):
        cursor = self.data.cursor()
        cursor.execute("SELECT id_str, hash, text FROM tweets WHERE hash=:hash",
            {"hash":tweet['hash']})
        return cursor.fetchone()

    def finish(self):
        if con:
            con.close()

