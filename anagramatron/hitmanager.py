# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sqlite3 as lite
import os
import time
import logging

from twitter.api import TwitterError

from . import anagramfunctions, twitterhandler, common

HIT_STATUS_REVIEW = 'review'
HIT_STATUS_SEEN = 'seen'
HIT_STATUS_REJECTED = 'rejected'
HIT_STATUS_POSTED = 'posted'
HIT_STATUS_APPROVED = 'approved'
HIT_STATUS_FAILED = 'failed'


class HitDBManager(object):

    """docstring for HitDBManager"""
    SQL_SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schema.sql')
    # we make ids from unix time so this is in 2255 somewhere
    MAX_HIT_ID = 9000000000000

    def __init__(self, dbpath, _testing=False):
        super(HitDBManager, self).__init__()
        self.dbpath = os.path.join(common.ANAGRAM_DATA_DIR, dbpath)
        self.hitsdb = self._setup()
        self.twitter_handler = twitterhandler.TwitterHandler()
        self.hits_counter = 0
        self._testing = _testing

    def _setup(self):
        if os.path.exists(self.dbpath):
            return lite.connect(self.dbpath)
        else:
            with open(self.SQL_SCHEMA, 'r') as f:
                db = lite.connect(self.dbpath)
                db.cursor().executescript(f.read())
                db.commit()
                return db

    # public API
    def new_hit(self, first, second):

        hit = {
            "id": int(time.time()*1000),
            "status": HIT_STATUS_REVIEW,
            "hash": first['tweet_hash'],
            "tweet_one": first,
            "tweet_two": second
        }

        if self._hit_collides_with_previous_hit(hit):
            return
        # stats.hit() 
        try:
            if not self._testing:
                hit = self._fetch_hit_tweets(hit)
            self.hits_counter += 1
            self._add_hit(hit)
        except TwitterError as err:
            print('tweet missing, will pass')

    def all_hits(self, with_status=None, max_id=MAX_HIT_ID, result_count=None):
        query = "SELECT * FROM hits WHERE hit_id < :hit_id"
        if with_status:
            query = "%s AND hit_status =:hit_status" % query

        query = "%s ORDER BY hit_id DESC" % query
        if result_count:
            query = "%s LIMIT :result_count" % query
        cursor = self.hitsdb.cursor()
        cursor.execute(
            query, {'hit_id': max_id, 'hit_status': with_status, 'result_count': result_count})
        return [self.hit_from_sql(h) for h in cursor.fetchall()]

    def hits_newer_than_hit(self, hit_id):
        cursor = self.hitsdb.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM hits WHERE hit_id > (?)", (hit_id,))
        return cursor.fetchone()[0]

    def new_hits_count(self):
        return self.hits_count(HIT_STATUS_REVIEW)

    def hits_count(self, status):
        try:
            cursor = self.hitsdb.cursor()
            cursor.execute("SELECT COUNT(*) FROM hits WHERE hit_status = (?)",
                           (status,))
            return cursor.fetchone()[0]
        except ValueError:
            return "420"

    def last_post_time(self):
        """return the time of the last successful post"""
        cursor = self.hitsdb.cursor()
        cursor.execute("SELECT * from hitinfo")
        results = cursor.fetchall()
        results = [float(x[0]) for x in results]
        if len(results):
            return max(results)

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

    def seen_hits(self, hit_ids):
        """this has been replaced by the more general update_status"""
        self.update_status_for_hits(hit_ids, HIT_STATUS_SEEN)

    def update_status_for_hits(self, hit_ids, new_status):
        placeholder = ', '.join('?' for h in hit_ids)
        query = "UPDATE hits SET hit_status = '%s' WHERE hit_id IN (%s)" % (
            new_status, placeholder)
        cursor = self.hitsdb.cursor()
        cursor.execute(query, hit_ids)
        self.hitsdb.commit()

    def set_hit_status(self, hit_id, status):
        cursor = self.hitsdb.cursor()
        cursor.execute("UPDATE hits SET hit_status = :status WHERE hit_id = :hit_id",
                       {
                           'status': status,
                           'hit_id': str(hit_id)
                       })
        self.hitsdb.commit()
        return True

    def next_approved_hit(self):
        """ no, this is not particuarly efficient """
        hits = self.all_hits(HIT_STATUS_APPROVED)
        hits = sorted(hits, key=lambda k: k['id'])
        if len(hits):
            return hits.pop()

    def reject_hit(self, hit_id):
        self.set_hit_status(hit_id, HIT_STATUS_REJECTED)
        return True

    def post_hit(self, hit_id):
        if self._testing or self.twitter_handler.post_hit(self.get_hit(hit_id)):
            self.set_hit_status(hit_id, HIT_STATUS_POSTED)
            # keep track of most recent post:
            cursor = self.hitsdb.cursor()
            cursor.execute(
                "INSERT INTO hitinfo VALUES (?)", (str(time.time()),))
            self.hitsdb.commit()
            return True
        else:
            self.set_hit_status(hit_id, HIT_STATUS_FAILED)
            return False

    def queue_hit(self, hit_id):
        cursor = self.hitsdb.cursor()
        cursor.execute("INSERT INTO post_queue VALUES (?)", (str(hit_id),))
        self.hitsdb.commit()

    def get_queued_hits(self):
        cursor = self.hitsdb.cursor()
        cursor.execute("SELECT * FROM post_queue")
        hits = cursor.fetchall()
        return [h[0] for h in hits]

    def post_queued_hit(self, hit_id):
        cursor = self.hitsdb.cursor()
        cursor.execute(
            "DELETE FROM post_queue WHERE hit_id = (?)", (str(hit_id),))
        self.hitsdb.commit()
        return self.post_hit(hit_id)

    def approve_hit(self, hit_id):
        self.set_hit_status(hit_id, HIT_STATUS_APPROVED)
        return True

    # private API etc
    def _add_hit(self, hit):
        cursor = self.hitsdb.cursor()
        cursor.execute("INSERT INTO hits VALUES (?,?,?,?,?,?)",
                       (str(hit['id']),
                        hit['status'],
                        str(time.time()),
                        str(hit['hash']),
                        repr(hit['tweet_one']),
                        repr(hit['tweet_two'])
                        ))
        self.hitsdb.commit()

    def _hit_collides_with_previous_hit(self, hit):
        cursor = self.hitsdb.cursor()
        cursor.execute(
            "SELECT * FROM hits WHERE hit_hash=?", (hit['hash'], ))
        result = cursor.fetchone()
        if result:
            return True
        return False

    def hit_from_sql(self, item):
        """
        convenience method for converting the result of an sql query
        into a python dictionary compatable with anagramer
        """
        return {'id': int(item[0]),
                'status': str(item[1]),
                'timestamp': item[2],
                'hash': str(item[3]),
                'tweet_one': eval(item[4]),
                'tweet_two': eval(item[5])
                }

    # twitter stuff:

    def _fetch_hit_tweets(self, hit):
        """
        attempts to fetch tweets in hit.
        if successful builds up more detailed hit object.
        returns the input hit unchaged on failure
        """

        t1 = self.twitter_handler.fetch_tweet(hit['tweet_one']['tweet_id'])
        t2 = self.twitter_handler.fetch_tweet(hit['tweet_two']['tweet_id'])
        if t1 and t2:
            hit['tweet_one']['fetched'] = self._cleaned_tweet(t1)
            hit['tweet_two']['fetched'] = self._cleaned_tweet(t2)

        return hit

    def dump_json(self, filename='hit_export.json'):
        """exports all hits as json"""
        import json
        if os.path.exists(filename):
            print('%s exists, please move it before exporting again' %
                  filename)
            sys.exit(1)

        hits = self.all_hits()
        json.dump(hits, open(filename, 'wb'))

    def _cleaned_tweet(self, tweet):
        """
        returns a dict of desirable twitter info
        """
        twict = dict()
        twict['text'] = anagramfunctions.correct_encodings(tweet.get('text'))
        twict['user'] = {
            'name': tweet.get('user').get('name'),
            'screen_name': tweet.get('user').get('screen_name'),
            'profile_image_url': tweet.get('user').get('profile_image_url')
        }
        twict['created_at'] = tweet.get('created_at')
        return twict


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="manages hit storage and access")
    parser.add_argument(
        '-r', '--review', help='run the hit review command line tool', action="store_true")
    parser.add_argument(
        '-p', '--post', help='with -r, review approved hits for posting', action="store_true")
    parser.add_argument('--json', help='export hits to json', type=str)
    args = parser.parse_args()

    if args.review:
        raise NotImplementedError('I took this out. sorry bud')

    hm = HitDBManager()
    if args.json:
        dump_json(args.json)


if __name__ == "__main__":
    main()
