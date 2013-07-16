import sqlite3 as lite
import os
import time

import anagramconfig
from twitterhandler import TwitterHandler
HIT_PATH_COMPONENT = 'hitdata'

HIT_STATUS_REVIEW = 'review'
HIT_STATUS_REJECTED = 'rejected'
HIT_STATUS_POSTED = 'posted'
HIT_STATUS_APPROVED = 'approved'
HIT_STATUS_MISC = 'misc'
HIT_STATUS_FAILED = 'failed'

dbpath = None
hitsdb = None
twitter_handler = None


def _setup(languages=['en']):
    global dbpath, hitsdb
    dbpath = (anagramconfig.STORAGE_DIRECTORY_PATH +
              HIT_PATH_COMPONENT +
              '_'.join(languages) + '.db')

    if not os.path.exists(dbpath):
        hitsdb = lite.connect(dbpath)
        cursor = hitsdb.cursor()
        print('hits db not found, creating')
        cursor.execute("""CREATE TABLE hits
            (hit_id integer, hit_status text, hit_date INTEGER, hit_hash TEXT, hit_rating text, flags TEXT, one_id text, two_id text, one_text text, two_text text)""")
        cursor.execute("CREATE TABLE blacklist (bad_hash TEXT)")
        hitsdb.commit()
    else:
        hitsdb = lite.connect(dbpath)


def _checkit():
    if not dbpath or hitsdb:
        _setup()


def new_hit(first, second):
    _checkit()
    hit = {
           "id": int(time.time()*1000),
           "status": HIT_STATUS_REVIEW,
           "hash": first['tweet_hash'],
           "tweet_one": first,
           "tweet_two": second
        }
    _add_hit(hit)


def _add_hit(hit):
    cursor = hitsdb.cursor()

    cursor.execute("INSERT INTO hits VALUES (?,?,?,?,?,?,?,?,?,?)",
                  (str(hit['id']),
                   hit['status'],
                   str(time.time()),
                   str(hit['hash']),
                   '0',
                   '0',
                   str(hit['tweet_one']['tweet_id']),
                   str(hit['tweet_two']['tweet_id']),
                   hit['tweet_one']['tweet_text'],
                   hit['tweet_two']['tweet_text'])
                   )
    hitsdb.commit()


def get_hit(hit_id):
    _checkit()
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * FROM hits WHERE hit_id=:id",
                   {"id": str(hit_id)})
    result = cursor.fetchone()
    return hit_from_sql(result)


def remove_hit(hit_id):
    _checkit()
    cursor = hitsdb.cursor()
    cursor.execute("DELETE FROM hits WHERE hit_id=:id",
                   {"id": str(hit_id)})
    hitsdb.commit()


def set_hit_status(hit_id, status):
    _checkit()
    if status not in [HIT_STATUS_REVIEW, HIT_STATUS_MISC,
                      HIT_STATUS_APPROVED, HIT_STATUS_POSTED,
                      HIT_STATUS_REJECTED, HIT_STATUS_FAILED]:
        return False
    # get the hit, delete the hit, add it again with new status.
    hit = get_hit(hit_id)
    hit['status'] = status
    remove_hit(hit_id)
    _add_hit(hit)


def all_hits():
    _checkit()
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * FROM hits")
    results = cursor.fetchall()
    hits = []
    for item in results:
        hits.append(hit_from_sql(item))
    return hits


def hit_from_sql(item):
    """
    convenience method for converting the result of an sql query
    into a python dictionary compatable with anagramer
    """
    return {'id': long(item[0]),
            'status': str(item[1]),
            'timestamp': item[2],
            'hash': str(item[3]),
            'rating': str(item[4]),
            'flags': str(item[5]),
            'tweet_one': {'tweet_id': long(item[6]), 'tweet_text': item[8]},
            'tweet_two': {'tweet_id': long(item[7]), 'tweet_text': item[9]}
            }


def add_to_blacklist(bad_hash):
    _checkit()
    cursor = hitsdb.cursor()
    cursor.execute("INSERT INTO blacklist VALUES (?)", (bad_hash,))
    cursor.commit()


def reject_hit(hit_id):
    set_hit_status(hit_id, HIT_STATUS_REJECTED)
    return True


def post_hit(hit_id):
    if not twitter_handler:
        twitter_handler = TwitterHandler()
    if twitter_handler.post_hit(get_hit(hit_id)):
        set_hit_status(hit_id, HIT_STATUS_POSTED)
        return True
    else:
        set_hit_status(hit_id, HIT_STATUS_FAILED)
        return False

def approve_hit(hit_id):
    set_hit_status(hit_id, HIT_STATUS_APPROVED)
    return True
