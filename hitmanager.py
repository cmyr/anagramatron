import sqlite3 as lite
import os
import time

import anagramconfig
import anagramfunctions
import anagramstats as stats
import logging

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
_new_hits_counter = 0


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
            (hit_id INTEGER, hit_status TEXT, hit_date INTEGER, hit_hash TEXT, hit_rating text, flags TEXT, one_id text, two_id text, one_text text, two_text text)""")
        cursor.execute("CREATE TABLE blacklist (bad_hash TEXT UNIQUE)")
        hitsdb.commit()
    else:
        hitsdb = lite.connect(dbpath)


def _checkit():
    if not dbpath or hitsdb:
        _setup()


def new_hit(first, second):
    global _new_hits_counter
    _checkit()
    hit = {
           "id": int(time.time()*1000),
           "status": HIT_STATUS_REVIEW,
           "hash": first['tweet_hash'],
           "tweet_one": first,
           "tweet_two": second
        }

    if _hit_on_blacklist(hit):
        return
    if _hit_collides_with_previous_hit(hit):
        return

    stats.hit()
    _new_hits_counter += 1
    _add_hit(hit)


def hits_newer_than_hit(hit_id):
    _checkit()
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * FROM hits WHERE hit_id > (?)", (hit_id,))
    results = cursor.fetchall()
    return len(results)


def new_hits_count():
    _checkit()
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * from hitinfo")
    try:
        last_hit = cursor.fetchall()[0][0]
        print (last_hit)
        if (last_hit):
            cursor.execute("SELECT * FROM hits WHERE hit_id > (?)", (last_hit,))
            results = cursor.fetchall()
            return len(results)
    except ValueError:
        return "420"


def _hit_on_blacklist(hit):
    _checkit()
    cursor = hitsdb.cursor()
    cursor.execute("SELECT count(*) FROM blacklist WHERE bad_hash=?", (hit['hash'],))
    result = cursor.fetchone()[0]
    if result == 1:
        logging.debug('hit on blacklist: %s' % hit['tweet_one']['tweet_text'])
        return True
    return False


def _hit_collides_with_previous_hit(hit):
    _checkit()
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * FROM hits WHERE hit_hash=?", (hit['hash'], ))
    result = cursor.fetchone()
    if result:
        # do some comparisons
        result = hit_from_sql(result)
        r1 = result['tweet_one']['tweet_text']
        r2 = result['tweet_two']['tweet_text']
        t1 = hit['tweet_one']['tweet_text']
        t2 = hit['tweet_two']['tweet_text']
        if anagramfunctions.test_anagram(r1, t1):
            # print('hit collision:', hit, result)
            logging.debug('hit collision: %s %s' % (r1, t1))
            return True
        if anagramfunctions.test_anagram(r1, t2):
            # print('hit collision:', hit, result)
            logging.debug('hit collision: %s %s' % (r1, t2))
            return True
        if anagramfunctions.test_anagram(r2, t1):
            # print('hit collision:', hit, result)
            logging.debug('hit collision: %s %s' % (r2, t1))
            return True
        if anagramfunctions.test_anagram(r2, t2):
            # print('hit collision:', hit, result)
            logging.debug('hit collision: %s %s' % (r2, t2))
            return True

    return False


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


def all_hits(with_status=None):
    _checkit()
    cursor = hitsdb.cursor()
    if not with_status:
        cursor.execute("SELECT * FROM hits")
    else:
        cursor.execute("SELECT * FROM hits WHERE hit_status = (?)", (with_status,))
    results = cursor.fetchall()
    hits = []
    for item in results:
        hits.append(hit_from_sql(item))
    return hits


def blacklist():
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * from blacklist")
    results = cursor.fetchall()
    return results


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
    cursor.execute("INSERT OR IGNORE INTO blacklist VALUES (?)", (bad_hash,))
    cursor.execute("DELETE FROM hits WHERE hit_hash=?", (bad_hash,))
    hitsdb.commit()


def reject_hit(hit_id):
    set_hit_status(hit_id, HIT_STATUS_REJECTED)
    return True


def post_hit(hit_id):
    global twitter_handler
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

def server_sent_hits(hits):
    newest_hit_sent = max([h['id'] for h in hits])
    print(newest_hit_sent)
    _checkit()
    cursor = hitsdb.cursor()
    try:
        cursor.execute("SELECT * FROM hitinfo")
        last_hit = cursor.fetchone()[0]
        if newest_hit_sent < last_hit:
            return
    except (lite.OperationalError, IndexError, TypeError):
        cursor.execute("DROP TABLE hitinfo")
        cursor.execute("CREATE TABLE hitinfo (last_hit INTEGER)")
        cursor.execute("INSERT INTO hitinfo VALUES (?)", (newest_hit_sent,))
        hitsdb.commit()
        print('inserted hit %i' % newest_hit_sent)
