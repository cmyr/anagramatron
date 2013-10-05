from __future__ import print_function

import sqlite3 as lite
import os
import time

# import anagramconfig
import anagramfunctions
import anagramstats as stats
import logging
import sys

from twitterhandler import TwitterHandler
from twitter.api import TwitterError
from constants import STORAGE_DIRECTORY_PATH
HIT_PATH_COMPONENT = 'hitdata2'

HIT_STATUS_REVIEW = 'review'
HIT_STATUS_SEEN = 'seen'
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
    dbpath = (STORAGE_DIRECTORY_PATH +
              HIT_PATH_COMPONENT +
              '_'.join(languages) + '.db')

    if not os.path.exists(dbpath):
        hitsdb = lite.connect(dbpath)
        cursor = hitsdb.cursor()
        print('hits db not found, creating')
        cursor.execute("""CREATE TABLE hits
            (hit_id INTEGER, hit_status TEXT, hit_date INTEGER, hit_hash TEXT, hit_rating text, flags TEXT,
                tweet_one TEXT, tweet_two TEXT)""")
        cursor.execute("CREATE TABLE hitinfo (last_post REAL)")
        # cursor.execute("CREATE TABLE blacklist (bad_hash TEXT UNIQUE)")
        hitsdb.commit()
    else:
        hitsdb = lite.connect(dbpath)

# def _checkit():
#     if not dbpath or hitsdb:
#         _setup()


def new_hit(first, second):
    global _new_hits_counter
    
    hit = {
           "id": int(time.time()*1000),
           "status": HIT_STATUS_REVIEW,
           "hash": first['tweet_hash'],
           "tweet_one": first,
           "tweet_two": second
        }

    # if _hit_on_blacklist(hit):
    #     return
    if _hit_collides_with_previous_hit(hit):
        return

    stats.hit()
    try:
        hit = _fetch_hit_tweets(hit)
        _new_hits_counter += 1
        _add_hit(hit)
    except TwitterError as err:
        print('tweet missing, will pass')
        pass


def _fetch_hit_tweets(hit):
    """
    attempts to fetch tweets in hit.
    if successful builds up more detailed hit object.
    returns the input hit unchaged on failure
    """
    global twitter_handler
    if not twitter_handler:
        twitter_handler = TwitterHandler()

    t1 = twitter_handler.fetch_tweet(hit['tweet_one']['tweet_id'])
    t2 = twitter_handler.fetch_tweet(hit['tweet_two']['tweet_id'])
    if t1 and t2:
        hit['tweet_one']['fetched'] = _cleaned_tweet(t1)
        hit['tweet_two']['fetched'] = _cleaned_tweet(t2)

    return hit


def _cleaned_tweet(tweet):
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


def hits_newer_than_hit(hit_id):
    
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * FROM hits WHERE hit_id > (?)", (hit_id,))
    results = cursor.fetchall()
    return len(results)


def new_hits_count():
    
    cursor = hitsdb.cursor()
    try:
        cursor.execute("SELECT * FROM hits WHERE hit_status = (?)",
            (HIT_STATUS_REVIEW,))
        results = cursor.fetchall()
        return len(results)
    except ValueError:
        return "420"


def last_post_time():
    # return the time of the last successful post
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * from hitinfo")
    results = cursor.fetchall()
    if len(results):
        return results


# def _hit_on_blacklist(hit):
#     
#     cursor = hitsdb.cursor()
#     cursor.execute("SELECT count(*) FROM blacklist WHERE bad_hash=?", (hit['hash'],))
#     result = cursor.fetchone()[0]
#     if result == 1:
#         logging.debug('hit on blacklist: %s' % hit['tweet_one']['tweet_text'])
#         return True
#     return False


def _hit_collides_with_previous_hit(hit):
    
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

    cursor.execute("INSERT INTO hits VALUES (?,?,?,?,?,?,?,?)",
                  (str(hit['id']),
                   hit['status'],
                   str(time.time()),
                   str(hit['hash']),
                   '0',
                   '0',
                   repr(hit['tweet_one']),
                   repr(hit['tweet_two'])
                   ))
    hitsdb.commit()


def get_hit(hit_id):
    
    cursor = hitsdb.cursor()
    cursor.execute("SELECT * FROM hits WHERE hit_id=:id",
                   {"id": str(hit_id)})
    result = cursor.fetchone()
    return hit_from_sql(result)


def remove_hit(hit_id):
    
    cursor = hitsdb.cursor()
    cursor.execute("DELETE FROM hits WHERE hit_id=:id",
                   {"id": str(hit_id)})
    hitsdb.commit()


def set_hit_status(hit_id, status):
    
    if status not in [HIT_STATUS_REVIEW, HIT_STATUS_MISC, HIT_STATUS_SEEN,
                      HIT_STATUS_APPROVED, HIT_STATUS_POSTED,
                      HIT_STATUS_REJECTED, HIT_STATUS_FAILED]:
        print('invalid status')
        return False
    # get the hit, delete the hit, add it again with new status.
    hit = get_hit(hit_id)
    hit['status'] = status
    remove_hit(hit_id)
    _add_hit(hit)
    assert(get_hit(hit_id)['status'] == status)
    return True


def all_hits(with_status=None, cutoff_id=None):
    
    cursor = hitsdb.cursor()
    if not with_status:
        cursor.execute("SELECT * FROM hits")
    else:
        cursor.execute("SELECT * FROM hits WHERE hit_status = (?)", (with_status,))
    results = cursor.fetchall()
    hits = []
    for item in results:
        hits.append(hit_from_sql(item))
    if cutoff_id:
        hits = [h for h in hits if h['id'] > cutoff_id]
    return hits


# def blacklist():
#     cursor = hitsdb.cursor()
#     cursor.execute("SELECT * from blacklist")
#     results = cursor.fetchall()
#     return results


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
            'tweet_one': eval(item[6]),
            'tweet_two': eval(item[7])
            }


# def add_to_blacklist(bad_hash):
#     _checkit()
#     cursor = hitsdb.cursor()
#     cursor.execute("INSERT OR IGNORE INTO blacklist VALUES (?)", (bad_hash,))
#     cursor.execute("DELETE FROM hits WHERE hit_hash=?", (bad_hash,))
#     hitsdb.commit()


def reject_hit(hit_id):
    set_hit_status(hit_id, HIT_STATUS_REJECTED)
    return True


def post_hit(hit_id):
    global twitter_handler
    if not twitter_handler:
        twitter_handler = TwitterHandler()
    if twitter_handler.post_hit(get_hit(hit_id)):
        set_hit_status(hit_id, HIT_STATUS_POSTED)
        # keep track of most recent post:
        cursor = hitsdb.cursor()
        cursor.execute("INSERT INTO hitinfo VALUES (?)", (str(time.time()),))
        hitsdb.commit()
        return True
    else:
        set_hit_status(hit_id, HIT_STATUS_FAILED)
        return False


def approve_hit(hit_id):
    set_hit_status(hit_id, HIT_STATUS_APPROVED)
    return True

def review_hits(to_post=False):
    """
    manual tool for reviewing hits on the command line
    """
    
    status = HIT_STATUS_REVIEW if not to_post else HIT_STATUS_APPROVED
    hits = all_hits(status)
    hits = [(h, anagramfunctions.grade_anagram(h)) for h in hits]
    hits = sorted(hits, key= lambda k: k[1], reverse=True)
    hits = [h[0] for h in hits]

    print('found %i hits in need of review' % len(hits))
    while True:
        print(' anagram review (%i)'.center(80, '-') % len(hits))
        term_height = int(os.popen('stty size', 'r').read().split()[0])
        display_count = min(((term_height - 3) / 3), len(hits))
        display_hits = {k: hits[k] for k in range(display_count)}
        
        for h in display_hits:
            msg = "%s  %s" % (display_hits[h]['tweet_one']['tweet_id'], display_hits[h]['tweet_two']['tweet_id'])
            print(msg)
            print(str(h).ljust(9), display_hits[h]['tweet_one']['tweet_text'])
            print(' '*10, display_hits[h]['tweet_two']['tweet_text'])

        print('enter space seperated numbers of anagrams to approve. q to quit.')
        inp = raw_input(': ')
        if inp in [chr(27), 'x', 'q']:
            break

        approved = inp.split()
        print(approved)
        for h in display_hits:
            if str(h) in approved:
                if not to_post:
                    print('approved', h)
                    approve_hit(display_hits[h]['id'])
                else:
                    print('marked %i as posted' % h)
                    set_hit_status(display_hits[h]['id'], HIT_STATUS_POSTED)
            else:
                if not to_post:
                    set_hit_status(display_hits[h]['id'], HIT_STATUS_SEEN)
            hits.remove(display_hits[h])





_setup()

if __name__ == "__main__":
    args = sys.argv[1:]
    if "-r" in args:
        review_hits(True)


    review_hits()

