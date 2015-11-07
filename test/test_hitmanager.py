
import os

from anagramatron import common, hitmanager

TEST_LOCATION = os.path.join(common.ANAGRAM_DATA_DIR, 'test_database.sqlite')

def test_setup():
    _cleanup()
    assert not os.path.exists(TEST_LOCATION)
    hm = hitmanager.HitDBManager(TEST_LOCATION, _testing=True)
    assert os.path.exists(TEST_LOCATION)

def test_schema():
    hm = hitmanager.HitDBManager(TEST_LOCATION, _testing=True)
    cursor = hm.hitsdb.cursor()
    cursor.execute("INSERT INTO hits VALUES (?,?,?,?,?,?)",
        ('123', 'review', '145.532', 'AABBCC', repr({'text:': 'tweetone'}), repr({'text:': 'tweettwo'}))
        )
    hm.hitsdb.commit()
    assert len(hm.all_hits()) == 1

def test_add_hit():
    _cleanup()
    hm = hitmanager.HitDBManager(TEST_LOCATION, _testing=True)
    first = {'text': 'aabbccddeeffgghh', 'tweet_hash': 'asfdlkj', 'tweet_id': 1}
    second = {'text': 'bbaacceeddgghhff', 'tweet_hash': 'asfdlkj', 'tweet_id': 2}
    hm.new_hit(first, second)

    hits = hm.all_hits()
    assert len(hits) == 1
    assert hm.new_hits_count() == 1

def test_queue():
    hm = hitmanager.HitDBManager(TEST_LOCATION, _testing=True)
    hm.queue_hit(1123)

    queued_hits = hm.get_queued_hits()
    assert len(queued_hits) == 1

def test_update_status():
    _cleanup()
    hm = hitmanager.HitDBManager(TEST_LOCATION, _testing=True)
    first = {'text': 'aabbccddeeffgghh', 'tweet_hash': 'asfdlkj', 'tweet_id': 1}
    second = {'text': 'bbaacceeddgghhff', 'tweet_hash': 'asfdlkj', 'tweet_id': 2}
    hm.new_hit(first, second)
    hits = hm.all_hits(with_status='review')
    assert len(hits) == 1
    hit = hits[0]
    hm.update_status_for_hits([hit['id']], 'seen')
    assert len(hm.all_hits(with_status='review')) == 0
    assert len(hm.all_hits(with_status='seen')) == 1

def test_posting():
    hm = hitmanager.HitDBManager(TEST_LOCATION, _testing=True)
    hits = hm.all_hits()
    assert len(hits)
    assert not hm.last_post_time()
    hit = hits[0]
    
    hm.post_hit(hit['id'])
    assert hm.last_post_time()







def _cleanup():
    if os.path.exists(TEST_LOCATION):
        os.remove(TEST_LOCATION)
