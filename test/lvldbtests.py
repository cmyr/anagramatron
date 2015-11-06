# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import cProfile
from memory_profiler import profile

import plyvel
import gdbm
import cPickle as pickle
from anagramfunctions import improved_hash

def do_cprofile(func):
    def profiled_func(*args, **kwargs):
        profile = cProfile.Profile()
        try:
            profile.enable()
            result = func(*args, **kwargs)
            profile.disable()
            return result
        finally:
            profile.print_stats()
    return profiled_func

# @do_cprofile

def load_test_tweets():
    test_data_files = [
    "testdata/filt_Dec151310.p",
    "testdata/filt_Dec151347.p",
    "testdata/filt_Dec151357.p", 
    "testdata/filt_Dec191819.p", 
    "testdata/filt_Dec191855.p", 
    "testdata/filt_Dec191930.p"
    ]

    test_tweets = list()
    for path in test_data_files:
        test_tweets.extend(pickle.load(open(path)))
    return test_tweets

# def test_hashes():
#     return [improved_hash(t) for t in load_test_tweets()]
test_tweets = load_test_tweets()
gdb_hashes = [t['tweet_hash'] for t in test_tweets]
lvl_hashes = [bytes(t) for t in gdb_hashes]
# plyvel.repair_db('/Users/cmyr/tweetdbm/feb/lvlFeb012119.db/', bloom_filter_bits=10)

# @do_cprofile
@profile
def test_level_db():
    hit_count = 0
    lvldb = plyvel.DB('/Users/cmyr/tweetdbm/feb/lvlFeb012119.db/')
    for t in gdb_hashes:
        if lvldb.get(t):
            hit_count += 1
    print('found %d hits' % hit_count)

# @do_cprofile

@profile
def test_gdbm():
    db = gdbm.open('/Users/cmyr/tweetdbm/feb/mdbmFeb010304.db')
    hit_count = 0
    for t in gdb_hashes:
        if t in db:
            hit_count += 1
    print('found %d hits' % hit_count)            









def main():
    test_gdbm()
    test_level_db()

if __name__ == "__main__":
    main()