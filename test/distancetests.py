# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals


import cPickle as pickle
import distance
import difflib



import anagramfunctions
import anagramfinder

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


def print_hit(one, two):
    print(one['tweet_text'], two['tweet_text'], sep='\n')

def ez_filter(one, two):
    return True

comparitor = difflib.SequenceMatcher()

def hamming_distance_calculator(one, two):
    t1 = anagramfunctions.stripped_string(one["tweet_text"])
    t2 = anagramfunctions.stripped_string(two["tweet_text"])

    comparitor.set_seqs(t1, t2)
    dist = 1.0 - float(distance.hamming(t1, t2)) / len(t1)
    if dist < 1:
        print(t1, t2, str(dist), str(comparitor.ratio()) + "\n\n", sep="\n")

    

def main():
    datacoordinator = anagramfinder.AnagramFinder(
        storage_location="testdata/",
        hit_handler=hamming_distance_calculator,
        anagram_test=ez_filter
        )

    tweets = load_test_tweets()
    # print(type(tweets))
    for t in tweets:
        # print(type(t))
        # print(t[0])
        # print(t.get('tweet_text'))
        datacoordinator.handle_input(t)
    print('handled %d tweets' % len(tweets))



if __name__ == "__main__":
    main()