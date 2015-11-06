# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import anagramfinder

dc = anagramfinder.AnagramFinder(noload=True)

def test_input_handling():
    test1 = "this is just a string"
    result1 = dc._text_from_input(test1)
    assert(test1 == result1)

    test2 = {
    "text": "this is a dict",
    "otherkey:": False
    }

    result2 = dc._text_from_input(test2, key="text")
    assert(result2 == test2['text'])

    test3 = {
    "tweet_text": "this is a dict",
    "otherkey:": False
    }

    result3 = dc._text_from_input(test3, key="butts")
    assert(result3 == test3['tweet_text'])


def main():
    test_input_handling()

if __name__ == "__main__":
    main()