#!/usr/bin/env python3
# -*- coding: utf-8 -*-#

'''A simple tool for profiling over stdin'''
import sys
from . import anagramfinder


class Stats(object):
    def __init__(self):
        self.seen = 0
        self.hits = []

    def __call__(self, *args):
        assert len(args) == 2
        self.hits.append(tuple(args))


def main():
    stats = Stats()
    finder = anagramfinder.AnagramFinder(hit_callback=stats)
    for line in sys.stdin:
        stats.seen += 1
        finder.handle_input(line)

    print("seen {}, hits {}".format(stats.seen, len(stats.hits)))
    for one, two in stats.hits:
        print("---------\n{}--↕︎--{}".format(one, two));


if __name__ == "__main__":
    main()
