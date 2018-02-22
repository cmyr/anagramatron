#!/usr/bin/env python3
# -*- coding: utf-8 -*-#

'''A simple tool for profiling over stdin'''
import sys
import tempfile
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
    tempdir = tempfile.TemporaryDirectory()
    print("storing in temp dir %s" % tempdir, file=sys.stderr)

    finder = anagramfinder.AnagramFinder(storage='mdbm', hit_callback=stats, path=tempdir.name)
    for line in sys.stdin:
        stats.seen += 1
        finder.handle_input(line)

    for one, two in stats.hits:
        print("---------\n{}--↕︎--\n{}".format(one, two));
    print("seen {}, hits {}".format(stats.seen, len(stats.hits)))


if __name__ == "__main__":
    main()
