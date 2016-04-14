
import time
import sys

from collections import defaultdict

from . import anagramfunctions


class StatTracker(object):
    __instance = None

    def __new__(cls):
        if StatTracker.__instance is None:
            StatTracker.__instance = object.__new__(cls)
        return StatTracker.__instance

    def __init__(self):
        self.start_time = time.time()
        self.stats = defaultdict(int)

    def __getitem__(self, key):
        return self.stats[key]

    def __setitem__(self, key, value):
        self.stats[key] = value

    def __str__(self):
        seen_perc = self.stats['passed_filter']/(self.stats['tweets_seen'] or 1)
        seen_perc *= 100
        runtime = time.time() - self.start_time
        cache_hit_perc = (
            self['cache_hits'] /
            ((self['possible_hits'] + self['fetch_pool_size'] + self['cache_hits']) or 1)
            ) * 100
        status = "seen %d, used %d (%0.1f%%), hits(/in cache) %d/%d (%0.1f%%), \
agrams %d, cachesize %d, buffered %d, runtime %s" % (
            self['tweets_seen'], self['passed_filter'], seen_perc,
            self['possible_hits'] + self['fetch_pool_size'], self['cache_hits'],
            cache_hit_perc, self['hits'], self['cache_size'], self['buffer'],
            anagramfunctions.format_seconds(runtime)
            )
        return status

    def print_stats(self):
        sys.stdout.write('%s\r' % str(self))
        sys.stdout.flush()

    def stats_dict(self):
        return {
            'tweets_seen': self['tweets_seen'],
            'passed_filter': self['passed_filter'],
            'possible_hits': self['possible_hits'],
            'hits': self['hits'],
            'start_time': self.start_time
        }
