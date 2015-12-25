
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
        cache_hit_perc = (self['cache_hits'] / ((self['possible_hits'] + self['fetch_pool_size'] + self['cache_hits']) or 1)) * 100
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
        

# _tweets_seen = 0
# _passed_filter = 0
# _possible_hits = 0
# _hits = 0
# _overflow = 0
# _start_time = time.time()
# _buffer = 0
# _max_buffer = 0
# _cache_hits = 0
# _cache_size = 0
# _fetch_pool_size = 0


# def clear_stats():
#     global _tweets_seen, _passed_filter, _possible_hits
#     global _hits, _overflow, _start_time, _buffer, _max_buffer
#     global _cache_size, _cache_hits, _fetch_pool_size

#     _tweets_seen = 0
#     _passed_filter = 0
#     _possible_hits = 0
#     _hits = 0
#     _overflow = 0
#     _start_time = time.time()
#     _buffer = 0
#     _max_buffer = 0
#     _cache_hits = 0
#     _cache_size = 0
#     _fetch_pool_size = 0


# def tweets_seen(seen=1):
#     global _tweets_seen
#     _tweets_seen += seen


# def passed_filter(passed=1):
#     global _passed_filter
#     _passed_filter += passed


# def possible_hit(possible=1):
#     global _possible_hits
#     _possible_hits += possible


# def hit(hit=1):
#     global _hits
#     _hits += hit


# def overflow(over=1):
#     global _overflow
#     _overflow += over


# def set_buffer(buffer_size):
#     global _buffer, _max_buffer
#     _buffer = buffer_size
#     if _buffer > _max_buffer:
#         _max_buffer = _buffer


# def set_fetch_pool_size(size):
#     global _fetch_pool_size
#     _fetch_pool_size = size


# def set_cache_size(size):
#     global _cache_size
#     _cache_size = size


# def cache_hit():
#     global _cache_hits
#     _cache_hits += 1


# def stats_dict():
#     return {
#             'tweets_seen': _tweets_seen,
#             'passed_filter': _passed_filter,
#             'possible_hits': _possible_hits,
#             'hits': _hits,
#             'start_time': _start_time
#             }

# def buffer_size():
#     return _buffer


# def update_console():
#     global _tweets_seen, _passed_filter, _possible_hits, _hits, _overflow
#     global _buffer, _start_time, _cache_hits, _cache_size

#     seen_percent = 0
#     if _tweets_seen > 0:
#         seen_percent = int(100*(float(_passed_filter)/_tweets_seen))
#     runtime = time.time()-_start_time

#     status = (
#         'tweets seen: ' + str(_tweets_seen) +
#         " passed filter: " + str(_passed_filter) +
#         " ({0}%)".format(seen_percent) +
#         " hits " + str(_possible_hits + _fetch_pool_size) + '/' + str(_cache_hits) +
#         " agrams: " + str(_hits) +
#         " cachesize: " + str(_cache_size) +
#         " buffer: " + str(_buffer) +
#         " runtime: " + anagramfunctions.format_seconds(runtime)
#     )
#     sys.stdout.write(status + '\r')
#     sys.stdout.flush()

