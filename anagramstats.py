from __future__ import print_function
import time
import sys

import anagramfunctions

_tweets_seen = 0
_passed_filter = 0
_possible_hits = 0
_hits = 0
_overflow = 0
_start_time = time.time()
_buffer = 0
_max_buffer = 0
_cache_hits = 0
_cache_size = 0
_fetch_pool_size = 0
# hit_distributions = [0 for x in range(140)]
# hash_distributions = [0 for x in range(140)]
# hitlist = []


def clear_stats():
    global _tweets_seen, _passed_filter, _possible_hits
    global _hits, _overflow, _start_time, _buffer, _max_buffer
    global _cache_size, _cache_hits, _fetch_pool_size

    _tweets_seen = 0
    _passed_filter = 0
    _possible_hits = 0
    _hits = 0
    _overflow = 0
    _start_time = time.time()
    _buffer = 0
    _max_buffer = 0
    _cache_hits = 0
    _cache_size = 0
    _fetch_pool_size = 0


# def new_hash(hash_text):
#     global hash_distributions
#     hashlength = len(hash_text)
#     if (hashlength < 140):
#         hash_distributions[hashlength] += 1


# def new_hit(self, hash_text):
#     global hitlist
#     global hit_distributions

#     hitlist.append(hash_text)
#     hashlength = len(hash_text)
#     if (hashlength < 140):
#         hit_distributions[hashlength] += 1

def tweets_seen(seen=1):
    global _tweets_seen
    _tweets_seen += seen


def passed_filter(passed=1):
    global _passed_filter
    _passed_filter += passed


def possible_hit(possible=1):
    global _possible_hits
    _possible_hits += possible


def hit(hit=1):
    global _hits
    _hits += hit


def overflow(over=1):
    global _overflow
    _overflow += over


def set_buffer(buffer_size):
    global _buffer, _max_buffer
    _buffer = buffer_size
    if _buffer > _max_buffer:
        _max_buffer = _buffer


def set_fetch_pool_size(size):
    global _fetch_pool_size
    _fetch_pool_size = size


def set_cache_size(size):
    global _cache_size
    _cache_size = size


def cache_hit():
    global _cache_hits
    _cache_hits += 1


def stats_dict():
    return {
            'tweets_seen': _tweets_seen,
            'passed_filter': _passed_filter,
            'possible_hits': _possible_hits,
            'hits': _hits,
            'overflow': _overflow,
            'start_time': _start_time
            }


def update_console():
    global _tweets_seen, _passed_filter, _possible_hits, _hits, _overflow
    global _buffer, _start_time, _cache_hits, _cache_size

    seen_percent = 0
    if _tweets_seen > 0:
        seen_percent = int(100*(float(_passed_filter)/_tweets_seen))
    runtime = time.time()-_start_time

    status = (
        'tweets seen: ' + str(_tweets_seen) +
        " passed filter: " + str(_passed_filter) +
        " ({0}%)".format(seen_percent) +
        " hits " + str(_possible_hits + _fetch_pool_size) + '/' + str(_cache_hits) +
        " agrams: " + str(_hits) +
        " cachesize: " + str(_cache_size) +
        " buffer: " + str(_buffer) +
        " fetchpool: " + str(_fetch_pool_size) +
        " runtime: " + anagramfunctions.format_seconds(runtime)
    )
    sys.stdout.write(status + '\r')
    sys.stdout.flush()


def close():
    pass
    # self.end_time = time.time()
    # filename = "data/stats/%s.p" % time.strftime("%b%d%H%M")
    # pickle.dump(self, open(filename, 'wb'))
    # logging.debug('saved stats with %i hits' % len(self.hitlist))
