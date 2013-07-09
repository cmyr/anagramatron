from __future__ import print_function
import time


_tweets_seen = 0
_passed_filter = 0
_possible_hits = 0
_hits = 0
_overflow = 0
_start_time = time.time()
_buffer = 0
hit_distributions = [0 for x in range(140)]
hash_distributions = [0 for x in range(140)]
hitlist = []


def new_hash(hash_text):
    global hash_distributions
    hashlength = len(hash_text)
    if (hashlength < 140):
        hash_distributions[hashlength] += 1


def new_hit(self, hash_text):
    global hitlist
    global hit_distributions

    hitlist.append(hash_text)
    hashlength = len(hash_text)
    if (hashlength < 140):
        hit_distributions[hashlength] += 1


def tweets_seen(seen=1):
    global _tweets_seen
    _tweets_seen += seen


def passed_filter(passed=1):
    global _passed_filter
    _passed_filter += passed


def possible_hit(possible=1):
    global _possible_hits
    _possible_hits += possible


def overflow(over=1):
    global _overflow
    _overflow += over


def set_buffer(buffer_size):
    _buffer = buffer_size


def stats_dict():
    return {
            'tweets_seen': _tweets_seen,
            'passed_filter': _passed_filter,
            'possible_hits': _possible_hits,
            'hits': _hits,
            'overflow': _overflow,
            'start_time': _start_time
            }


def close():
    pass

# class AnagramStats(object):
#     """
#     keeps track of stats for us
#     """

#     def __init__(self):
#         self.tweets_seen = 0
#         self.passed_filter = 0
#         self.possible_hits = 0
#         self.hits = 0
#         self.overflow = 0
#         self.start_time = 0
#         self.hit_distributions = [0 for x in range(140)]
#         self.hash_distributions = [0 for x in range(140)]
#         self.hitlist = []

#     def new_hash(self, hash_text):
#         hashlength = len(hash_text)
#         if (hashlength < 140):
#             self.hash_distributions[hashlength] += 1

#     def new_hit(self, hash_text):
#         self.hitlist.append(hash_text)
#         hashlength = len(hash_text)
#         if (hashlength < 140):
#             self.hit_distributions[hashlength] += 1

#     def close(self):
#         self.end_time = time.time()
#         filename = "data/stats/%s.p" % time.strftime("%b%d%H%M")
#         pickle.dump(self, open(filename, 'wb'))
#         logging.debug('saved stats with %i hits' % len(self.hitlist))

