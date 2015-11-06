# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

from operator import itemgetter
import cPickle as pickle
import logging

ITEM_KEY = 'tweet'
COUNT_KEY = 'hit_count'

class AnagramSimpleStore(object):
    """AnagramSimpleStore is a simple data store implemented
    using standard library data structures. It is intended for use as
    a cache, or for smaller, static input sources."""
    def __init__(self, path=None, maxsize=None):
        super(AnagramSimpleStore, self).__init__()
        self.path = path
        self.datastore = self.load()
    
    def __len__(self):
        return len(self.datastore)

    def __contains__(self, item):
        return self.datastore.get(item) != None

    def __getitem__(self, key):
        return self.datastore[key][ITEM_KEY]

    def __setitem__(self, key, value):
        if key in self:
            self.datastore[key][ITEM_KEY] = value
            self.datastore[key][COUNT_KEY] += 1
        else:
            self.datastore[key] = {ITEM_KEY: value, COUNT_KEY: 0}

    def __delitem__(self, instance):
        del self.datastore[instance]
        
    def load(self):
        if not self.path:
            return dict()
        print('loading cache')
        cache = dict()
        try:
            loaded = pickle.load(open(self.path, 'r'))
            for t in loaded:
                cache[t['tweet_hash']] = {'tweet': t, 'hit_count': 0}
            print('loaded %i items to cache' % len(cache))
            return cache
        except IOError:
            logging.error('error loading cache :(')
            return cache

    def save(self):
        """
        pickles the data currently in the cache.
        doesn't save hit_count. we don't want to keep briefly popular
        items in cache indefinitely
        """
        if self.path:
            to_save = [self.datastore[t]['tweet'] for t in self.datastore]
            try:
                pickle.dump(to_save, open(self.path, 'wb'))
                print('saved cache to disk with %i items' % len(to_save))
            except:
                logging.error('unable to save cache')

    def least_used(self, count):
        items = [(key, value[ITEM_KEY], value[COUNT_KEY])
        for key, value in self.datastore.items()]

        items = sorted(items, key=itemgetter(2))
        least_used_keys = [x for (x, y, z) in items[:count]]
        return least_used_keys
        # print('returning %d least used keys: %s' % 
        #     (len(least_used_keys), "\n".join(least_used_keys[:10])))

def main():
    pass


if __name__ == "__main__":
    main()