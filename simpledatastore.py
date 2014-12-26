# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

ITEM_KEY = 'tweet'
COUNT_KEY = 'hit_count'

class AnagramSimpleStore(object):
    """AnagramSimpleStore is a simple data store implemented
    using standard library data structures. It is intended for use as
    a cache, or for smaller, static input sources."""
    def __init__(self, path):
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

    def __delete__(self, instance):
        del self.datastore[x]
        
    def load(self):
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
            # really not tons we can do ehre

    def save(self):
        """
        pickles the data currently in the cache.
        doesn't save hit_count. we don't want to keep briefly popular
        items in cache indefinitely
        """
        to_save = [self.datastore[t]['tweet'] for t in self.datastore]
        try:
            pickle.dump(tweets_to_save, open(self.path, 'wb'))
            print('saved cache to disk with %i tweets' % len(tweets_to_save))
        except:
            logging.error('unable to save cache, writing')
            self._trim_cache(len(self.datastore))

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('arg1', type=str, help="required argument")
    parser.add_argument('arg2', '--argument-2', help='optional boolean argument', action="store_true")
    args = parser.parse_args()


if __name__ == "__main__":
    main()