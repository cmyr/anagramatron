from __future__ import print_function
import anydbm
import cPickle as pickle
import os
import time
import re
import logging
from stat import S_ISREG, ST_CTIME, ST_MODE


_METADATA_FILE = 'meta.p'
_PATHKEY = 'X43q2smxlkFJ28h$@3xGN' # gurrenteed unlikely!!


class MultiDBM(object):
    """
    MultiDBM acts as a wrapper around multiple DBM files
    as data retrieval becomes too slow older files are archived.
    """

    def __init__(self, path, chunk_size=2000000):
        self._data = []
        self._metadata = dict()
        self._path = path
        self._section_size = chunk_size
        self._setup()

    def __contains__(self, item):
        for db in self._data:
            if item in db:
                return True
        return False

    def __getitem__(self, key):
        for db in self._data:
            if key in db:
                return db[key]
        raise KeyError

    def __setitem__(self, key, value):
        i = 0
        if self._metadata['cursize'] == self._section_size:
            self._add_db()
        last_db = len(self._data) - 1
        for db in self._data:
            if key in db or i == last_db:
                if i == last_db and key not in db:
                    self._metadata['totsize'] += 1
                    self._metadata['cursize'] += 1
                # logging.debug('adding key to file # %i' % i)
                db[key] = value
                return
            i += 1

    def __delitem__(self, key):
        for db in self._data:
            if key in db:
                del db[key]
                self._metadata['totsize'] -= 1
                return
        raise KeyError

    def __len__(self):
        """
        length calculations are estimates since we assume
        all non-current chunks are at capacity.
        In reality some keys will likely get deleted.
        """
        return (self._section_size * len(self._data-1)
            + self._metadata['cursize'])


    def _setup(self):
        if os.path.exists(self._path):
            self._metadata = pickle.load(open('%s/%s' % (self._path, _METADATA_FILE), 'r'))
            print('loaded metadata: %s' % repr(self._metadata))
            logging.debug('loaded metadata %s' % repr(self._metadata))
            
            # sort our dbm segments by creation date
            ls = (os.path.join(self._path, i) for i in os.listdir(self._path)
                if re.findall('mdbm', i))
            ls = ((os.stat(path), path) for path in ls)
            ls = ((stat[ST_CTIME], path) for stat, path in ls)
            dbses = [path for stat, path in sorted(ls)]
            for db in dbses:
                try:
                    self._data.append(anydbm.open(db, 'c'))
                except Exception as err:
                    print('error appending dbfile: %s' % db, err)

            print('loaded %i dbm files' % len(self._data))
        else:
            print('path not found, creating')
            os.makedirs(self._path)
            os.makedirs('%s/archive' % self._path)
            self._metadata['totsize'] = 0
            self._metadata['cursize'] = 0

        if not len(self._data):
            self._add_db()

    def _add_db(self):
        filename = 'mdbm%s.db' % time.strftime("%b%d%H%M")
        # filename = 'mdbm%s.db' % str(time.time())
        path = self._path + '/%s' % filename
        db = anydbm.open(path, 'c')
        db[_PATHKEY] = filename
        self._data.append(db)
        self._metadata['cursize'] = 0
        logging.debug('mdbm added new dbm file: %s' % filename)

    def _remove_old(self):
        db = self._data.pop(0)
        filename = db[_PATHKEY]
        db.close()
        target = '%s/%s' % (self._path, filename)
        destination = '%s/archive/%s' % (self._path, filename)
        os.rename(target, destination)
        logging.debug('mdbm moved old dbm file to %s' % destination)
        return destination

    def section_count(self):
        return len(self._data)

    def archive(self):
        return self._remove_old()

    def close(self):
        path = '%s/%s' % (self._path, _METADATA_FILE)
        print('dumping path:', path)
        pickle.dump(self._metadata, open(path, 'wb'))
        for db in self._data:
            db.close()


    def perform_maintenance(self):
        import whichdb
        print("performing maintenance on %d database chunks" % len(self._data))
        for db in self._data:
            path = db[_PATHKEY]
            db_type = whichdb.whichdb(path)
            print("checking %s, type: %s" % (path, db_type))
            db.reorganize()

if __name__ == '__main__':
    test()