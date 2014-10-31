from __future__ import print_function
import gdbm
import cPickle as pickle
import os
import time
import re
import logging
import sys
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
            try:
                self._metadata = pickle.load(open('%s/%s' % (self._path, _METADATA_FILE), 'r'))
                print('loaded metadata: %s' % repr(self._metadata))
                logging.debug('loaded metadata %s' % repr(self._metadata))
            except IOError:
                print("IO error loading metadata?")
            
            dbses = _load_paths(self._path)
            for db in dbses:
                try:
                    self._data.append(gdbm.open(db, 'c'))
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
        db = gdbm.open(path, 'c')
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
        try:
            print("performing maintenance on %d database chunks" % len(self._data))
            for db in self._data:
                path = db[_PATHKEY]
                db_type = whichdb.whichdb(path)
                print("checking %s, type: %s" % (path, db_type))
                try:
                    db.reorganize()
                except gdbm.error:
                    print("error: failed to reorganize db chunk")
                    continue
        finally:
            self.close()


def check_integrity_for_chunk(db_chunk):
    # path = db_chunk[_PATHKEY]
    # print("checking keys in db: %s\n" % path)
    k = db_chunk.firstkey()
    nextkey = k
    seen = 0
    while k is not None:
        nextkey = db_chunk.nextkey(k)
        if nextkey == k:
            print("next key == current key!")
            break
        k = nextkey
        seen += 1
        sys.stdout.write('checked: %i\t\t\r' % seen)
        sys.stdout.flush()
    print("\nno next key found. total keys: %i" % seen)

def _load_paths(mdbm_path):
    """returns a creation-date sorted list of chunks in our path"""
    ls = (os.path.join(mdbm_path, i) for i in os.listdir(mdbm_path)
            if re.findall('mdbm', i))
    ls = ((os.stat(path), path) for path in ls)
    ls = ((stat[ST_CTIME], path) for stat, path in ls)
    return [path for stat, path in sorted(ls)]
            
def verify_database(dbpath):
    db_files = _load_paths(dbpath)
    print("verifying %i mdbm chunks" % len(db_files))
    for db in db_files:
        dbchunk = gdbm.open(db, 'w')
        # print("reorganizing %s" % db)
        # try:
        #     dbchunk.reorganize()
        # except Exception as err:
        #     print("couldn't reorganize: error %s" % err)
        print("checking %s" % db)
        try:
            # check_integrity_for_chunk(dbchunk)
            k = dbchunk.firstkey()
            print("first key: %s" % k)
            check_integrity_for_chunk(dbchunk)
        except Exception as err:
            print("integrity check failed: error %s" % err)
        finally:
            dbchunk.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--repair', help='repair/verify datastore', action="store_true")
    parser.add_argument('db', type=str, help="source database file")
    args = parser.parse_args()

    if not args.db:
        print('please specify the mdbm directory')

    if args.repair:
        verify_database(args.db)