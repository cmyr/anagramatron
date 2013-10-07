from __future__ import print_function
import anydbm
import cPickle as pickle
import os
import time
import re

_METADATA_FILE = 'meta.p'
_PATHKEY = 'X43q2smxlkFJ28h$@3xGN' # gurrenteed unlikely!!


# def open(path, flag):
#     return MultiDBM(path, flag)


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
            ls = os.listdir(self._path)
            dbses = sorted([i for i in ls if re.findall('mdbm', i)])
            for db in dbses:
                dbpath = '%s/%s' % (self._path, db)
                print(dbpath)
                self._data.append(anydbm.open(dbpath, 'c'))
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
        filename = time.strftime("%b%d%H%M")
        filename = 'mdbm%s.db' % str(time.time())
        path = self._path + '/%s' % filename
        db = anydbm.open(path, 'c')
        db[_PATHKEY] = filename
        self._data.append(db)
        self._metadata['cursize'] = 0

    def _remove_old(self):
        print(self._path)
        db = self._data.pop(0)
        filename = db[_PATHKEY]
        db.close()
        target = '%s/%s' % (self._path, filename)
        destination = '%s/archive/%s' % (self._path, filename)
        print(target, destination)
        assert(os.path.exists(target))
        os.rename(target, destination)

    def section_count(self):
        return len(self._data)

    def archive(self):
        self._remove_old()

    def close(self):
        path = '%s/%s' % (self._path, _METADATA_FILE)
        print('dumping path:', path)
        pickle.dump(self._metadata, open(path, 'wb'))
        for db in self._data:
            db.close()





def test():
    # import pudb; pu.db
    mdbm = MultiDBM('mdbmtest', 10000, 5)
    print('running tests')

    for i in range(15410):
        key = str(i)
        mdbm[key] = 'tstmsg'
        # print('%s/%s' % (mdbm._metadata['cursize'], mdbm._metadata['totsize']))
    print(len(mdbm))
    assert(len(mdbm) == 15410)
    print(len(mdbm._data))
    assert(mdbm._section_size == 2000)
    assert(len(mdbm._data) == 3)
    assert('12' in mdbm)
    assert('198' in mdbm)
    assert('11411' not in mdbm)

    mdbm.close()
    mdbm = MultiDBM('mdbmtest', 10000, 5)


    assert(len(mdbm) == 5410)
    for i in range(5410):
        assert(str(i) in mdbm)

    if 'teststr' in mdbm:
        print('arbitrary string found')
    else:
        print('arbitary string not found')

if __name__ == '__main__':
    test()