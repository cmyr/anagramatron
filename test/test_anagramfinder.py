
import os
import shutil

from anagramatron import anagramfinder, common

TEST_STORE_PATH =  os.path.join(common.ANAGRAM_DATA_DIR, 'test_store.mdbm')

def test_setup():
    _cleanup()
    finder = anagramfinder.AnagramFinder(path=TEST_STORE_PATH, storage=None)
    assert not os.path.exists(TEST_STORE_PATH)
    assert finder.datastore == None
    assert finder.cache != None

    finder = anagramfinder.AnagramFinder(path=TEST_STORE_PATH, storage='mdbm')
    assert finder.datastore
    assert finder.cache != None


def _cleanup():
    if os.path.exists(TEST_STORE_PATH):
        shutil.rmtree(TEST_STORE_PATH)
