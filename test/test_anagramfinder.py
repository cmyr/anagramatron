
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


a_count = 0
 
def counter(*args):
    global a_count
    a_count += 1

def test_simple_anagrams():
    _cleanup()

    test_input = ['So bored all the time😴',
        'Berit od hates me lol',
        "Lord Jesus it's a fart",
        "It's just sad forreal",
        'Maybe trying to hard .',
        'Angry birthday to me 😠',
        "This flow ain't right 😪",
        'how is that flirting.',
        'My little sister hands go !',
        'time destroys all things',
        'i hate this one republic song',
        'Bae slurping on this icee tho 😝😂',
        'Moist as heck in here',
        'He The Reason Im Sick .',
        'Cheetah girls two is on !',
        'I Got One Class With Her.',
        'Freight is so pathetic.',
        'straight piece of shit',
        'Saturday morning in bed 😊',
        'Im in #Danger Darn you BTS!']

    
    finder = anagramfinder.AnagramFinder(hit_callback=counter)

    for inp in test_input:
        finder.handle_input(inp)

    assert a_count == 10


def _cleanup():
    if os.path.exists(TEST_STORE_PATH):
        shutil.rmtree(TEST_STORE_PATH)
