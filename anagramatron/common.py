
import sys
import os


def __setup_data_dir():
    """sets up the directory where prepared training data is stored."""
    basedir = os.environ.get('ANAGRAMATRON_DATA_DIR')
    if not basedir:
        basedir = os.path.expanduser(
            os.path.join('~', 'anagramatron_data'))
        print('$ANAGRAMATRON_DATA_DIR env var not set, using %s' %
              basedir, file=sys.stderr)
    return basedir

ANAGRAM_DATA_DIR = __setup_data_dir()

if not os.path.exists(ANAGRAM_DATA_DIR):
    os.mkdir(ANAGRAM_DATA_DIR)

ANAGRAM_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
ANAGRAM_SEC_DIR = os.path.join(ANAGRAM_BASE_DIR, 'sec')

ANAGRAM_CACHE_SIZE = 200000
ANAGRAM_STREAM_BUFFER_SIZE = 20000

ANAGRAM_LOW_CHAR_CUTOFF = 16
ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF = 11
ANAGRAM_ALPHA_RATIO_CUTOFF = 0.85

ANAGRAM_POST_INTERVAL = 150  # minutes

# STORAGE_DIRECTORY_PATH = 'data/'

ENGLISH_LETTER_FREQUENCIES = {
    'e': 1,
    't': 2,
    'a': 3,
    'o': 4,
    'i': 5,
    'n': 6,
    's': 7,
    'h': 8,
    'r': 9,
    'd': 10,
    'l': 11,
    'c': 12,
    'u': 13,
    'm': 14,
    'w': 15,
    'f': 16,
    'g': 17,
    'y': 18,
    'p': 19,
    'b': 20,
    'v': 21,
    'k': 22,
    'j': 23,
    'x': 24,
    'q': 25,
    'z': 26
}

