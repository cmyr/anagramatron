import re
import unicodedata
import json

from .common import (ANAGRAM_LOW_CHAR_CUTOFF, ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF,
    ANAGRAM_ALPHA_RATIO_CUTOFF, ENGLISH_LETTER_FREQUENCIES)

ENGLISH_LETTER_LIST = sorted(ENGLISH_LETTER_FREQUENCIES.keys(),
                             key=lambda t: ENGLISH_LETTER_FREQUENCIES[t])

# This contains the various functions for filtering tweets, comparing
# potential anagrams, as well as some shared helper utilities.


freqsort = ENGLISH_LETTER_FREQUENCIES
# just to keep line_lengths sane


def simple_hash(text, debug=False):
    text = stripped_string(text)
    t_hash = re.sub(
        r'([a-z])\1{1,}', 
        lambda m: '%s%d' % (m.group(1), len(m.group())-1),
        ''.join(sorted(text)))
    return t_hash

def improved_hash(text, debug=False):
    """
    only very *minorly* improved. sorts based on letter frequencies.
    """
    CHR_COUNT_START = 64  # we convert to chars; char 65 is A
    t_text = stripped_string(text)
    t_hash = ''.join(sorted(t_text, key=lambda t: freqsort[t]))
    letset = set(t_hash)
    break_letter = t_hash[-1:]
    if break_letter not in ENGLISH_LETTER_LIST:
        break_letter = ENGLISH_LETTER_LIST[-1]
    compressed_hash = ''
    for letter in ENGLISH_LETTER_LIST:
        if letter in letset:
            count = len(re.findall(letter, t_hash))
            count = (count if count < 48 else 48)
            # this is a hacky way of sanity checking our values.
            # if this shows up as a match we'll ignore it
            compressed_hash += chr(count + CHR_COUNT_START)
        else:
            if freqsort[letter] > freqsort[break_letter]:
                if len(compressed_hash) % 2:
                    # an uneven number of bytes will cause unicode errors?
                    compressed_hash += chr(64)
                break
            compressed_hash += chr(64)

    if len(compressed_hash) == 0:
        print('hash length is zero?')
        return '@@'
    return compressed_hash
    # return t_hash

def length_from_hash(in_hash):
    """
    takes an improved hash and returns the number of characters
    in the original string.
    """
    length = 0
    chars = list(in_hash)
    for c in chars:
        length += ord(c) - 64
    return length


def correct_encodings(text):
    """
    twitter auto converts &, <, > to &amp; &lt; &gt;
    """
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    return text


def _strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def _text_contains_tricky_chars(text):
    if re.search(r'[\u0080-\u024F]', text):
        return True
    return False


def _text_decodes_to_ascii(text):
    try:
        text.encode('ascii')
    except UnicodeEncodeError:
        return False
    return True


def _basic_filters(tweet):
    if tweet.get('lang') != 'en':
        return False
    if len(tweet.get('entities').get('user_mentions')) is not 0:
        return False
    #check for retweets
    if tweet.get('retweeted_status'):
        return False
    # check for links:
    if len(tweet.get('entities').get('urls')) is not 0:
        return False
    if re.search(r'[0-9]', tweet['text']):
        return False
    t = stripped_string(tweet['text'])
    if len(t) <= ANAGRAM_LOW_CHAR_CUTOFF:
        return False
    # ignore tweets with few characters
    st = set(t)
    if len(st) <= ANAGRAM_LOW_UNIQUE_CHAR_CUTOFF:
        return False
    return True


def _low_letter_ratio(text, cutoff=0.8):
    t = re.sub(r'[^a-zA-Z .,!?"\']', '', text)
    if (float(len(t)) / len(text)) < cutoff:
        return True
    return False


def filter_tweet(tweet):
    """
    filters out anagram-inappropriate tweets.
    Returns the original tweet object and cleaned tweet text on success.
    """
    if not _basic_filters(tweet):
        return False

    tweet_text = correct_encodings(tweet.get('text'))
    if not _text_decodes_to_ascii(tweet_text):
        # check for latin chars:
        if _text_contains_tricky_chars(tweet_text):
            tweet_text = _strip_accents(tweet_text)

    if _low_letter_ratio(tweet_text, ANAGRAM_ALPHA_RATIO_CUTOFF):
        return False

    return {'anagram_hash': improved_hash(tweet_text),
            'tweet_id': int(tweet['id_str']),
            'text': tweet_text
            }


def test_anagram(one, two):
    """
    most basic test, finds if tweets are just identical
    """
    if not _char_diff_test(one, two):
        return False
    if not _word_diff_test(one, two):
        return False
    if not _combined_words_test(one, two):
        return False
    if not one_test_to_rule_them(one, two):
        return False
    return True


def _char_diff_test(one, two, cutoff=0.3):
    """
    basic test, looks for similarity on a char by char basis
    """
    stripped_one = stripped_string(one)
    stripped_two = stripped_string(two)

    total_chars = len(stripped_two)
    same_chars = 0

    if len(stripped_one) != len(stripped_two):
        return False

    for i in range(total_chars):
        if stripped_one[i] == stripped_two[i]:
            same_chars += 1
    try:
        if (float(same_chars) / total_chars) < cutoff:
            return True
    except ZeroDivisionError:
        print(one, two)
    return False


def _word_diff_test(one, two, cutoff=0.3):
    """
    looks for tweets containing the same words in different orders
    """
    words_one = stripped_string(one, spaces=True).split()
    words_two = stripped_string(two, spaces=True).split()

    word_count = len(words_one)
    same_words = 0

    if len(words_two) < len(words_one):
            word_count = len(words_two)
        # compare words to each other:
    for word in words_one:
        if word in words_two:
            same_words += 1
        # if more then $CUTOFF words are the same, fail test
    if (float(same_words) / word_count) < cutoff:
        return True
    else:
        return False

def _combined_words_test(one, two, cutoff=0.5):
    """
    looks for tweets where the same words have been #CombinedWithoutSpaces

    """
    words_one = stripped_string(one, spaces=True).split()
    words_two = stripped_string(two, spaces=True).split()

    if len(words_one) == len(words_two):
        return True
    # print(words_one, words_two)
    more_words = words_one if len(words_one) > len(words_two) else words_two;
    fewer_words = words_one if words_two == more_words else words_two
    # rejoin fewer words into a string:
    fewer_words = ' '.join(fewer_words)

    for word in more_words:
        if re.search(word, fewer_words):
            fewer_words = re.sub(word, '', fewer_words, count=1)

    # this leaves us, hopefully, with a smoking hulk of non-string.
    more_string = ''.join(more_words)
    fewer_words = re.sub(' ', '', fewer_words)
    more_string = re.sub(' ', '', more_string)
    if (len(fewer_words)/float(len(more_string))) > cutoff:
        return True
    else:
        return False


def one_test_to_rule_them(one, two, cutoff=0.8, stop=False):
    """
    searches s2 for words from s1, removing them where found.
    repeats in the opposite order on pass.
    """
    s1 = sorted(stripped_string(one, spaces=True).split(),
                key=len,
                reverse=True)
    s2 = stripped_string(two, spaces=True)
    for word in s1:
        if len(word) > 2 and re.search(word, s2):
            s2 = re.sub(word, '', s2, count=1)
    s1 = ''.join(s1)
    s2 = stripped_string(s2, spaces=False)

    if float(len(s2))/len(s1) < cutoff:
        return False
    else:
        if stop:
            return True
        return one_test_to_rule_them(two, one, stop=True)


def grade_anagram(hit):
    """
    an attempt to come up with a numerical value that expresses an anagrams
    potential 'interestingness'.
    """
    t1 = hit['tweet_one']['tweet_text']
    t2 = hit['tweet_two']['tweet_text']

    letter_count = len(stripped_string(t1))
    unique_letters = len(set(stripped_string(t1)))

    return letter_count, unique_letters


def format_seconds(seconds):
    """
    convert a number of seconds into a custom string representation
    """
    d, seconds = divmod(seconds, (60*60*24))
    h, seconds = divmod(seconds, (60*60))
    m, seconds = divmod(seconds, 60)
    time_string = ("%im %0.2fs" % (m, seconds))
    if h or d:
        time_string = "%ih %s" % (h, time_string)
    if d:
        time_string = "%id %s" % (d, time_string)
    return time_string


# def show_anagram(one, two):
#     print one
#     print two
#     print stripped_string(one, spaces=True)
#     print stripped_string(two, spaces=True)
#     print stripped_string(one)
#     print stripped_string(two)
#     print ''.join(sorted(stripped_string(two), key=str.lower))


def stripped_string(text, spaces=False):
    """
    returns lower case string with all non alpha chars removed
    """
    if spaces:
        text = re.sub(r'[_-]', ' ', text)  # replace dashes and underbars
        return re.sub(r'[^a-zA-Z ]', '', text).lower()
    return re.sub(r'[^a-zA-Z]', '', text).lower()


def encode_tweet(tweet_dict):
    assert isinstance(tweet_dict, dict), "%s %s" % (type(tweet_dict), tweet_dict)
    return json.dumps(tweet_dict)


def decode_tweet(tweet_str):
    assert isinstance(tweet_str, str), "%s %s" % (type(tweet_str), tweet_str)
    return json.loads(tweet_str)

if __name__ == "__main__":
    pass
