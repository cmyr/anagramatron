import re
import anagramstats as stats
import sqlite3 as lite


def test_anagram(string_one, string_two):
    """
    most basic test, finds if tweets are just identical
    """
    stats.possible_hit()
    if not _char_diff_test(string_one, string_two):
        return False
    if not _word_diff_test(string_one, string_two):
        return False

    stats.hit()
    return True


def _char_diff_test(string_one, string_two, cutoff=0.5):
    """
    basic test, looks for similarity on a char by char basis
    """
    stripped_one = stripped_string(string_one)
    stripped_two = stripped_string(string_two)

    total_chars = len(stripped_two)
    same_chars = 0

    if len(stripped_one) != len(stripped_two):
        print('diff check called on unequal length strings')
        print(string_one, string_two)
        return False

    for i in range(total_chars):
        if stripped_one[i] == stripped_two[i]:
            same_chars += 1
    try:
        if (float(same_chars) / total_chars) < cutoff:
            return True
    except ZeroDivisionError:
        print(string_one, string_two)
    return False


def _word_diff_test(string_one, string_two, cutoff=0.5):
    """
    looks for tweets containing the same words in different orders
    """
    words_one = stripped_string(string_one, spaces=True).split()
    words_two = stripped_string(string_two, spaces=True).split()

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


def show_anagram(one, two):
    print one
    print two
    print stripped_string(one, spaces=True)
    print stripped_string(two, spaces=True)
    print stripped_string(one)
    print stripped_string(two)
    print ''.join(sorted(stripped_string(two), key=str.lower))


def stripped_string(text, spaces=False):
    """
    returns lower case string with all non alpha chars removed
    """
    if spaces:
        return re.sub(r'[^a-zA-Z]', ' ', text).lower()
    return re.sub(r'[^a-zA-Z]', '', text).lower()


if __name__ == "__main__":
    pass