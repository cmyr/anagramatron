import re


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
