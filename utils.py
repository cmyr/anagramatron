import re

def format_seconds(seconds):
    """
    yea fine this is bad deal with it
    """
    seconds = int(seconds)
    DAYSECS = 86400
    HOURSECS = 3600
    MINSECS = 60
    dd = hh = mm = ss = 0

    dd = seconds / DAYSECS
    seconds = seconds % DAYSECS
    hh = seconds / HOURSECS
    seconds = seconds % HOURSECS
    mm = seconds / MINSECS
    seconds = seconds % MINSECS
    ss = seconds
    # time_string = (str(mm)+'m ' + str(ss) + 's')
    time_string = ("%im %s" %(mm,ss))
    if hh or dd:
        # time_string = str(hh) + 'h ' + time_string
        time_string = "%ih %s" % (hh, time_string)
    if dd:
        # time_string = str(dd) + 'd ' + time_string
        time_string = "%id %s" % (dd, time_string)
    return time_string


if __name__ == "__main__":
    tests = [10,
             12.025808095932007,
             15.282587051391602,
             101,
             1124,
             23232,
             4334321,
             1231450698173748]

    for t in tests:
        print format_seconds(t)
        print "format test: %s" % format_seconds(t)

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