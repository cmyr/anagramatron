import cPickle as pickle
import time
import sys
from streamhandler import StreamHandler
import anagramfunctions

"""a helper file for fetching & saving test data from the twitter stream"""

if __name__ == "__main__":
    stream = StreamHandler()
    stream.start()
    count = 0
    save_interval = 50000
    tlist = []

    try:
        for t in stream:
            if not t: 
                continue

            tlist.append(t)
            count += 1
            sys.stdout.write(str(count) + '\r')
            sys.stdout.flush()
            if count > save_interval:
                filename = "testdata/filt_%s.p" % time.strftime("%b%d%H%M")
                pickle.dump(tlist, open(filename, 'wb'))
                count = 0
                tlist = []
    finally:
        if count > 1000:
            filename = "testdata/filt_%s.p" % time.strftime("%b%d%H%M")
            pickle.dump(tlist, open(filename, 'wb'))
