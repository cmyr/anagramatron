import cPickle as pickle
import time
import sys
import twitterhandler

if __name__ == "__main__":
    stream = twitterhandler.StreamHandler(languages=['fr', 'es'])
    stream.start()
    count = 0
    save_interval = 20000
    tlist = []

    try:
        for t in stream:
            tlist.append(t)
            count += 1
            sys.stdout.write(str(count) + '\r')
            sys.stdout.flush()
            if count > save_interval:
                filename = "testdata/raw_%s.p" % time.strftime("%b%d%H%M")
                pickle.dump(tlist, open(filename, 'wb'))
                count = 0
                tlist = []
    finally:
        if count > 1000:
            filename = "testdata/raw_esfr_%s.p" % time.strftime("%b%d%H%M")
            pickle.dump(tlist, open(filename, 'wb'))
