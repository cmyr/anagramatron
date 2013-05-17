import cPickle as pickle
import time
import shutil

hits = pickle.load(open('data/hits0.54.p', 'r'))
shutil.copy('data/hits0.54.p', 'data/hitsbackup.p')
new_hits = []
id_counter = 0
for hit in hits:
    timestamp = int(time.time()*1000) + id_counter
    id_counter += 1
    new_hit = {'id': timestamp, 'tweet_one': hit['tweet_one'], 'tweet_two': hit['tweet_two']}
    new_hits.append(new_hit)

pickle.dump(new_hits, open('data/hits0.54.p', 'wb'))


