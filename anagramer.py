from __future__ import print_function

import sys
import re
import time
import string
import cPickle as pickle
import shutil

from twitterhandler import TwitterHandler, TwitterRateLimitError, TweetUnavailableError
from comptests import compare
from twitter.api import TwitterHTTPError

VERSION_NUMBER = 0.54
DATA_FILE_NAME = 'data/data' + str(VERSION_NUMBER) + '.p'
BACKUP_FILE_NAME = 'data/databackup' + str(VERSION_NUMBER) + '.p'
BLACKLIST_FILE_NAME = 'data/blacklist.p'


class Anagramer(object):
    """
    Anagramer hunts for anagrams on twitter. A TwitterHandler object handles
    interactions with the twitter API.
    """
    def __init__(self):
        self.twitter_handler = TwitterHandler()
        self.data = {}
        self.hits = []
        self.black_list = set()
        self.load()
        self.activity_time = 0
        self.http_connection_attempts = 0
        self.http_420_attempts = 0
        # below is for logging stats. is this the best way?
        self.stat_start_time = time.time()
        self.stat_in_total = 0
        self.stat_in_has_text = 0
        self.stat_in_nonascii = 0
        self.stat_in_has_text = 0
        self.stat_in_has_url = 0
        self.stat_in_has_mention = 0
        self.stat_in_is_retweet = 0
        self.stat_in_low_chars = 0
        self.stat_in_low_unique_chars = 0
        self.stat_in_on_blacklist = 0
        self.stat_in_passed_filter = 0
        self.stat_hit_possible = 0
        self.stat_hit_confirmed = 0

    def load(self):
        """
        loads state from a data file, if present
        """
        try:
            open(BLACKLIST_FILE_NAME, 'r')
        except IOError:
            print('no data file present. creating new data file.')
            pickle.dump(self.black_list, open(BLACKLIST_FILE_NAME, 'wb'))
        try:
            open(DATA_FILE_NAME, 'r')
        except IOError:
            # create a new pickle file if it isn't here
            tosave = {
                'data': {},
                'hits': [],
            }
            pickle.dump(tosave, open(DATA_FILE_NAME, 'wb'))
        try:
            saved_data = pickle.load(open(DATA_FILE_NAME, 'rb'))
            self.data = saved_data['data']
            self.hits = saved_data['hits']
            self.black_list = pickle.load(open(BLACKLIST_FILE_NAME, 'rb'))
            print("loaded data file with:", len(self.data), "entries.")
            print("loaded blacklist with:", len(self.black_list), "entries.")
        except (pickle.UnpicklingError, EOFError) as e:
            print("error loading data \n")
            print(e)
            sys.exit(1)

    def save(self):
        """
        saves a list of fetched tweets & possible matches
        """
        try:
            tosave = {
                'data': self.data,
                'hits': self.hits,
            }
            pickle.dump(tosave, open(DATA_FILE_NAME, 'wb'))
            pickle.dump(self.black_list, open(BLACKLIST_FILE_NAME, 'wb'))
            shutil.copy(DATA_FILE_NAME, BACKUP_FILE_NAME)
            print("\nsaved data with:", len(self.data), "entries")
        except IOError:
            print("unable to save file, debug me plz")
            sys.exit(1)

        # print some stats for the kids
        print('total tweets seen: ', str(self.stat_in_total))
        txtperc = int(100*(float(
            self.stat_in_has_text)/self.stat_in_total))
        print('had text: ', str(self.stat_in_has_text), ' (', txtperc,'%)')
        asciiperc = int(100*(float(
            self.stat_in_nonascii)/self.stat_in_has_text))
        print('non ascii:', str(self.stat_in_nonascii), ' (,', asciiperc, '%)')
        urlperc = int(100*(float(
            self.stat_in_has_url)/self.stat_in_has_text))
        print('had url:', str(self.stat_in_has_url), ' (,', urlperc, '%)')
        mentionperc = int(100*(float(
        self.stat_in_has_mention)/self.stat_in_has_text))
        print('has mention:', str(self.stat_in_has_mention), ' (,', mentionperc, '%)')
        rtperc = int(100*(float(
        self.stat_in_is_retweet)/self.stat_in_has_text))
        print('is retweet:', str(self.stat_in_is_retweet), ' (,', rtperc, '%)')
        shortperc = int(100*(float(
        self.stat_in_low_chars)/self.stat_in_has_text))
        print('too short:', str(self.stat_in_low_chars), ' (,', shortperc, '%)')
        lowuniqperc = int(100*(float(
        self.stat_in_low_unique_chars)/self.stat_in_has_text))
        print('few unique chars:', str(self.stat_in_low_unique_chars), ' (,', lowuniqperc, '%)')

    def run(self):
        """
        starts the program's main run-loop
        """
        # review previous hits before beginning:
        HTTP_COOLDOWN = 5
        HTTP_420_COOLDOWN = 60
        if len(self.hits):
            self.review_hits()
        while 1:
            try:
                print('entering run loop')
                self.start_stream()
            except KeyboardInterrupt:
                self.save()
                break
            except TwitterHTTPError as e:
                print('\n', e)
                # begin back off strategy specified in streaming API Docs
                if e.e.code == 420:
                    self.http_420_attempts += 1
                    cooldown = HTTP_420_COOLDOWN ^ self.http_420_attempts
                    time.sleep(cooldown)
                else:
                    self.http_connection_attempts += 1
                    cooldown = HTTP_COOLDOWN ^ self.http_connection_attempts
                    if cooldown > 320:
                        cooldown = 320
                    self.sleep(cooldown)
                # wait before attempting reconnect, double each time

    def start_stream(self):
        """
        main run loop
        """
        # how long do we stay alive without getting a new tweet?
        # TIMEOUT = 90

        stream_iterator = self.twitter_handler.stream_iter()
        # if the connection is succesful recount our attempt tallies
        self.http_connection_attempts = 0
        self.http_420_attempts = 0
        for tweet in stream_iterator:
            self.activity_time = time.time()
            self.stat_in_total += 1
            if tweet.get('text'):
                self.stat_in_has_text += 1
                if self.filter_tweet(tweet):
                    self.stat_in_passed_filter += 1
                    self.update_console()
                    self.process_input(tweet)
            # time.sleep(1)
            # print(time.time() - self.activity_time)
            # if (time.time() - self.activity_time) > 90:
            #     print('timeout, exiting run loop')
            #     break

    def process_input(self, tweet):
        """
        takes a tweet, generates a hash and checks for a double in
        our data. if a double is found begin analyzing the match.
        else add the input hash to the data store.
        """

        tweet_id = long(tweet['id_str'])
        tweet_hash = self.make_hash(tweet['text'])
        tweet_text = tweet['text']
        tweet_time = time.time()
        hashed_tweet = {
            'id': tweet_id,
            'hash': tweet_hash,
            'text': tweet_text,
            'time': tweet_time
        }

        # uniqueness checking:

        if tweet_hash in self.black_list:
            pass
        elif tweet_hash in self.data:
            self.process_hit(hashed_tweet)
        else:
            self.add_to_data(hashed_tweet)

    def update_console(self):
        """
        prints various bits of status information to the console.
        """
        # what all do we want to have, here? let's blueprint:
        # tweets seen: $IN_HAS_TEXT passed filter: $PASSED_F% Hits: $HITS
        seen_percent = int(100*(float(
            self.stat_in_passed_filter)/self.stat_in_has_text))
        runtime = int(time.time()-self.stat_start_time)
        # save every ten minutes
        if not runtime % 600:
            self.save()

        status = (
            'tweets seen: ' + str(self.stat_in_has_text) +
            " passed filter: " + str(self.stat_in_passed_filter) +
            # " ({0:.2f}%)".format(seen_percent)
            " ({0}%)".format(seen_percent) +
            " hits " + str(self.stat_hit_possible) +
            " agrams: " + str(self.stat_hit_confirmed) +
            " runtime: " + self.format_seconds(runtime)
        )

        sys.stdout.write(status + '\r')
        sys.stdout.flush()

    def review_hits(self):
        hit_count = len(self.hits)
        print('recorded ' + str(hit_count) + ' hits in need of review')

        while len(self.hits):
            hit = self.hits.pop()
            print(hit['tweet_one']['id'], hit['tweet_two']['id'])
            print(hit['tweet_one']['text'])
            print(hit['tweet_two']['text'])

            while 1:
                inp = raw_input("(a)ccept, (r)eject, keep (1)st, keep (2)nd, (s)kip review?:")
                if inp not in ['a', 'r', '1', '2', ' s']:
                    print("invalid input. Please enter 'a', 'r', '1', '2' or 's'.")
                else:
                    break
            if inp == 'a':
                flag = self.post_hit(hit)
                if not flag:
                    print('retweet failed, sorry bud')
                else:
                    print('post successful')
                    self.black_list.add(hit['tweet_one']['hash'])
            if inp == 'r':
                # ignore & add to blacklist
                self.black_list.add(hit['tweet_one']['hash'])
            if inp == '1':
                #reject, but keep first tweet in data:
                self.add_to_data(hit['tweet_one'])
            if inp == '2':
                #reject, but keep first tweet in data:
                self.add_to_data(hit['tweet_two'])
            if inp == 's':
                self.hits.append(hit)
                break

    def filter_tweet(self, tweet):
        """
        filter out anagram-inappropriate tweets
        """
        LOW_CHAR_CUTOFF = 10
        MIN_UNIQUE_CHARS = 5
        # pass_flag = True
        # ignore tweets w/ non-ascii characters
        #check for retweets
        if tweet.get('retweeted_status'):
            return False
            self.stat_in_is_retweet += 1
        #check for mentions
        if len(tweet.get('entities').get('user_mentions')) is not 0:
            return False
            self.stat_in_has_mention += 1        
        try:
            tweet['text'].decode('ascii')
        except UnicodeEncodeError:
            return False
            self.stat_in_nonascii += 1
        # check for links:
        if len(tweet.get('entities').get('urls')) is not 0:
            return False
            self.stat_in_has_url += 1
        # ignore short tweets
        t = str(re.sub(r'[^a-zA-Z]', '', tweet['text']).lower())
        if len(t) <= LOW_CHAR_CUTOFF:
            return False
            self.stat_in_low_chars += 1
        # ignore tweets with few characters
        st = set(t)
        if len(st) < MIN_UNIQUE_CHARS:
            return False
            self.stat_in_low_unique_chars += 1
        return True

# some sample data from when we weren't returning on False
# total tweets seen:  127707
# had text:  111669  ( 0 %)
# non ascii: 47269  (, 42 %)
# had url: 12242  (, 10 %)
# has mention: 61852  (, 55 %)
# is retweet: 26174  (, 23 %)
# too short: 17409  (, 15 %)
# few unique chars: 11187  (, 10 %)



    def add_to_data(self, hashed_tweet):
        self.data[hashed_tweet['hash']] = hashed_tweet

    def make_hash(self, text):
        """
        takes a tweet as input. returns a character-unique hash
        from the tweet's text.
        """
        t_text = str(re.sub(r'[^a-zA-Z]', '', text).lower())
        t_hash = ''.join(sorted(t_text, key=str.lower))
        return t_hash

    def process_hit(self, new_tweet):
        """
        called when a duplicate is found.
        does some sanity checking. If that passes sends a msg to
        the boss for final human evaluation.
        """
        # tweet = hit = tweet_text = hit_text = None
        self.stat_hit_possible += 1
        hit_tweet = self.data.pop(new_tweet['hash'])
        # print("possible hit:", hashed_tweet['ID'], hit_id)
        if not hit_tweet:
            print('error retrieving hit')
            return

        if self.compare(new_tweet['text'], hit_tweet['text']):
            self.stat_hit_confirmed += 1
            hit = {
                "tweet_one": new_tweet,
                "tweet_two": hit_tweet,
            }
            self.hits.append(hit)
        else:
            self.add_to_data(new_tweet)

    def compare(self, tweet_one, tweet_two):
        """
        most basic test, finds if tweets are just identical
        """
        stripped_one = str(re.sub(r'[^a-zA-Z]', '', tweet_one).lower())
        stripped_two = str(re.sub(r'[^a-zA-Z]', '', tweet_two).lower())

        # cull identical tweets:
        for i in range(len(stripped_one)):
                if stripped_one[i] != stripped_two[i]:
                    return True
        return False

    def post_hit(self, hit):
        # check that the tweets still exist:
        try:
            self.twitter_handler.fetch_tweet(hit['tweet_one']['id'])
            self.twitter_handler.fetch_tweet(hit['tweet_two']['id'])
        except TweetUnavailableError:
            return

        # try to retweet:
        flag = self.twitter_handler.retweet(hit['tweet_one']['id'])
        if not flag:
            return False
        else:
            flag = self.twitter_handler.retweet(hit['tweet_two']['id'])
        if not flag:
            # if the first passes but the second does not delete the first
            self.twitter_handler.delete_last_tweet()
            return False
        return True

    def confirmed_hit(self, tweet1, tweet2):
        """
        called when we have a likely match that we want to send for
        review.
        """
        self.stat_hit_confirmed += 1
        # try:
        #     url_one = (r'http://twitter.com/' + tweet1['user']['screen_name']
        #                + r'/status/' + tweet1['id_str'])
        #     url_two = (r'http://twitter.com/' + tweet2['user']['screen_name']
        #                + r'/status/' + tweet2['id_str'])

        #     # msg_text = "possible match?: \n" + url_one + '\n' + url_two
        #     # self.twitter_handler.send_msg(msg_text)
        # except KeyError:
        #     print("ERR: KEY ERROR")
        #     print(tweet1)
        #     print(tweet2)

    def format_seconds(self, seconds):
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

        time_string = str(mm)+'m ' + str(ss) + 's'
        if hh or dd:
            time_string = str(hh) + 'h ' + time_string
        if dd:
            time_string = str(dd) + 'd ' + time_string
        return time_string


def main():
    anagramer = Anagramer()
    return anagramer.run()


if __name__ == "__main__":
    main()
