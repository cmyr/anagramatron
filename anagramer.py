from __future__ import print_function

import time
import logging
import cPickle as pickle

from twitterhandler import StreamHandler
from datahandler import DataCoordinator
import anagramstats as stats


LOG_FILE_NAME = 'data/anagramer.log'


def main():
    # set up logging:
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        format='%(asctime)s - %(levelname)s:%(message)s',
        level=logging.DEBUG
    )
    stream_handler = StreamHandler()
    data_coordinator = DataCoordinator()
    server_process = None
    # server_process = subprocess.call('python hit_server.py')

    while 1:
        try:
            logging.info('entering run loop')
            stats.start_time = time.time()
            stream_handler.start()
            for processed_tweet in stream_handler:
                data_coordinator.handle_input(processed_tweet)
                stats.update_console()
        except KeyboardInterrupt:
            break
        finally:
            if server_process:
                server_process.terminate()
            stream_handler.close()
            stream_handler = None
            data_coordinator.close()
            stats.close()


def test(source, raw=True):
    stats._clear_stats()
    data_coordinator = DataCoordinator()
    for tweet in source:
        stats.tweets_seen()
        if raw:
            processed_tweet = filter_tweet(tweet)
            if processed_tweet:
                stats.passed_filter()
                data_coordinator.handle_input(processed_tweet)
        else:
            tweet_text = tweet.get('text') or tweet.get('tweet_text')
            tweet_id = tweet.get('id') or tweet.get('tweet_id')

            tweet_text = _correct_encodings(tweet_text)
            tweet = {'tweet_hash': improved_hash(tweet_text),
                     'tweet_id': tweet_id,
                     'tweet_text': tweet_text
                               }

            stats.passed_filter()
            data_coordinator.handle_input(tweet)

        stats.update_console()
    data_coordinator.close()


if __name__ == "__main__":
    main()
    # source = pickle.load(open('testdata/tst2.p', 'r'))
    # source = pickle.load(open('tstdata/20ktst1.p'))
    # test(source, False)
    # db_conversion_utility()
