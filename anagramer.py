from __future__ import print_function

import time
import logging
import cPickle as pickle

from twitterhandler import StreamHandler
from datahandler import (DataCoordinator, NeedsMaintenance)
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
    stats.clear_stats()

    while 1:
        try:
            logging.info('entering run loop')
            stream_handler.start()
            for processed_tweet in stream_handler:
                data_coordinator.handle_input(processed_tweet)
                stats.update_console()

        except NeedsMaintenance:
            stream_handler.close()
            data_coordinator.perform_maintenance()

        except KeyboardInterrupt:
            stream_handler.close()
            data_coordinator.close()
            stats.close()
            break


def test(source, raw=True):
    stats.clear_stats()
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

