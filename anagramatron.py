from __future__ import print_function

import time
import logging
import sys
from datetime import datetime
import cPickle as pickle

from twitterhandler import TwitterHandler
from streamhandler import StreamHandler
from anagramfinder import (AnagramFinder, NeedsMaintenance)
import anagramstats as stats
import hit_server
import multiprocessing


LOG_FILE_NAME = 'data/anagramer.log'


def run(server_only=False):
    # set up logging:
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        format='%(asctime)s - %(levelname)s:%(message)s',
        level=logging.DEBUG
    )

    if server_only:
        hit_server.start_hit_server()
    else:

        hitserver = multiprocessing.Process(target=hit_server.start_hit_server)
        hitserver.daemon = True
        hitserver.start()

        anagram_finder = AnagramFinder()
        stats.clear_stats()

        while 1:
            print('top of run loop')
            logging.debug('top of run loop')
            try:
                print('starting stream handler')
                stream_handler = StreamHandler()
                stream_handler.start()
                for processed_tweet in stream_handler:
                    anagram_finder.handle_input(processed_tweet)
                    stats.update_console()

            except NeedsMaintenance:
                logging.debug('caught NeedsMaintenance exception')
                print('performing maintenance')
                stream_handler.close()
                anagram_finder.perform_maintenance()

            except KeyboardInterrupt:
                stream_handler.close()
                anagram_finder.close()
                return 0

            except Exception as err:
                logging.error(sys.exc_info())
                stream_handler.close()
                anagram_finder.close()
                TwitterHandler().send_message(str(err) +
                                              "\n" +
                                              datetime.today().isoformat())
                print(sys.exc_info())
                return 1


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--server-only',
        help="run in server mode only",
        action="store_true")
    args = parser.parse_args()

    return run(args.server_only)


if __name__ == "__main__":
    main()
