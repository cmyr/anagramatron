from __future__ import print_function

import time
import logging
import cPickle as pickle

from twitterhandler import StreamHandler
from datahandler import (DataCoordinator, NeedsMaintenance)
import anagramstats as stats
import hit_server
import multiprocessing


LOG_FILE_NAME = 'data/anagramer.log'


def main():
    # set up logging:
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        format='%(asctime)s - %(levelname)s:%(message)s',
        level=logging.DEBUG
    )

    # hit_server.start_hit_daemon()
    hitserver = multiprocessing.Process(target=hit_server.start_hit_server)
    hitserver.daemon = True
    hitserver.start()

    data_coordinator = DataCoordinator()
    stats.clear_stats()

    while 1:
        print('top of run loop')
        logging.debug('top of run loop')
        try:
            print('starting stream handler')
            stream_handler = StreamHandler()
            stream_handler.start()
            for processed_tweet in stream_handler:
                data_coordinator.handle_input(processed_tweet)
                stats.update_console()

        except NeedsMaintenance:
            logging.debug('caught NeedsMaintenance exception')
            print('performing maintenance')
            stream_handler.close()
            data_coordinator.perform_maintenance()

        except KeyboardInterrupt:
            stream_handler.close()
            data_coordinator.close()
            break



if __name__ == "__main__":
    test()
    # main()

