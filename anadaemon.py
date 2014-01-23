# coding=utf-8

from __future__ import print_function
import time
import sys
import random
import hitmanager
import anagramfunctions

POST_INTERVAL = 120


class Daemon(object):

    """
    A stand alone tool for automatic posting of approved anagrams
    """

    def __init__(self, post_interval=POST_INTERVAL, debug=False):
        super(Daemon, self).__init__()
        self.datasource = None
        self._debug = debug
        self.post_interval = post_interval * 60

    def run(self):
        try:
            self._check_post_time()
            while True:
                self.entertain_the_huddled_masses()
                self.sleep(self.post_interval)

        except KeyboardInterrupt:
            print('exiting')
            sys.exit(0)

    def _check_post_time(self):
        last_post = hitmanager.last_post_time() or 0
        temps_perdu = time.time() - last_post
        if last_post and temps_perdu < (self.post_interval / 2):
            print('skipping post. %d elapsed, post_interval %d' %
                  (temps_perdu, self.post_interval))

            self.sleep(self.post_interval - temps_perdu)

    def entertain_the_huddled_masses(self):

        # get most recent hit:
        hit = hitmanager.next_approved_hit()
        if not hit:
            print('no postable hit found')
            return

        if not hitmanager.post_hit(hit['id']):
            # on failed post attempt again
            self.entertain_the_huddled_masses()

    def sleep(self, interval, debug=False):
        interval = int(interval)
        randfactor = random.randrange(0, interval)
        interval = interval * 0.5 + randfactor
        sleep_chunk = 10  # seconds

        print('sleeping for %d minutes' % (interval / 60))

        if not debug:
            while interval > 0:
                sleep_status = ' %s remaining \r' % (
                    anagramfunctions.format_seconds(interval))
                sys.stdout.write(sleep_status.rjust(35))
                sys.stdout.flush()
                time.sleep(sleep_chunk)
                interval -= sleep_chunk

            print('\n')

        else:
            return interval / 60


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--post-interval', type=int,
                        help='interval (in minutes) between posts')
    parser.add_argument('-d', '--debug',
                        help='run with debug flag', action="store_true")
    args = parser.parse_args()

    kwargs = {}
    kwargs['debug'] = args.debug
    kwargs['post_interval'] = args.post_interval or POST_INTERVAL

    print(kwargs)
    print(type(kwargs['post_interval']))

    daemon = Daemon(**kwargs)
    return daemon.run()


if __name__ == "__main__":
    main()
