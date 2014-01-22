# coding=utf-8

from __future__ import print_function
import time
import sys
import random


# from twitter.oauth import OAuth
# from twitter.stream import TwitterStream
# from twitter.api import Twitter, TwitterError, TwitterHTTPError

POST_INTERVAL = 240


class Daemon(object):

    """
    I sit idle, calm
    waiting for the call that wakes
    so I may hold forth
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
        last_post = self.datasource.last_post()
        temps_perdu = time.time() - last_post
        if last_post and temps_perdu < (self.post_interval / 2):
            print('skipping post. %d elapsed, post_interval %d' %
                  (temps_perdu, self.post_interval))

            self.sleep(self.post_interval - temps_perdu)

    def entertain_the_huddled_masses(self):

        count = self.datasource.count()
        self._check_count(count)
        print('datasource count = %d' % count)
        if not count:
            return

        haiku = self.datasource.haiku_for_post()
        formatted_haiku = self.format_haiku(haiku)

        if formatted_haiku and self.post(formatted_haiku):
            self.datasource.post_succeeded(haiku)
        else:
            self.datasource.post_failed(haiku)
            self.entertain_the_huddled_masses()



    def sleep(self, interval):
        interval = int(interval)
        randfactor = random.randrange(0, interval)
        interval = interval * 0.5 + randfactor
        sleep_chunk = 10  # seconds

        print('sleeping for %d minutes' % (interval / 60))

        while interval > 0:
            sleep_status = ' %s remaining \r' % (
                anagramfunctions.format_seconds(interval))
            sys.stdout.write(sleep_status.rjust(35))
            sys.stdout.flush()
            time.sleep(sleep_chunk)
            interval -= sleep_chunk

        print('\n')

    def send_dm(self, message):
        """sends me a DM if I'm running out of haiku"""
        try:
            self.twitter.direct_messages.new(user=BOSS_USERNAME, text=message)
        except TwitterError as err:
            print(err)


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
