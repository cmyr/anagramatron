# coding=utf-8

from __future__ import print_function
import time
import sys
import random
import hitmanager
import anagramfunctions
import constants
import requests




class Daemon(object):

    """
    A stand alone tool for automatic posting of approved anagrams
    """

    def __init__(self, post_interval=0, debug=False):
        super(Daemon, self).__init__()
        self.datasource = None
        self._debug = debug
        self.post_interval = post_interval

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
        print('checking last post time')
        last_post = hitmanager.last_post_time() or 0
        print('last post at %s' % str(last_post))
        temps_perdu = time.time() - last_post
        if last_post and temps_perdu < (self.post_interval or constants.ANAGRAM_POST_INTERVAL) / 2:
            print('skipping post. %d elapsed, post_interval %d' %
                  (temps_perdu, self.post_interval))

            self.sleep()

    def entertain_the_huddled_masses(self):

        # ah, experience, my old master
        try:
            requests.head('http://www.twitter.com')
        except Exception as err:
            print('server appears offline', err, sep='\n')
            return

        # get most recent hit:
        hit = hitmanager.next_approved_hit()
        if not hit:
            print('no postable hit found')
            return

        print(hit['tweet_one']['tweet_text'], hit['tweet_two']['tweet_text'])
        if not hitmanager.post_hit(hit['id']):
            print('failed to post hit')
            # on failed post attempt again
            self.entertain_the_huddled_masses()
        else:
            print('posted hit')

    def sleep(self, interval=0, debug=False):
        interval = int(interval)
        
        if not interval:
            reload(constants)
            interval = constants.ANAGRAM_POST_INTERVAL * 60

        print('base interval is %d' % (interval / 60))

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


# some reference stuff if we want to make this an actual daemon:


# def existing_instance():

#     if os.access(DAEMON_LOCK, os.F_OK):
#         print('accessed lockfile')
# if the lockfile is already there then check the PID number
# in the lock file
#         pidfile = open(DAEMON_LOCK, "r")
#         pidfile.seek(0)
#         old_pd = pidfile.readline()
#         print('found pidfile %d' % int(old_pd))
# Now we check the PID from lock file matches to the current
# process PID
#         if os.path.exists("/proc/%s" % old_pd):
#             print("You already have an instance of the program running")
#             print("It is running as process %s," % old_pd)
#             return True
#         else:

#             os.remove(DAEMON_LOCK)
#             return False
#     else:
#         print('no lock file found')

# def set_lock():
#     print('setting lock file')
#     pidfile = open(DAEMON_LOCK, "w")
#     pidfile.write("%s" % os.getpid())
#     pidfile.close


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
    kwargs['post_interval'] = args.post_interval or 0

    print(kwargs)
    print(type(kwargs['post_interval']))

    daemon = Daemon(**kwargs)
    return daemon.run()


if __name__ == "__main__":
    main()
