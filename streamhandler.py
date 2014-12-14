from __future__ import print_function

import logging
import Queue
import multiprocessing
import time

from collections import deque
from ssl import SSLError
from socket import error as SocketError
from urllib2 import HTTPError

from twitter.stream import TwitterStream
from twitter.api import TwitterError, TwitterHTTPError
import json

import anagramfunctions
import anagramstats as stats
from anagramstream import AnagramStream


# my twitter OAuth key:
from twittercreds import (CONSUMER_KEY, CONSUMER_SECRET,
                          ACCESS_KEY, ACCESS_SECRET, 
                          BOSS_USERNAME, PRIVATE_POST_URL)

from constants import (ANAGRAM_STREAM_BUFFER_SIZE)

SECONDS_SINCE_LAUNCH_TO_IGNORE_BUFFER = 30 * 60

class StreamHandler(object):
    """
    handles twitter stream connections. Buffers incoming tweets and
    acts as an iter.
    """
    def __init__(self,
                 buffersize=ANAGRAM_STREAM_BUFFER_SIZE,
                 timeout=90,
                 languages=['en']
                 ):
        self.buffersize = buffersize
        self.timeout = timeout
        self.languages = languages
        self.stream_process = None
        self.queue = multiprocessing.Queue()
        self._error_queue = multiprocessing.Queue()
        self._buffer = deque()
        self._should_return = False
        self._iter = self.__iter__()
        self._tweets_seen = multiprocessing.Value('L', 0)
        self._passed_filter = multiprocessing.Value('L', 0)
        self._lock = multiprocessing.Lock()
        self._backoff_time = 0
        self._start_time = time.time()


    def update_stats(self):
        with self._lock:
            if self._tweets_seen.value:
                stats.tweets_seen(self._tweets_seen.value)
                self._tweets_seen.value = 0
            if self._passed_filter.value:
                stats.passed_filter(self._passed_filter.value)
                self._passed_filter.value = 0
        stats.set_buffer(self.bufferlength())

    def __iter__(self):
        """
        the connection to twitter is handled in another process
        new tweets are added to self.queue as they arrive.
        on each call to iter we move any tweets in the queue to a fifo buffer
        this makes keeping track of the buffer size a lot cleaner.
        """
        while 1:
            if self._should_return:
                print('breaking iteration')
                raise StopIteration
            while 1:
                # add all new items from the queue to the buffer
                try:
                    # after launch we don't have any keys in memory, so processing is slow.
                    # this checks if launch was recent, and resets the buffer if it was.
                    if len(self._buffer) > ANAGRAM_STREAM_BUFFER_SIZE * 0.9:
                        if time.time() - self._start_time < SECONDS_SINCE_LAUNCH_TO_IGNORE_BUFFER:
                            self._buffer = self._buffer[:100]  # keep a hundred items in buffer
                            logging.debug('recent launch, reset buffer')

                    self._buffer.append(self.queue.get_nowait())
                except Queue.Empty:
                    break
            try:
                self.update_stats()
                if len(self._buffer):
                    # if there's a buffer element return it
                    yield self._buffer.popleft()
                else:
                    yield self.queue.get(True, self.timeout)
                    self._backoff_time = 0
                    continue
            except Queue.Empty:
                print('queue timeout, restarting thread')
                # means we've timed out, and should try to reconnect
                self._stream_did_timeout()
        print('exiting iter loop')

    def next(self):
        return self._iter.next()

    def start(self):
        """
        creates a new thread and starts a streaming connection.
        If a thread already exists, it is terminated.
        """
        self._should_return = False
        print('creating new server connection')
        logging.debug('creating new server connection')
        if self.stream_process is not None:
            print('terminating existing server connection')
            logging.debug('terminating existing server connection')
            self.stream_process.terminate()
            if self.stream_process.is_alive():
                pass
            else:
                print('thread terminated successfully')
                logging.debug('thread terminated successfully')

        self.stream_process = multiprocessing.Process(
                                target=self._run,
                                args=(self.queue,
                                      self._error_queue,
                                      self._backoff_time,
                                      self._tweets_seen,
                                      self._passed_filter,
                                      self._lock,
                                      self.languages))
        self.stream_process.daemon = True
        self.stream_process.start()

        print('created process %i' % self.stream_process.pid)

    def _stream_did_timeout(self):
        """
        check for errors and choose a reconnection strategy.
        see: (https://dev.twitter.com/docs/streaming-apis/connecting#Stalls)
        """
        err = None
        while 1:
            # we could possible have more then one error?
            try:
                err = self._error_queue.get_nowait()
                logging.error('received error from stream process', err)
                print(err, 'backoff time:', self._backoff_time)
            except Queue.Empty:
                break
        if err:
            print(err)
            error_code = err.get('code')

            if error_code == 420:
                if not self._backoff_time:
                    self._backoff_time = 60
                else:
                    self._backoff_time *= 2
            else:
                # a placeholder, for now
                # elif error_code in [400, 401, 403, 404, 405, 406, 407, 408, 410]:
                if not self._backoff_time:
                    self._backoff_time = 5
                else:
                    self._backoff_time *= 2
                if self._backoff_time > 320:
                    self._backoff_time = 320
            # if error_code == 'TCP/IP level network error':
            #     self._backoff_time += 0.25
            #     if self._backoff_time > 16.0:
            #         self._backoff_time = 16.0
        self.start()

    def close(self):
        """
        terminates existing connection and returns
        """
        self._should_return = True
        if self.stream_process:
            self.stream_process.terminate()
        print("\nstream handler closed with buffer size %i" %
              (self.bufferlength))
        logging.debug("stream handler closed with buffer size %i" %
              (self.bufferlength))

    def bufferlength(self):
        return len(self._buffer)

    def _run(self, queue, errors, backoff_time, seen, passed, lock, languages):
        """
        handle connection to streaming endpoint.
        adds incoming tweets to queue.
        runs in own process.
        errors is a queue we use to transmit exceptions to parent process.
        """
        # if we've been given a backoff time, sleep
        if backoff_time:
            time.sleep(backoff_time)
        stream = AnagramStream(
            CONSUMER_KEY,
            CONSUMER_SECRET,
            ACCESS_KEY,
            ACCESS_SECRET)

        try:
            stream_iter = stream.stream_iter(languages=languages)
            logging.debug('stream begun')
            for tweet in stream_iter:
                if tweet is not None:
                    try:
                        tweet = json.loads(tweet)
                    except ValueError:
                        continue
                    if not isinstance(tweet, dict):
                        continue
                    if tweet.get('warning'):
                        print('\n', tweet)
                        logging.warning(tweet)
                        errors.put(dict(tweet))
                        continue
                    if tweet.get('disconnect'):
                        logging.warning(tweet)
                        errors.put(dict(tweet))
                        continue
                    if tweet.get('text'):
                        with lock:
                            seen.value += 1
                        processed_tweet = anagramfunctions.filter_tweet(tweet)
                        if processed_tweet:
                            with lock:
                                passed.value += 1
                            try:
                                queue.put(processed_tweet, block=False)
                            except Queue.Full:
                                pass

        except (HTTPError, SSLError, TwitterHTTPError, SocketError) as err:
            print(type(err))
            print(err)
            error_dict = {'error': str(err), 'code': err.code}
            errors.put(error_dict)


if __name__ == "__main__":

    count = 0
    stream = StreamHandler()
    stream.start()

    for t in stream:
        count += 1
        # print(count)

        print(t['tweet_text'], 'buffer length: %i' % len(stream._buffer))
        # if count > 100:
        #     stream.close()