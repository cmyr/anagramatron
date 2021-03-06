from __future__ import print_function

import logging
import queue as Queue
import multiprocessing
import time

from collections import deque


from . import anagramfunctions, twitterhandler
from .anagramstats import StatTracker
from zmqstream.consumer import zmq_iter

from .common import (ANAGRAM_STREAM_BUFFER_SIZE)

SECONDS_SINCE_LAUNCH_TO_IGNORE_BUFFER = 60 * 60 * 2


class StreamHandler(object):

    """
    handles twitter stream connections. Buffers incoming tweets and
    acts as an iter.
    """

    def __init__(self,
                 buffersize=ANAGRAM_STREAM_BUFFER_SIZE,
                 timeout=90,
                 languages=['en'],
                 host="127.0.0.1",
                 port="8069"
                 ):
        self.buffersize = buffersize
        self.timeout = timeout
        self.languages = languages
        self.host = host
        self.port = port
        print(host, port)
        self.stream_process = None
        self.queue = multiprocessing.Queue()
        self._buffer = deque()
        self._should_return = False
        self._iter = self.__iter__()
        self._tweets_seen = multiprocessing.Value('L', 0)
        self._passed_filter = multiprocessing.Value('L', 0)
        self._lock = multiprocessing.Lock()
        self._start_time = time.time()
        self._last_message_check = self._start_time
        self.stats = StatTracker()

    def update_stats(self):
        with self._lock:
            if self._tweets_seen.value:
                self.stats['tweets_seen'] += self._tweets_seen.value
                self._tweets_seen.value = 0
            if self._passed_filter.value:
                self.stats['passed_filter'] += self._passed_filter.value
                self._passed_filter.value = 0
        self.stats['buffer'] = self.bufferlength()

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
                    self._buffer.append(self.queue.get_nowait())
                except Queue.Empty:
                    break
            try:
                # after launch we don't have any keys in memory, so processing is slow.
                # this checks if launch was recent, and resets the buffer if it
                # was.
                if len(self._buffer) > ANAGRAM_STREAM_BUFFER_SIZE * 0.9:
                    if time.time() - self._start_time < SECONDS_SINCE_LAUNCH_TO_IGNORE_BUFFER:
                        self._buffer = deque()
                        logging.debug('recent launch, reset buffer')

                self.update_stats()
                # 5 minutes
                if time.time() - self._last_message_check > (5 * 60):
                    self._last_message_check = time.time()
                    twitterhandler.TwitterHandler().handle_directs()

                if len(self._buffer):
                    # if there's a buffer element return it
                    yield self._buffer.popleft()
                else:
                    yield self.queue.get(True, self.timeout)
                    continue
            except Queue.Empty:
                print('queue timeout')
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
                  self._tweets_seen,
                  self._passed_filter,
                  self._lock,
                  self.languages))
        self.stream_process.daemon = True
        self.stream_process.start()

        print('created process %i' % self.stream_process.pid)

    def close(self):
        """
        terminates existing connection and returns
        """
        self._should_return = True
        if self.stream_process:
            self.stream_process.terminate()
        print("\nstream handler closed with buffer size %i" %
              (self.bufferlength()))
        logging.debug("stream handler closed with buffer size %i" %
                      (self.bufferlength()))

    def bufferlength(self):
        return len(self._buffer)

    def _run(self, queue, seen, passed, lock, languages):
        """
        handle connection to streaming endpoint.
        adds incoming tweets to queue.
        runs in own process.
        errors is a queue we use to transmit exceptions to parent process.
        """

        stream_iter = zmq_iter(host=self.host, port=self.port)
        logging.debug('stream begun')
        for tweet in stream_iter:
            if not isinstance(tweet, dict):
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
