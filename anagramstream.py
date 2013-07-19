from __future__ import print_function
import requests
import json
from requests_oauthlib import OAuth1
from twittercreds import (CONSUMER_KEY, CONSUMER_SECRET,
                          ACCESS_KEY, ACCESS_SECRET)



class AnagramStream(object):
    """
    very basic single-purpose object for connecting to the streaming API
    in most use-cases python-twitter-tools or tweepy would be preferred
    BUT we need both gzip compression and the 'language' parameter
    """
    def __init__(self, access_key, access_secret, consumer_key, consumer_secret):
        self._access_key = access_key
        self._access_secret = access_secret
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret

    def stream_iter(self, endpoint='sample', languages='None', stall_warnings=True):
        # auth = OAuth1(CONSUMER_KEY,CONSUMER_SECRET,
        #           ACCESS_KEY, ACCESS_SECRET)
        auth = OAuth1(self._access_key, self._access_secret,
                      self._consumer_key, self._consumer_secret)

        url = 'https://stream.twitter.com/1.1/statuses/%s.json' % endpoint
        query_headers = {'Accept-Encoding': 'deflate, gzip',
                         'User-Agent': 'ANAGRAMATRON v0.5'}
        query_params = dict()
        lang_string = None
        if languages:
            if type(languages) is list:
                lang_string = ','.join(languages)
            elif isinstance(languages, basestring):
                lang_string = languages

        if lang_string:
            query_params['language'] = lang_string
        if stall_warnings:
            query_params['stall_warnings'] = True

        if __name__ == '__main__':
            print(url, query_params, query_headers)
        stream_connection = requests.get(url, auth=auth, stream=True,
                                         params=query_params, headers=query_headers)
        return stream_connection.iter_lines()

if __name__ == '__main__':
    anagram_stream = AnagramStream(CONSUMER_KEY, CONSUMER_SECRET,
                                   ACCESS_KEY, ACCESS_SECRET)

    stream_connection = anagram_stream.stream_iter(languages=['es'])
    for line in stream_connection:
        if line:
            try:
                tweet = json.loads(line)
                if tweet.get('text'):
                    print(tweet.get('text'))
            except ValueError:
                print(line)
