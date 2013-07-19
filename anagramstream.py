from __future__ import print_function
import requests
import json
from requests_oauthlib import OAuth1
from twittercreds import (CONSUMER_KEY, CONSUMER_SECRET,
                          ACCESS_KEY, ACCESS_SECRET)


# so let's start out barebons. we need to: open a connection twitter, and authenticate.

url = 'https://stream.twitter.com/1.1/statuses/sample.json'
auth = OAuth1(CONSUMER_KEY,CONSUMER_SECRET,
                  ACCESS_KEY, ACCESS_SECRET)

headers = {'Accept-Encoding': 'deflate, gzip',
                   'User-Agent': 'ANAGRAMATRON v0.5'}

query_params = {'language': 'en'}

stream = requests.get(url, auth=auth, stream=True, params=query_params, headers=headers)
print(stream.headers)
for line in stream.iter_lines():
    if line:
        tweet = json.loads(line)
        if tweet.get('text'):
            print(tweet.get('text'))


class AnagramStream(object):
    """
    very basic single-purpose object for connecting to the streaming API
    in most use-cases python-twitter-tools or tweepy would be preferred
    BUT we need both gzip compression and the 'language' parameter
    """
    def __init__(app_key, app_secret, consumer_key, consumer_secret,
                  endpoint='sample', languages='None', stall_warnings=True):
        self._app_key = app_key
        self._app_secret = app_secret
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self.url = 'https:/stream.twitter.com/1.1/statuses/%s.json' % endpoint
        self._languages = languages
        self._stall_warnings = stall_warnings
