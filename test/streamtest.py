from __future__ import print_function
from twitter.stream import TwitterStream
from twitter.oauth import OAuth

CONSUMER_KEY = "EcQJ04E5vdcf2khvPJL6g"
CONSUMER_SECRET = "nFPGsLhnkJjjJhWxSP7CvW0WR2F3pgKgrpRODdj4CKI"
ACCESS_KEY = "1170208442-WqVqXDJawBfcuXcB5D6tSutSuYMpXwlXqmxwZeA"
ACCESS_SECRET = "ULWFvn6LKNQYiRqVClHokxgBxUtCpYYSxPb6rQUc"

# th = twitterhandler.TwitterHandler()
stream = TwitterStream(
    auth=OAuth(ACCESS_KEY,
        ACCESS_SECRET,
        CONSUMER_KEY,
        CONSUMER_SECRET),
    api_version='1.1',
    secure='True',
    _timeout=1)

stream_iter = stream.statuses.sample()

count = 0
for tweet in stream_iter:
    count += 1
    print(str(count) + '\r')
