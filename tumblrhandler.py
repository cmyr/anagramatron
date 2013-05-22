import tumblpy
import twitterhandler

from twitter.api import TwitterError
from tumblrcreds import TUMBLR_KEY, TUMBLR_SECRET, TOKEN_KEY, TOKEN_SECRET
BLOG_URL = 'http://anagramatron.tumblr.com/'

tmbl = tumblpy.Tumblpy(app_key=TUMBLR_KEY,
                       app_secret=TUMBLR_SECRET,
                       oauth_token=TOKEN_KEY,
                       oauth_token_secret=TOKEN_SECRET
                      )
twttr = twitterhandler.TwitterHandler()
# def post_test():




#     post = t.post('post',
#                   blog_url=BLOG_URL,
#                   params={'type': 'text',
#                           'title': 'test title!',
#                           'body': 'test my body'
#                           }
#                   )
#     print post

def post_hit(hit):
  try:
    n1 = twttr.fetch_tweet(hit['tweet_one']['id']).get('user').get('screen_name')
    n2 = twttr.fetch_tweet(hit['tweet_two']['id']).get('user').get('screen_name')
    t1 = twttr.oembed_for_tweet(hit['tweet_one']['id'])
    t2 = twttr.oembed_for_tweet(hit['tweet_two']['id'])
  except TwitterError:
    return False
  post_title = "%s vs %s" % (n1,n2)
  post_content = '%s<br />%s' % (t1['html'], t2['html'])
  post = tmbl.post('post',
                   blog_url=BLOG_URL,
                   params={'type': 'text',
                           'title': '',
                           'body': post_content
                           })
  if not post:
    return False
  return True



