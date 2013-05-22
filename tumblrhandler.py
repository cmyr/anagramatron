import tumblpy
import twitterhandler

from tumblrcreds import TUMBLR_KEY, TUMBLR_SECRET, TOKEN_KEY, TOKEN_SECRET
BLOG_URL = 'http://anagramatron.tumblr.com/'

def post_test():

    t = tumblpy.Tumblpy(app_key=TUMBLR_KEY,
                        app_secret=TUMBLR_SECRET,
                        oauth_token=TOKEN_KEY,
                        oauth_token_secret=TOKEN_SECRET
                        )
    post = t.post('post',
                  blog_url=BLOG_URL,
                  params={'type': 'text',
                          'title': 'test title!',
                          'body': 'test my body'
                          }
                  )
    print post

