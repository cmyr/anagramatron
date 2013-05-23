import datahandler
import twitterhandler
import tumblpy
import utils

from tumblrcreds import TUMBLR_KEY, TUMBLR_SECRET, TOKEN_KEY, TOKEN_SECRET
BLOG_URL = 'http://anagramatron.tumblr.com/'

tmbl = tumblpy.Tumblpy(app_key=TUMBLR_KEY,
                       app_secret=TUMBLR_SECRET,
                       oauth_token=TOKEN_KEY,
                       oauth_token_secret=TOKEN_SECRET
                       )

data = datahandler.DataHandler(just_the_hits=True)
twitter_handler = twitterhandler.TwitterHandler()
# HITS = data.get_all_hits()


HIT_STATUS_REVIEW = 'review'
HIT_STATUS_REJECTED = 'rejected'
HIT_STATUS_POSTED = 'posted'
HIT_STATUS_APPROVED = 'approved'
HIT_STATUS_MISC = 'misc'


def review_hits():
    hits = data.get_all_hits()
    hits = [h for h in hits if h['status'] in [HIT_STATUS_REVIEW]]
    print('recorded %i hits in need of review' % len(hits))
    for hit in hits:
        print(hit['tweet_one']['text'], hit['tweet_one']['id'])
        print(hit['tweet_two']['text'], hit['tweet_two']['id'])
    for hit in hits:
        print(hit['tweet_one']['id'], hit['tweet_two']['id']), hit['status']
        print(hit['tweet_one']['text'])
        print(hit['tweet_two']['text'])

        while 1:
            inp = raw_input("(a)ccept, (r)eject, (s)kip, (i)llustrate, (q)uit:")
            if inp == 'i':
                utils.show_anagram(hit['tweet_one']['text'], hit['tweet_two']['text'])
                continue
            if inp not in ['a', 'r', 's', 'q', 'i']:
                print("invalid input. Please enter 'a', 'r', 's', 'i' or 'q'.")
            else:
                break
        if inp == 'a':
            flag = post_hit(hit)
            if not flag:
                print('retweet failed, sorry bud')
                data.set_hit_status(hit['id'], HIT_STATUS_REJECTED)
            else:
                data.set_hit_status(hit['id'], HIT_STATUS_POSTED)
                print('post successful')
        if inp == 'r':
            # remove from list of hits
            data.set_hit_status(hit['id'], HIT_STATUS_REJECTED)
        if inp == 's':
            data.set_hit_status(hit['id'], HIT_STATUS_APPROVED)
        if inp == 'q':
            break
    data.finish()


def post_hit(hit):
# check that the tweets still exist:
    t1 = twitter_handler.fetch_tweet(hit['tweet_one']['id'])
    t2 = twitter_handler.fetch_tweet(hit['tweet_two']['id'])
    if not t1 or not t2:
        # print("tweet not found :(")
        return False
    # try to retweet:
    flag = twitter_handler.retweet(hit['tweet_one']['id'])
    if not flag:
        return False
    else:
        flag = twitter_handler.retweet(hit['tweet_two']['id'])
    if not flag:
        # if the first passes but the second does not delete the first
        twitter_handler.delete_last_tweet()
        return False

    # then post to tumblr:
    sn1 = t1.get('user').get('screen_name')
    sn2 = t2.get('user').get('screen_name')
    oembed1 = twitter_handler.oembed_for_tweet(hit['tweet_one']['id'])
    oembed2 = twitter_handler.oembed_for_tweet(hit['tweet_two']['id'])
    post_title = "@%s vs @%s" % (sn1, sn2)
    post_content = '<div class="tweet-pair">\n%s<br /><br />%s\n</div>' % (oembed1['html'], oembed2['html'])
    post = tmbl.post('post',
                     blog_url=BLOG_URL,
                     params={'type': 'text',
                             'title': post_title,
                             'body': post_content
                             })
    if not post:
        return False
    return True


if __name__ == "__main__":
    review_hits()
