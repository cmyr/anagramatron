import datahandler
import twitterhandler

data = datahandler.DataHandler(just_the_hits=True)
twitter_handler = twitterhandler.TwitterHandler()
HITS = data.get_all_hits()


HIT_STATUS_REVIEW = 'review'
HIT_STATUS_REJECTED = 'rejected'
HIT_STATUS_POSTED = 'posted'
HIT_STATUS_APPROVED = 'approved'
HIT_STATUS_MISC = 'misc'

def review_hits():
    hit_count = len(HITS)
    print('recorded ' + str(hit_count) + ' hits in need of review')
    HITS = data.get_all_hits()
    no_rejects = [h for h in HITS if h['status'] is not HIT_STATUS_REJECTED]
    # just print them all to start
    for h in no_rejects:
        print(hit['tweet_one']['text'], hit['tweet_one']['id'])
        print(hit['tweet_two']['text'], hit['tweet_two']['id'])

    for hit in no_rejects:
        print(hit['tweet_one']['id'], hit['tweet_two']['id'])
        print(hit['tweet_one']['text'])
        print(hit['tweet_two']['text'])

        while 1:
            inp = raw_input("(a)ccept, (r)eject, (s)kip, (q)uit:")
            if inp not in ['a', 'r', 's', 'q']:
                print("invalid input. Please enter 'a', 'r', 's', or 'q'.")
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
            pass
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
    return True


if __name__ == "__main__":
    review_hits()
