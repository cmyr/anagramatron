from bottle import route, run, request, response
import datahandler

data = datahandler.DataHandler(just_the_hits=True)
HITS = data.get_all_hits()


def hit_for_id(hit_id):
    for hit in HITS:
        if hit['id'] == hit_id:
            return hit


@route('/hits')
def get_hits():
    HITS = data.get_all_hits()
    return {'hits': HITS}


@route('/rt')
def retweet():
    hit_id = int(request.query.id)
    hit = hit_for_id(hit_id)
    # return str(hit_id) + hit
    return "retweeted '%s' and '%s'" % (hit['tweet_one']['text'], hit['tweet_two']['text'])


@route('/del')
def delete():
    hit_id = int(request.query.id)
    data.remove_hit(hit_id)
    return "success"


run(host='localhost', port=8080, debug=True)

# if __name__ == "__main__":
#     print hit_for_id(1368809545607)
