from __future__ import print_function
from bottle import (Bottle, run, request, server_names,
                    ServerAdapter, abort)
import time
import hitmanager
import anagramstats as stats


from hitmanager import (HIT_STATUS_REVIEW, HIT_STATUS_SEEN, HIT_STATUS_MISC,
    HIT_STATUS_REJECTED, HIT_STATUS_POSTED, HIT_STATUS_APPROVED)
# SSL subclass of bottle cribbed from:
# http://dgtool.blogspot.com.au/2011/12/ssl-encryption-in-python-bottle.html

# Declaration of new class that inherits from ServerAdapter
# It's almost equal to the supported cherrypy class CherryPyServer

from serverauth import AUTH_TOKEN, TEST_PORT


class MySSLCherryPy(ServerAdapter):
    def run(self, handler):
        import cherrypy
        from cherrypy import wsgiserver
        server = cherrypy.wsgiserver.CherryPyWSGIServer(
                                                        (self.host, self.port),
                                                        handler,
                                                        numthreads=1,
                                                        max=1)
        # If cert variable is has a valid path, SSL will be used
        # You can set it to None to disable SSL
        cert = 'data/server.pem'  # certificate path
        server.ssl_certificate = cert
        server.ssl_private_key = cert
        try:
            server.start()
        finally:
            server.stop()

# Add our new MySSLCherryPy class to the supported servers
# under the key 'mysslcherrypy'

server_names['sslbottle'] = MySSLCherryPy
app = Bottle()

def authenticate(auth):
    return True
    if auth == AUTH_TOKEN:
        return True
    print('failed authentication')
    abort(401, '-_-')
# actual bottle stuff


@app.route('/hits')
def get_hits():
    print(request)
    count = 50
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    # update data
    try:
        status = str(request.query.status)
    except ValueError:
        status = HIT_STATUS_REVIEW
    try:
        cutoff = int(request.query.cutoff)
    except ValueError:
        cutoff = 0
    if (request.query.count):
        count = int(request.query.count)

    print('client requested %i hits with %s status, from %i' % 
        (count, status, cutoff))    
    hits = hitmanager.all_hits(status, cutoff)
    print('hitmanager returned %i hits' % len(hits))
    hits = hits[:count]
    print("returned %i hits" % len(hits))
    return {'hits': hits}


@app.route('/mod')
def modify_hit():
    print(request)
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    hit_id = int(request.query.id)
    action = str(request.query.status)
    print(hit_id, action)
    if not hit_id or not action:
        abort(400, 'v0_0v')
    if action == HIT_STATUS_POSTED:
        # if data.post_hit(hit_id):
        print('post requested')
        if hitmanager.post_hit(hit_id):
            return {'hit': hitmanager.get_hit(hit_id), 'response': True}
        else:
            return {'hit': hitmanager.get_hit(hit_id), 'response': False}
    if action == HIT_STATUS_APPROVED:
        print('approve requested')
        if hitmanager.approve_hit(hit_id):
            return {'hit': hitmanager.get_hit(hit_id), 'response': True}
        else:
            return {'hit': hitmanager.get_hit(hit_id), 'response': False}
    if action == HIT_STATUS_REJECTED:
        print('reject requested')
        if hitmanager.reject_hit(hit_id):
            return {'hit': hitmanager.get_hit(hit_id), 'response': True}
        else:
            return {'hit': hitmanager.get_hit(hit_id), 'response': False}
    if action == HIT_STATUS_SEEN:
        print('ignore requested')
        if hitmanager.set_hit_status(hit_id, HIT_STATUS_SEEN):
            return {'hit': hitmanager.get_hit(hit_id), 'response': True}
        else:
            return {'hit': hitmanager.get_hit(hit_id), 'response': False}


@app.route('/seen')
def mark_seen():
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    hit_ids = request.query.hits
    hit_ids = hit_ids.split(',')
    print(hit_ids)
    if not len(hit_ids):
        print('no ids -_-')

    if len(hit_ids) == 1:
        itwerked = hitmanager.set_hit_status(hit_ids[0], HIT_STATUS_SEEN)
        print('status changed? %s' % str(itwerked))
    for i in hit_ids:
        hitmanager.set_hit_status(i, HIT_STATUS_SEEN)




@app.route('/blacklist')
def add_to_blacklist():
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    bad_hash = str(request.query.hash)
    print('blacklisting hash: %s' % bad_hash)
    hitmanager.add_to_blacklist(bad_hash)


@app.route('/approve')
def approve_hit():
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    print("/approve endpoint received request:", request.json, request.params)
    hit_id = int(request.query.id)
    post_now = int(request.query.post_now)
    print(hit_id, post_now)
    flag = None
    if (post_now):
        flag = hitmanager.post_hit(hit_id)
        print('posting hit: %i' % hit_id)
    else:
        flag = hitmanager.approve_hit(hit_id)
        print('approved hit: %i' % hit_id)
    return {'hit': hitmanager.get_hit(hit_id), 'response': flag}


@app.route('/info')
def info():
    """
    returns some basic stats about what's happening on the server.
    """
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return

    # last_hit = request.query.last_hit

    stats_dict = stats.stats_dict()
    new_hits = hitmanager.new_hits_count()
    last_post = hitmanager.last_post_time()
    return {'stats': stats_dict, 'new_hits': new_hits}


# API v: 2.0:
@app.route('/2.0/hits')
def get_hits2():
    """
    can take two arguments, count and older_than.
    count is the number of hits to return.
    older_than is a hit_id.
    """
    print('new hits requested')
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return

    count = 50
    cutoff = 0
    get_new = False
    status = HIT_STATUS_REVIEW
    hits = hitmanager.all_hits()
    if (request.query.count):
        count = int(request.query.count)
    try:
        cutoff = long(request.query.cutoff)
    except ValueError:
        cutoff = 0
        # becuase we use %d to find a value below
    if (request.query.status):
        status = request.query.status
    if (request.query.get_new):
        get_new = True

    msgstring = "new" if get_new else "old"
    print('client requested %i %s hits with cutoff %i'
          % (count, msgstring, cutoff))
    hits = [h for h in hits if h['status'] == status]
    if cutoff and get_new:
        hits = [h for h in hits if h['id'] > cutoff]
    elif cutoff and not get_new:
        hits = [h for h in hits if h['id'] < cutoff]
    hits.reverse()
    return_hits = hits[:count]

    print("returned %i hits" % len(return_hits))
    for hit in return_hits:
        timestring = time.strftime("%d, %H:%M:%S",time.localtime(hit['timestamp']))
        print("%i: %s, %s" % (hit['id'], timestring, hit['status']))

    if return_hits:
        # hitmanager.server_sent_hits(return_hits)
        return {'hits': return_hits}
    else:
        return {'hits': None}

run(app, host='0.0.0.0', port=TEST_PORT, debug=True, server='sslbottle')
# run(app, host='127.0.0.1', port=TEST_PORT, debug=True)

# if __name__ == "__main__":
#     print hit_for_id(1368809545607)
