from __future__ import print_function
from bottle import (Bottle, route, run, request, response, server_names,
                    ServerAdapter, abort)
import time
# import datahandler
import hitmanager

HIT_STATUS_REVIEW = 'review'
CLIENT_ACTION_POST = 'posted'
CLIENT_ACTION_REJECT = 'rejected'
CLIENT_ACTION_APPROVE = 'approved'
CLIENT_ACTION_FAILED = 'failed'

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
# data = None
app = Bottle()

def authenticate(auth):
    if auth == AUTH_TOKEN:
        return True
    print('failed authentication')
    abort(401, '-_-')
# actual bottle stuff


@app.route('/hits')
def get_hits():
    print(request)
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    # update data
    hits = hitmanager.all_hits()
    if (request.query.status):
        hits = [h for h in hits if h['status'] == request.query.status]
    else:
        hits = [h for h in hits if h['status'] not in [CLIENT_ACTION_POST, CLIENT_ACTION_REJECT, CLIENT_ACTION_FAILED]]
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
    if action == CLIENT_ACTION_POST:
        # if data.post_hit(hit_id):
        print('post requested')
        if hitmanager.post_hit(hit_id):
            return {'hit': hitmanager.get_hit(hit_id), 'response': True}
        else:
            return {'hit': hitmanager.get_hit(hit_id), 'response': False}
    if action == CLIENT_ACTION_APPROVE:
        print('approve requested')
        if hitmanager.approve_hit(hit_id):
            return {'hit': hitmanager.get_hit(hit_id), 'response': True}
        else:
            return {'hit': hitmanager.get_hit(hit_id), 'response': False}
    if action == CLIENT_ACTION_REJECT:
        print('reject requested')
        if hitmanager.reject_hit(hit_id):
            return {'hit': hitmanager.get_hit(hit_id), 'response': True}
        else:
            return {'hit': hitmanager.get_hit(hit_id), 'response': False}


@app.route('/blacklist')
def add_to_blacklist():
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    bad_hash = str(request.query.hash)
    print('blacklisting hash: %s' % bad_hash)
    hitmanager.add_to_blacklist(bad_hash)
    return {'response': True}
 

@app.route('/approve')
def approve_hit():
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return

    hit_id = int(request.query.id)
    post_now = bool(request.query.post_now)
    flag = None
    if (post_now):
        flag = hitmanager.post_hit(hit_id)
    else:
        flag = hitmanager.approve_hit(hit_id)
        print('posting hit: %i' % hit_id)
    return {'hit': hitmanager.get_hit(hit_id), 'response': flag}


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
    older_than = 0
    status = HIT_STATUS_REVIEW
    hits = hitmanager.all_hits()
    if (request.query.count):
        count = int(request.query.count)
    if (request.query.older_than):
        older_than = long(request.query.older_than)
    if (request.query.status):
        status = request.query.status

    print('client requested %i hits older then %i'
          % (count, older_than))
    hits = [h for h in hits if h['status'] == status]
    if older_than:
        hits = [h for h in hits if h['id'] < older_than]
    hits.reverse()
    return_hits = hits[:count]
    
    print("returned %i hits" % len(return_hits))
    for hit in return_hits:
        timestring = time.strftime("%d, %H:%M:%s",time.localtime(hit['timestamp']))
        print("%i: %s, %s" % (hit['id'], timestring, hit['status']))
    

    return {'hits': return_hits}

run(app, host='0.0.0.0', port=TEST_PORT, debug=True, server='sslbottle')

# if __name__ == "__main__":
#     print hit_for_id(1368809545607)
