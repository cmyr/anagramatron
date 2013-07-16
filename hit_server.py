from __future__ import print_function
from bottle import (Bottle, route, run, request, response, server_names,
                    ServerAdapter, abort)
# import datahandler
import hitmanager


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
    # global data
    # if not data:
    #     data = datahandler.DataHandler(just_the_hits=True)
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
            return {'hit': data.get_hit(hit_id), 'response': True}
        else:
            return {'hit': data.get_hit(hit_id), 'response': False}
    if action == CLIENT_ACTION_APPROVE:
        print('approve requested')
        if hitmanager.approve_hit(hit_id):
            return {'hit': data.get_hit(hit_id), 'response': True}
        else:
            return {'hit': data.get_hit(hit_id), 'response': False}
    if action == CLIENT_ACTION_REJECT:
        print('reject requested')
        if hitmanager.reject_hit(hit_id):
            return {'hit': data.get_hit(hit_id), 'response': True}
        else:
            return {'hit': data.get_hit(hit_id), 'response': False}


run(app, host='0.0.0.0', port=TEST_PORT, debug=True, server='sslbottle')

# if __name__ == "__main__":
#     print hit_for_id(1368809545607)
