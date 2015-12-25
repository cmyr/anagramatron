from __future__ import print_function

import os

from bottle import (Bottle, run, request, server_names,
                    ServerAdapter, abort)

from . import hitmanager, anagramstats, common

from .hitmanager import (HIT_STATUS_REVIEW, HIT_STATUS_SEEN,
                         HIT_STATUS_POSTED, HIT_STATUS_APPROVED)
# SSL subclass of bottle cribbed from:
# http://dgtool.blogspot.com.au/2011/12/ssl-encryption-in-python-bottle.html

# Declaration of new class that inherits from ServerAdapter
# It's almost equal to the supported cherrypy class CherryPyServer

from .serverauth import AUTH_TOKEN, TEST_PORT


class MySSLCherryPy(ServerAdapter):
    def run(self, handler):
        import cherrypy
        from cherrypy import wsgiserver
        server = cherrypy.wsgiserver.CherryPyWSGIServer(
                                                        (self.host, self.port),
                                                        handler,
                                                        numthreads=1,
                                                        max=1)
        # If cert path exists SSL will be used
        # You can set it to None to disable SSL
        # cert_path = 'data/server.pem'  # certificate path
        if os.path.exists(common.ANAGRAM_SEC_DIR):
            cert = os.path.join(common.ANAGRAM_SEC_DIR, 'cert.pem')
            priv = os.path.join(common.ANAGRAM_SEC_DIR, 'privkey.pem')
            assert os.path.exists(cert) and os.path.exists(priv), 'missing SSL credential'

            server.ssl_certificate = cert
            server.ssl_private_key = priv
            server.ssl_module = 'builtin'

            print('running with ssl')
        else:
            print('no cert found, running without ssl')
        try:
            server.start()
        finally:
            server.stop()

# Add our new MySSLCherryPy class to the supported servers
# under the key 'mysslcherrypy'

server_names['sslbottle'] = MySSLCherryPy
app = Bottle()

manager = None


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
        cutoff = hitmanager.HitDBManager.MAX_HIT_ID
    if (request.query.count):
        count = int(request.query.count)

    print('client requested %i hits with %s status, from %i' %
          (count, status, cutoff))
    hits = manager.all_hits(status, cutoff)
    total_hits = len(hits)
    print('hitmanager returned %i hits' % total_hits)
    hits = sorted(hits, key=lambda k: k['id'], reverse=True)
    hits = hits[:count]
    print("returned %i hits" % len(hits))
    return {'hits': hits, 'total_count': total_hits}


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
        
    success_flag = manager.set_hit_status(hit_id, action)
    success_string = 'succeeded' if success_flag else 'FAILED'
    print('modification of hit %i to status %s %s'
          % (hit_id, action, success_string))
    return {'action': action, 'hit': hit_id, 'success': success_flag}


@app.route('/seen')
def mark_seen():
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    hit_ids = request.query.hits
    hit_ids = hit_ids.split(',')
    print('clearing %d hits' % len(hit_ids))

    if len(hit_ids) == 1:
        itwerked = manager.set_hit_status(hit_ids[0], HIT_STATUS_SEEN)
        print('status changed? %s' % str(itwerked))
    else:
        manager.seen_hits(hit_ids)

    return {'action': HIT_STATUS_SEEN, 'count': len(hit_ids)}


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
        flag = manager.post_hit(hit_id)
        if flag:
            print('posting hit: %i' % hit_id)
    else:
        flag = manager.approve_hit(hit_id)
        if flag:
            print('approved hit: %i' % hit_id)

    action = HIT_STATUS_POSTED if post_now else HIT_STATUS_APPROVED
    return {
        'action': action,
        'hit': hit_id,
        'success': flag}


@app.route('/info')
def info():
    """
    returns some basic stats about what's happening on the server.
    """
    auth = request.get_header('Authorization')
    if not authenticate(auth):
        return
    stats_dict = anagramstats.StatTracker().stats_dict()
    new_hits = manager.new_hits_count()
    last_post = manager.last_post_time()
    return {'stats': stats_dict, 'new_hits': new_hits, 'last_post': last_post}


def start_hit_server(db_path, debug=False):
    global manager
    manager = hitmanager.HitDBManager(db_path)
    if debug:
        run(app, host='127.0.0.1', port=TEST_PORT, debug=True)
    else:
        run(app, host='0.0.0.0', port=TEST_PORT, debug=True, server='sslbottle')
    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    # parser.add_argument('-d', '--daemonize',
    #                     help='run as daemon', action="store_true")
    parser.add_argument('--debug',
                        help='run locally', action="store_true")
    args = parser.parse_args()
    # if args.daemonize:
    #     start_hit_daemon(args.debug)
    # else:
    start_hit_server(args.debug)
