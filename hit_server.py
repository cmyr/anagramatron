from __future__ import print_function
from bottle import (Bottle, run, request, server_names,
                    ServerAdapter, abort)
import time
import hitmanager
import anagramstats as stats
import daemon
# import os
# import sys

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
        print('daemon starting')
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
        
    success_flag = hitmanager.set_hit_status(hit_id, action)
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
    print(hit_ids)
    if not len(hit_ids):
        print('no ids -_-')

    if len(hit_ids) == 1:
        itwerked = hitmanager.set_hit_status(hit_ids[0], HIT_STATUS_SEEN)
        print('status changed? %s' % str(itwerked))
    for i in hit_ids:
        hitmanager.set_hit_status(i, HIT_STATUS_SEEN)

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
        flag = hitmanager.post_hit(hit_id)
        if flag:
            print('posting hit: %i' % hit_id)
    else:
        flag = hitmanager.approve_hit(hit_id)
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
    stats_dict = stats.stats_dict()
    new_hits = hitmanager.new_hits_count()
    last_post = hitmanager.last_post_time()
    return {'stats': stats_dict, 'new_hits': new_hits, 'last_post': last_post}


def start_hit_server(debug=False):
    if debug:
        run(app, host='127.0.0.1', port=TEST_PORT, debug=True)
    else:
        run(app, host='0.0.0.0', port=TEST_PORT, debug=True, server='sslbottle')
    return True


DAEMON_LOG_FILE = 'data/serverd.log'
DAEMON_LOCK = 'data/.server.lock'

def start_hit_daemon(debug=False):
    print('starting hit server daemon')
    # if existing_instance():
    #     return
    # else:
    #     set_lock()
    #     # start daemon
    with daemon.DaemonContext():
        start_hit_server(debug)

# it is unclear whether this is necessary, and whether it would work if it was.

# def existing_instance():

#     if os.access(DAEMON_LOCK, os.F_OK):
#         print('accessed lockfile')
#         #if the lockfile is already there then check the PID number 
#         #in the lock file
#         pidfile = open(DAEMON_LOCK, "r")
#         pidfile.seek(0)
#         old_pd = pidfile.readline()
#         print('found pidfile %d' % int(old_pd))
#         # Now we check the PID from lock file matches to the current
#         # process PID
#         if os.path.exists("/proc/%s" % old_pd):
#             print("You already have an instance of the program running")
#             print("It is running as process %s," % old_pd)
#             return True
#         else:

#             os.remove(DAEMON_LOCK)
#             return False
#     else:
#         print('no lock file found')

# def set_lock():
#     print('setting lock file')
#     pidfile = open(DAEMON_LOCK, "w")
#     pidfile.write("%s" % os.getpid())
#     pidfile.close


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--daemonize',
                        help='run as daemon', action="store_true")
    parser.add_argument('--debug',
                        help='run locally', action="store_true")
    args = parser.parse_args()
    if args.daemonize:
        start_hit_daemon(args.debug)
    else:
        start_hit_server(args.debug)
