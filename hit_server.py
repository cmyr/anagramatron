from __future__ import print_function
from bottle import Bottle, route, run, request, response, server_names, ServerAdapter
import datahandler

# SSL subclass of bottle cribbed from:
# http://dgtool.blogspot.com.au/2011/12/ssl-encryption-in-python-bottle.html

# Declaration of new class that inherits from ServerAdapter
# It's almost equal to the supported cherrypy class CherryPyServer


class MySSLCherryPy(ServerAdapter):
    def run(self, handler):
        import cherrypy
        from cherrypy import wsgiserver
        cherrypy.config.update('cherrypy.config')
        # print(cherrypy.config.items())
        server = cherrypy.wsgiserver.CherryPyWSGIServer(
                                                        (self.host, self.port),
                                                        handler,
                                                        numthreads=1,
                                                        max=1)
        # print(server.requests._threads)
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

# data = datahandler.DataHandler(just_the_hits=True)
# HITS = data.get_all_hits()
data = None

def hit_for_id(hit_id):
    for hit in HITS:
        if hit['id'] == hit_id:
            return hit


@route('/hits')
def get_hits():
    global data
    if not data:
        data = datahandler.DataHandler(just_the_hits=True)
    hits = data.get_all_hits()
    return {'hits': hits}


@route('/ok')
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


run(host='localhost', port=8080, debug=True, server='sslbottle')

# if __name__ == "__main__":
#     print hit_for_id(1368809545607)
