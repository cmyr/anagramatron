# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmq

def zmq_iter(host="localhost", port=8069):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    socket.connect("tcp://%s:%s" % (host, str(port)))
    while True:
        try:
            yield socket.recv_json()
        except KeyboardInterrupt as err:
            socket.close()
            context.term()
            break

def main():
    for msg in zmq_iter():
        print(msg.get('text'))

if __name__ == "__main__":
    main()