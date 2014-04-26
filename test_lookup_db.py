import zmq
import time
import json
import sys
from hashlib import md5
from gen_strings import gen_strings

context = zmq.Context()
host = 'localhost'
port = 7700


def lookup_db(originals):
    req = context.socket(zmq.REQ)
    req.connect('tcp://%s:%s' % (host, port))
    # Sleep before sending, or the first message maybe lost.
    # ref: http://lists.zeromq.org/pipermail/zeromq-dev/2013-July/022246.html
    time.sleep(1)

    for s in gen_strings(str_len):
        msg = md5(s).hexdigest() + ' {"action": "query"}'
        req.send(msg)
        print 'sending: ', msg
        print 'received: ', req.recv()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        str_len = int(sys.argv[1])
    else:
        str_len = 1
    lookup_db(gen_strings(str_len))
