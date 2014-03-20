import zmq
import time
import sys
from hashlib import md5


context = zmq.Context()
sub_host = 'localhost'
sub_port = 7700

pub_host = 'localhost'
pub_port = 7701

pusher_host = 'localhost'
pusher_port = 7702


def pub():
    pub = context.socket(zmq.PUB)
    #pub.setsockopt(zmq.HWM, 100)
    pub.connect('tcp://%s:%s' % (sub_host, sub_port))

    while True:
        # Sleep before sending, or the first message maybe lost.
        # ref: http://lists.zeromq.org/pipermail/zeromq-dev/2013-July/022246.html
        time.sleep(1)
        msg = md5('abcd').hexdigest() + '{"type": "query"}'
        pub.send(msg)
        print 'sending', msg


def sub():
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, '')
    sub.connect('tcp://%s:%s' % (pub_host, pub_port))

    while True:
        msg = sub.recv()
        print 'Got:', msg


def pull():
    pull = context.socket(zmq.PULL)
    pull.connect('tcp://%s:%s' % (pusher_host, pusher_port))

    while True:
        msg = pull.recv()
        print 'Got: ', msg

if __name__ == '__main__':
    if 'sub' in sys.argv:
        sub()
    elif 'pull' in sys.argv:
        pull()
    else:
        pub()
