#!/usr/bin/env python

import zmq
import argparse

# zeromq 3.x compatible.
# Ref: http://www.zeromq.org/docs:3-1-upgrade
if hasattr(zmq, 'SNDHWM'):
    zmq.HWM = zmq.SNDHWM

context = zmq.Context()


def log(msg):
    pass


def create_subscriber(port):
    sub = context.socket(zmq.SUB)
    sub.bind('tcp://*:%s' % port)
    sub.setsockopt(zmq.SUBSCRIBE, '')
    return sub


def create_publisher(port):
    pub = context.socket(zmq.PUB)
    pub.bind('tcp://*:%s' % port)
    pub.setsockopt(zmq.HWM, 0)
    return pub


def create_pusher(port):
    pusher = context.socket(zmq.PUSH)
    pusher.bind('tcp://*:%s' % port)
    return pusher


def get_args():
    parser = argparse.ArgumentParser(
        description='Terminus Message Queue Server')
    parser.add_argument('--sub-port', action='store', type=int, default='7700',
                        help='Port for subscriber to listen.')
    parser.add_argument('--pub-port', action='store', type=int, default='7701',
                        help='Port for publisher to listen.')
    parser.add_argument('--push-port', action='store', type=int,
                        default='7702', help='Port for pusher to listen.')
    parser.add_argument('-v', '--verbose', action='store_const', const=True,
                        help='Verbose output.')

    return parser.parse_args()


def main():
    args = get_args()

    if args.verbose:
        global log

        def log(msg):
            print msg

    sub = create_subscriber(args.sub_port)
    pub = create_publisher(args.pub_port)
    pusher = create_pusher(args.push_port)

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    while True:
        socks = poller.poll()
        for k, v in socks:
            message = k.recv()
            pub.send(message)
            # FIXME: Use gevent instead.
            try:
                pusher.send(message.split(' ', 1)[-1], zmq.NOBLOCK)
            except:
                pass
            log(message)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'End.'
