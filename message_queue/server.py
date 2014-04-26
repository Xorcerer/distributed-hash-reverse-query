#!/usr/bin/env python

import zmq
import argparse

# zeromq 3.x compatible.
# Ref: http://www.zeromq.org/docs:3-1-upgrade
if hasattr(zmq, 'SNDHWM'):
    zmq.HWM = zmq.SNDHWM

context = zmq.Context()


class MessageLogger(object):

    def __init__(self):
        self.message_count = 0

    def __call__(self, msg):
        self.message_count += 1
        print msg, self.message_count


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
    # pub.setsockopt(zmq.HWM, 0)
    return pub


def get_args():
    parser = argparse.ArgumentParser(
        description='Terminus Message Queue Server')
    parser.add_argument('--sub-port', action='store', type=int, default='7700',
                        help='Port for subscriber to listen.')
    parser.add_argument('--pub-port', action='store', type=int, default='7701',
                        help='Port for publisher to listen.')
    parser.add_argument('-v', '--verbose', action='store_const', const=True,
                        help='Verbose output.')

    return parser.parse_args()


def main():
    args = get_args()

    if args.verbose:
        global log
        log = MessageLogger()

    sub = create_subscriber(args.sub_port)
    pub = create_publisher(args.pub_port)

    log('pub HWM: %d' % pub.getsockopt(zmq.HWM))

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    while True:
        socks = poller.poll()
        for k, v in socks:
            message = k.recv()
            pub.send(message)
            log(message)

if __name__ == '__main__':
    main()
