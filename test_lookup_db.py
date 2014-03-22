import zmq
import time
from hashlib import md5


context = zmq.Context()
sub_host = 'localhost'
sub_port = 7700


def pub():
    pub = context.socket(zmq.PUB)
    #pub.setsockopt(zmq.HWM, 100)
    pub.connect('tcp://%s:%s' % (sub_host, sub_port))
    # Sleep before sending, or the first message maybe lost.
    # ref: http://lists.zeromq.org/pipermail/zeromq-dev/2013-July/022246.html
    time.sleep(1)

    msg = md5('9').hexdigest() + ' {"action": "query"}'
    print 'sending', msg
    pub.send(msg)


if __name__ == '__main__':
    pub()
