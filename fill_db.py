import zmq
import time
import json
from hashlib import md5
from gen_strings import gen_strings

context = zmq.Context()
sub_host = 'localhost'
sub_port = 7700


def pub(originals):
    pub = context.socket(zmq.PUB)
    #pub.setsockopt(zmq.HWM, 100)
    pub.connect('tcp://%s:%s' % (sub_host, sub_port))
    # Sleep before sending, or the first message maybe lost.
    # ref: http://lists.zeromq.org/pipermail/zeromq-dev/2013-July/022246.html
    time.sleep(1)

    for o in originals:
        req = {'action': 'put', 'original': o}
        msg = md5(o).hexdigest() + ' ' + json.dumps(req)
        pub.send(msg)
        print 'sending ', msg

if __name__ == '__main__':
    pub(gen_strings(1))
