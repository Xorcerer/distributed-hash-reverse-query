import zmq
import sys
import json
from libs.leveldb import DB

context = zmq.Context()
sub_host = 'localhost'
sub_port = 7700

pub_host = 'localhost'
pub_port = 7701

pusher_host = 'localhost'
pusher_port = 7702

MD5_LEN = 32

DB_NAME = 'test.leveldb'


def lookup(md5):
    db = DB(DB_NAME, default_fill_cache=False)
    original = db.get(md5.decode('hex'))
    db.close()
    return original


def extract_json(msg):
    return msg[:MD5_LEN], json.loads(msg[MD5_LEN:])


def sub(prefix):
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, prefix)
    sub.connect('tcp://%s:%s' % (pub_host, pub_port))

    while True:
        msg_buffer = sub.recv()
        md5, msg = extract_json(msg_buffer)
        type_ = msg['type']
        if type_ == 'query':
            print 'md5: %s type: %s original: %s' % (md5, type_, lookup(md5))
        else:
            print 'Unkown type: %s' % type_


if __name__ == '__main__':
    prefix = sys.argv[1]
    sub(prefix)
