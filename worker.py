import zmq
import sys
import json
from hashlib import md5
from libs.leveldb import DB

context = zmq.Context()
sub_host = 'localhost'
sub_port = 7700

pub_host = 'localhost'
pub_port = 7701

pusher_host = 'localhost'
pusher_port = 7702

DB_NAME = 'dev.leveldb'


def lookup(md5):

    # We don't cache the db instances for there are too many.
    db = DB(DB_NAME, default_fill_cache=False)
    original = db.get(md5.decode('hex'))
    db.close()
    return original


def extract_json(msg):
    md5_value, json_str = msg.split(' ', 1)
    return md5_value, json.loads(json_str)


def query(md5, msg):
    original = lookup(md5)
    print 'md5: %s type: %s, original: %s' % (md5, msg['action'], original)
    return original


def put(md5_value, msg):
    original = msg['original']
    if md5_value != md5(original).hexdigest():
        print 'Missmatch md5: %s orignal: %s' % (md5_value, original)
        return

    db = DB(DB_NAME, default_fill_cache=False, create_if_missing=True)
    db.put(md5_value, original)
    db.close()
    print 'md5: %s type: %s, original: %s' % \
        (md5_value, msg['action'], original)


def sub(prefix, handlers):
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, prefix)
    sub.connect('tcp://%s:%s' % (pub_host, pub_port))

    while True:
        msg_buffer = sub.recv()
        md5_hex, msg = extract_json(msg_buffer)
        handler = handlers.get(msg['action'], None)
        if handler is None:
            print 'Unkown action: %s' % msg['action']
        else:
            handler(md5_hex, msg)


if __name__ == '__main__':
    prefix = sys.argv[1]
    handlers = {
        'query': query,
        'put': put,
    }

    sub(prefix, handlers)
