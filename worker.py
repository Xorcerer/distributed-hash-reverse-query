import zmq
import sys
import json
from hashlib import md5
from libs.leveldb import DB


def lookup(md5, db_name):

    # We don't cache the db instances for there are too many.
    db = DB(db_name, default_fill_cache=False)
    original = db.get(md5.decode('hex'))
    db.close()
    return original


def extract_json(msg):
    print 'msg:', msg
    md5_value, json_str = msg.split(' ', 1)
    return md5_value, json.loads(json_str)


def query(md5, msg, db_name):
    original = lookup(md5)
    print 'md5: %s type: %s, original: %s' % (md5, msg['action'], original)
    return original


def put(md5_value, msg, db_name):
    original = msg['original']
    if md5_value != md5(original).hexdigest():
        print 'Missmatch md5: %s orignal: %s' % (md5_value, original)
        return

    db = DB(db_name, default_fill_cache=False, create_if_missing=True)
    db.put(md5_value, original)
    db.close()
    print 'md5: %s type: %s, original: %s' % \
        (md5_value, msg['action'], original)


def sub(prefix, handlers, db_name, mq_host, mq_port):
    context = zmq.Context()
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, prefix)
    sub.connect('tcp://%s:%s' % (mq_host, int(mq_port)))

    while True:
        msg_buffer = sub.recv()
        md5_hex, msg = extract_json(msg_buffer)
        handler = handlers.get(msg['action'], None)
        if handler is None:
            print 'Unkown action: %s' % msg['action']
        else:
            handler(md5_hex, msg, db_name)


if __name__ == '__main__':
    prefix, db_name, mq_host, mq_port = sys.argv[1:]
    print 'prefix, db_name, mq_host, mq_port = ', prefix, db_name, mq_host, mq_port

    handlers = {
        'query': query,
        'put': put,
    }

    sub(prefix, handlers, db_name, mq_host, mq_port)
