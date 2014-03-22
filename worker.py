import zmq
import sys
import json
import time
from hashlib import md5
from libs.leveldb import DB


def extract_json(msg):
    print 'msg:', msg
    md5_value, json_str = msg.split(' ', 1)
    return md5_value, json.loads(json_str)


class HashDB(object):

    def __init__(self, prefix, db_name, mq_host, mq_port):
        self.prefix = prefix
        self.db_name = db_name
        self.mq_host = mq_host
        self.mq_port = mq_port

        self.db = DB(db_name, default_fill_cache=False, create_if_missing=True)

    def lookup(self, md5):
        original = self.db.get(md5.decode('hex'))
        return original

    def query(self, md5, msg):
        original = self.lookup(md5)
        print 'md5: %s action: %s, original: %s' % (md5, msg['action'], original)
        return original

    def put(self, md5_value, msg):
        original = msg['original']
        if md5_value != md5(original).hexdigest():
            print 'Missmatch md5: %s orignal: %s' % (md5_value, original)
            return

        self.db.put(md5_value.decode('hex'), original)
        print 'md5: %s type: %s, original: %s, time: %s' % \
            (md5_value, msg['action'], original, time.time())

    def sub(self):
        context = zmq.Context()
        sub = context.socket(zmq.SUB)
        sub.setsockopt(zmq.SUBSCRIBE, prefix)
        sub.connect('tcp://%s:%s' % (mq_host, int(mq_port)))

        handlers = {
            'query': self.query,
            'put': self.put,
        }

        while True:
            msg_buffer = sub.recv()
            md5_hex, msg = extract_json(msg_buffer)
            handler = handlers.get(msg['action'], None)
            if handler is None:
                print 'Unkown action: %s' % msg['action']
            else:
                handler(md5_hex, msg)

    def close(self):
        self.db.close()

if __name__ == '__main__':
    prefix, db_name, mq_host, mq_port = sys.argv[1:]
    print 'prefix, db_name, mq_host, mq_port = ', \
        prefix, db_name, mq_host, mq_port

    hashdb = HashDB(prefix, db_name, mq_host, mq_port)
    try:
        hashdb.sub()
    except:
        hashdb.close()
