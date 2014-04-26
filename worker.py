import zmq
import sys
import json
import time
from hashlib import md5
from dbs import KyotoCabinetDB as BackendDB


def log(msg):
    print msg


def extract_json(msg):
    log('msg: %s' % msg)
    md5_value, json_str = msg.split(' ', 1)
    return md5_value, json.loads(json_str)


class DB(object):

    def __init__(self, prefix, db_name, mq_host, mq_port):
        self.prefix = prefix
        self.db_name = db_name
        self.mq_host = mq_host
        self.mq_port = mq_port

        self.db = BackendDB(db_name)

    def lookup(self, md5):
        original = self.db.get(md5.decode('hex'))
        return original

    def query(self, md5_value, msg):
        original = self.lookup(md5_value)
        print 'md5: %s action: %s, original: %s' % \
            (md5_value, msg['action'], original)
        return original

    def put(self, md5_value, msg):
        original = msg['original']
        if md5_value != md5(original).hexdigest():
            print 'Missmatch md5: %s orignal: %s' % (md5_value, original)
            return

        self.db.put(md5_value.decode('hex'), original)
        print 'md5: %s type: %s, original: %s, time: %s' % \
            (md5_value, msg['action'], original, time.time())

    def close(self):
        self.db.close()


def serve(db):
    context = zmq.Context()
    sock = context.socket(zmq.REP)
    sock.bind('tcp://%s:%s' % (mq_host, int(mq_port)))

    handlers = {
        'query': db.query,
        'put': db.put,
    }

    while True:
        msg_buffer = sock.recv()
        md5_hex, msg = extract_json(msg_buffer)
        handler = handlers.get(msg['action'], None)
        if handler is None:
            log('Unkown action: %s' % msg['action'])
        else:
            result = handler(md5_hex, msg)
            reply = {'action': msg['action'], 'result': str(result)}
            sock.send(json.dumps(reply))


if __name__ == '__main__':
    prefix, db_name, mq_host, mq_port = sys.argv[1:]
    print 'prefix, db_name, mq_host, mq_port = ', \
        prefix, db_name, mq_host, mq_port

    db = DB(prefix, db_name, mq_host, mq_port)
    try:
        serve(db)
    finally:
        db.close()
