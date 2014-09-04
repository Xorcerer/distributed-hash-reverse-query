#!/bin/env python

import os
import sys
from hashlib import md5

from dbs.leveldb import DB as BackendDB


def log(msg):
    print msg


def extract_json(msg):
    log('msg: %s' % msg)
    md5_value, json_str = msg.split(' ', 1)
    return md5_value, json.loads(json_str)


class DB(object):

    def __init__(self, db_name):
        self.db_name = db_name

        self.db = BackendDB(db_name, create_if_missing=True)

    def lookup(self, md5):
        original = self.db.get(md5.decode('hex'))
        return original

    def query(self, md5_value):
        original = self.lookup(md5_value)
        return original

    def put(self, original):
        self.db.put(md5(original).hexdigest().decode('hex'), original)

    def close(self):
        self.db.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: %s <md5-hex>' % sys.argv[0]
        sys.exit(1)

    db = DB('md5.leveldb')
    print sys.argv[1]
    print db.query(sys.argv[1])
    db.close()
