import sys
from hashlib import md5
from libs.leveldb import DB
from gen_strings import gen_strings

DB_NAME = 'perf_test.leveldb'


def perf_write():
    db = DB(DB_NAME, create_if_missing=True)

    for s in gen_strings(4):
        db.put(md5(s).digest(), s)

    db.close()


def perf_read():
    db = DB(DB_NAME, default_fill_cache=False)
    for s in gen_strings(4):
        db.get(md5(s).digest())

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: %s [writing] [reading]' % sys.argv[0]
        sys.exit(1)

    if 'writing' in sys.argv:
        perf_write()

    if 'reading' in sys.argv:
        perf_read()
