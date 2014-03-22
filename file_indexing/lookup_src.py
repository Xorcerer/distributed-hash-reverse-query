import sys
import struct
from make_index import md5_hash, Bucket

INT_SIZE = 4


def lookup(md5_value, index_filename):
    raw_md5 = md5_value.decode('hex')
    h = md5_hash(raw_md5)

    with open(index_filename, 'rb') as f:
        bucket_count = struct.unpack('I', f.read(INT_SIZE))[0]

        pos = Bucket.SIZE * (h % bucket_count) + INT_SIZE
        f.seek(pos)
        b = Bucket.from_bytes(f.read(Bucket.SIZE))
        print b
        while b.key != raw_md5:
            if b.next != 0:
                pos = Bucket.SIZE * b.next + INT_SIZE
                f.seek(pos)
                b = Bucket.from_bytes(f.read(Bucket.SIZE))
                print b
            else:
                return None

        return b.value


def main():
    index_filename = sys.argv[1]
    src_filename = sys.argv[2]
    md5_value = sys.argv[3]

    pos = lookup(md5_value, index_filename)
    if pos is None:
        print 'Not found.'
        return

    with open(src_filename, 'rb') as f:
        f.seek(pos)
        l = f.readline().rstrip('\n')
        assert l.endswith(md5_value)
        print l


if __name__ == '__main__':
    main()
