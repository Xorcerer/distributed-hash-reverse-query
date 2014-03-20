import sys
import struct


def count_file_lines(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def md5_hash(md5_value):
    a, b, c, d = struct.unpack('IIII', md5_value)
    return a ^ b ^ c ^ d


class Bucket(object):
    SIZE = 24  # 16(key) + 4(value) + 4(next)

    def __init__(self, key, value, next=0):
        assert len(key) == 16

        self.key = key
        self.value = value
        self.next = next

    def to_bytes(self):
        return struct.pack('16sii', self.key, self.value, self.next)

    @staticmethod
    def from_bytes(byte_array):
        key, value, next = struct.unpack('16sII', byte_array)
        return Bucket(key, value, next)

    def __str__(self):
        return str((self.key.encode('hex'), self.value, self.next))

EMPTY_BUCKET = Bucket(struct.pack('iiii', 0, 0, 0, 0), 0)


class HashTable(object):

    def __init__(self, bucket_count):
        self.buckets = {}  # pos: offset
        self.bucket_count = bucket_count
        self.new_bucket_pos = bucket_count

    def last_bucket_in_chain(self, head_pos):
        assert head_pos in self.buckets

        bucket = None
        while head_pos in self.buckets:
            bucket = self.buckets[head_pos]
            head_pos = bucket.next

        assert bucket is not None
        return bucket

    def __setitem__(self, key, value):
        assert isinstance(key, (str, unicode))
        assert len(key) == 32
        key = key.decode('hex')

        h = md5_hash(key)
        pos = h % self.bucket_count

        if pos in self.buckets:
            self.buckets[self.new_bucket_pos] = Bucket(key, value)
            prev_bucket = self.last_bucket_in_chain(pos)

            assert prev_bucket.next == 0

            prev_bucket.next = self.new_bucket_pos
            self.new_bucket_pos += 1
        else:
            self.buckets[pos] = Bucket(key, value)

    def iterbuckets(self, include_empty=True):
        for i in range(self.new_bucket_pos):
            bucket = self.buckets.get(i, EMPTY_BUCKET)
            yield bucket


def index_file(filename):
    bucket_count = count_file_lines(filename)
    table = HashTable(bucket_count)

    with open(filename, 'rb') as f:
        pos = 0
        for l in f:
            original_value, md5_value = l.rstrip('\n').rsplit(' ', 1)
            table[md5_value] = pos

            pos += len(l)
    return table


if __name__ == '__main__':
    content_filename = sys.argv[1]
    index_filename = sys.argv[2]

    hash_table = index_file(content_filename)
    with open(index_filename, 'wb') as f:
        f.write(struct.pack('i', hash_table.bucket_count))
        for b in hash_table.iterbuckets():
            f.write(b.to_bytes())
