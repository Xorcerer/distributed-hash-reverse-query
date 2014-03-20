import sys

candidates = ([chr(x) for x in xrange(ord('a'), ord('z') + 1)] +
              [chr(x) for x in xrange(ord('A'), ord('Z') + 1)] +
              [chr(x) for x in xrange(ord('0'), ord('9') + 1)])


def gen_strings(len_remained, prefix=''):
    if len_remained == 0:
        yield prefix
        return

    for c in candidates:
        for s in gen_strings(len_remained - 1, prefix + c):
            yield s

if __name__ == '_main__':
    MAX_LENGTH = int(sys.argv[1])

    for s in gen_strings(MAX_LENGTH):
        print s
