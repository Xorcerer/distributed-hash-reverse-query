from hashlib import md5
import sys

input_file = open(sys.argv[1], 'r')


for l in input_file:
    print l.strip('\n'), md5(l).hexdigest()
