#!/usr/bin/env python

import sys

# maps words to their counts
word2count = {}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)
    word2count[word] = word2count.get(word, 0) + int(count)

# write the results to STDOUT (standard output)
for word, count in word2count.items():
    print '%s\t%s'% (word, count)