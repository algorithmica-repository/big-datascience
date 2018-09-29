#!/usr/bin/env python

import sys

current_word = None
current_count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)
    # convert count (currently a string) to int
    if current_word:
        if word == current_word:
            current_count += int(count)
        else:
            print "%s\t%d" % (current_word, current_count)
            current_count = 1

    current_word = word

if current_count > 1:
    print "%s\t%d" % (current_word, current_count)