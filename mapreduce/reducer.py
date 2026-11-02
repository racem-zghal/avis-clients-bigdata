#!/usr/bin/env python3
import sys

cur = None
total = 0
for line in sys.stdin:
    w, c = line.rstrip("\n").split("\t")
    if w == cur:
        total += int(c)
    else:
        if cur is not None:
            sys.stdout.write(cur + "\t" + str(total) + "\n")
        cur = w
        total = int(c)
if cur is not None:
    sys.stdout.write(cur + "\t" + str(total) + "\n")
