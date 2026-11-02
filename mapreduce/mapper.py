#!/usr/bin/env python3
import sys

STOP = set("the and for are but not you all was this that with have had has from they its out one our can get got them were what when will more than just also very would there their been which about into your like these it's don't i've i'm that's they're".split())

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 3:
        continue
    for w in parts[2].split():
        if len(w) > 2 and w not in STOP:
            sys.stdout.write(w + "\t1\n")
