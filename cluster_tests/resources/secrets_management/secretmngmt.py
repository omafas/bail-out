#!/usr/bin/env python3

import sys
import random
import os
import string

def main():
    if len(sys.argv) <= 3:
        print("too few args")
        exit(1)
    cmd = sys.argv[1]
    seedfile = sys.argv[2]
    secretfile = sys.argv[3]
    rand = sys.argv[4]
    arg3 = sys.argv[5] if len(sys.argv) > 5 else ""
    arg4 = sys.argv[6] if len(sys.argv) > 6 else ""

    with open(seedfile, "r") as f:
        seed = f.read()
        random.seed(seed)

    print(f'Using seed: {seed}', file=sys.stderr)

    nextseed = ''.join(random.choices(string.ascii_lowercase, k=16))
    with open(seedfile, "w") as f:
        f.write(nextseed)

    def read():
        if os.path.exists(secretfile):
            with open(secretfile, "r") as f:
                print(f.read(), end='')

    def write(s1, s2):
        with open(secretfile, "w") as f:
            f.write(f"{s1} {s2}")

    def delete():
        if os.path.exists(secretfile):
            os.remove(secretfile)

    if cmd == "write":
        write(arg3, arg4)
    elif cmd == "bad_write":
        maybe_exit(0.35)
        write(arg3, arg4)
    elif cmd == "read":
        read()
    elif cmd == "bad_read":
        maybe_exit(0.35)
        read()
    elif cmd == "delete":
        delete()
    elif cmd == "bad_delete":
        maybe_exit(0.5)
        delete()
    else:
        print(f"unknown cmd: {cmd}")
        exit(2)

    exit(0)

def maybe_exit(p):
    if random.uniform(0,1) < p:
        print('Test exit', file=sys.stderr)
        exit(4)

if __name__ == '__main__':
    main()
