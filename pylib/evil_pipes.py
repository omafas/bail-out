#!/usr/bin/env python3
# -*- python -*-
#
# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import argparse
import random
import string
import sys
import time

def generate_random_string(length):
    characters = string.ascii_letters + string.digits
    chars = [random.choice(characters) for _ in range(length)]
    return ''.join(chars)

# This test will focus on the core issue related to the deadlock while reading
# stderr/stdout concurrently.
def stderr_test():
    for i in range(1, 101):
        sys.stderr.write(generate_random_string(i * 1024))
        sys.stdout.write(generate_random_string(i * 1024))

# This test will produce a lot of stdout data s/t we can ensure we aren't
# buffering the entirety of it in memory. This produces ~1gb of raw data.
# NOTE: can go higher, but we will time-out on CI
def stdout_test():
    random_data = generate_random_string(1024 * 1024)
    for i in range(0, 1024):
        sys.stdout.write(random_data)

        # every so often add a newline s/t we don't hit the "pathological case"
        # where we would be forced to buffer the entire file to then turn it
        # into "chunks". There is no other way to do the redaction so this is
        # just a side-effect of that design. It should be very unlikely to
        # have stdout data that is huge AND doesn't contain any newlines.
        if i % 10 == 0:
            sys.stdout.write("\n")
            sys.stdout.flush()

# By nature of this test not hanging indefintely, we know our latest fixes
# related to MB-66860 are working.
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument("-1", "--stdout", action="store_true")
    parser.add_argument("-2", "--stderr", action="store_true")
    args = parser.parse_args()

    if args.stdout:
        stdout_test()

    if args.stderr:
        stderr_test()
