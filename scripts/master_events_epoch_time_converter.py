#!/usr/bin/env python3

#   Copyright 2023-Present Couchbase, Inc.
#
#   Use of this software is governed by the Business Source License included
#   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
#   in that file, in accordance with the Business Source License, use of this
#   software will be governed by the Apache License, Version 2.0, included in
#   the file licenses/APL2.txt.
#

"""
Currently the master_events.log uses UNIX/epoch time, which isn't very readable.
This script adds "time" field which is a human-readable date/time format.
e.g. 1675955945.324646 -> 2023/02/09T15:19:05.324646+00:00
"""

import datetime
import json
import argparse
import os

SECONDS_IN_ONE_HOUR = 3600


def arg_parse():
    parser = argparse.ArgumentParser(
        description="Adds a human readable time to master_events.log")
    parser.add_argument(
        '--in', '-i',
        dest='infile',
        required=True,
        help='input file',
        metavar='')
    parser.add_argument(
        '--out', '-o',
        dest='outfile',
        help='output file',
        metavar='')
    parser.add_argument(
        '--utc-offset', '-t',
        dest='timezone',
        help='number of hours to shift by. e.g. 4.5 is equivalent to +04:30',
        default=0,
        type=float,
        metavar='')
    return parser.parse_args()


def timezone_to_string(tz: float):
    """
    :param tz: The time zone as a float.
    :return: a string which represents the timezone
        examples: 6 -> '+06:00'
                  -3.5 -> '-03:30'
    """
    prefix = f"+"
    if tz < 0:
        tz *= -1
        prefix = f"-"
    return f"{prefix}{tz // 1:02.0f}:{60 * (tz % 1):02.0f}"


def main():
    """ Adds a field "time" which converts "ts" to a human-readable time"""
    args = arg_parse()

    timezone_in_seconds = args.timezone * SECONDS_IN_ONE_HOUR
    timezone_string = timezone_to_string(args.timezone)

    outfile = args.outfile
    infile_path = os.path.dirname(args.infile)
    if args.outfile is None:
        outfile = f"{infile_path}/master_events_with_time.log"
    with open(args.infile, "r") as file:
        with open(outfile, "w") as out:
            for current_line in file:
                j = json.loads(current_line)
                time_unix = j["ts"]
                human_readable_time = datetime.datetime \
                    .fromtimestamp(time_unix + timezone_in_seconds) \
                    .strftime("%Y-%m-%dT%H:%M:%S.%f")
                # time field is formatted as YYYY/MM/DDThh:mm:ss.ms+\d\d:\d\d
                j["time"] = f"{human_readable_time}{timezone_string}"
                out.write(f"{json.dumps(j)}\n")

    # If the user doesn't specify an outfile, just replace the old
    # master_events.log
    if args.outfile is None:
        os.remove(args.infile)
        os.rename(outfile, args.infile)


if __name__ == "__main__":
    main()
