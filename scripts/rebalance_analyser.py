#!/usr/bin/env python3
# @author Couchbase <info@couchbase.com>
# @copyright 2023 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import argparse
import subprocess
import sys
from jq.rebalance_report.parser import get_vbucket_moves
from jq.master_events.plot_bucket_rebalance import plot_rebalance


def main():
    args = parse_args()

    if args.report is not None:
        """
        To reuse the same code which plots the moves from the master_events.log,
        but for a rebalance report instead, we convert the report into a list of
        moves in the appropriate format, and directly pass that to
        plot_bucket_rebalance.plot_rebalance.
        """

        with open(args.report) as f:
            j = json.load(f)

        last_rebalance = get_vbucket_moves(j, args.bucket)
        if len(last_rebalance) == 0:
            print(f"No moves found for bucket '{args.bucket}'")
            return

        last_rebalance.sort(key=lambda move: move["start"])
        plot_rebalance({"moves": last_rebalance, "bucket": args.bucket})
    elif args.master_events is not None:
        """
         /jq/master_events/plot-bucket-rebalance is intended to be called in
         the following manor:

           ./read master_events.log | \
              ./last-rebalance | \
              ./plot-bucket-rebalance "bucket-name"

        We use subprocess.Popen and subprocess.PIPE to achieve the above in
        Python.
        """

        scriptdir = sys.path[0]
        master_events = subprocess.Popen(
            [scriptdir + "/jq/master_events/read", args.master_events],
            stdout=subprocess.PIPE)
        last_rebalance = subprocess.Popen(
            [scriptdir + "/jq/master_events/last-rebalance"],
            stdin=master_events.stdout,
            stdout=subprocess.PIPE)

        subprocess.run([scriptdir + "/jq/master_events/plot-bucket-rebalance",
                        args.bucket],
                       stdin=last_rebalance.stdout)


def parse_args():
    arg_parser = argparse.ArgumentParser(
        prog="./rebalance_analyser.py",
        description="Generate a plot of the vbucket moves for a bucket "
                    "rebalance from a master_events.log. If a bucket is not "
                    "specified, the last bucket that began rebalancing is used."
    )
    group = arg_parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--report', '-r',
        type=str,
        metavar="<path>",
        help=f'Rebalance report to read from')
    group.add_argument(
        '--master-events', '-m',
        type=str,
        metavar="<path>",
        help=f'Master events log read from')
    arg_parser.add_argument(
        '--bucket', '-b',
        type=str,
        default="",
        metavar="<name>",
        help=f'Bucket')

    return arg_parser.parse_args()


if __name__ == '__main__':
    main()
