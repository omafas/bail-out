#!/usr/bin/env python3
#
# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This tool is used to validate the stats returned by /_prometheusMetrics
# and /_prometheusMetricHigh have entries in the stats description file. Any
# missing ones need to be added to the description file.
#
# It also checks that a stat is returned for each stats description entry.
# Ones missing may not necessarily indicate an issue as the workload may
# not be hitting the paths which generate the stat.

# python3 validate_stats.py -c localhost:9000 -u Administrator \
#        -p asdasd -d <file>

import argparse
import requests
import json


class StatsValidator:

    def __init__(self, args):
        self.cluster = args.cluster
        self.user = args.user
        self.password = args.password
        self.descriptors = args.descriptors
        self.unknown_stats = []

        with open(self.descriptors, 'r') as file:
            self.json_data = json.load(file)

        print(f"Found {len(self.json_data)} items in {self.descriptors}")

    def finalize(self):
        # Print stats without entries in the description file. These need
        # to be added to the file.
        if self.unknown_stats:
            print("\nStats returned by endpoints with no entry in the "
                  "descriptions file. This is a bug!\n")
            for item in self.unknown_stats:
                print(f"{item}")

        # Print stats present in the description file but not returned
        # in the stats endpoints results. This may be ok as the workload
        # may not be generating the stats.
        first = True
        for item in self.json_data:
            if not "found" in self.json_data[item]:
                if first:
                    print("\nStats in description file but not returned "
                          "by the REST endpoints. This isn't necessarily "
                          "a bug as the workload may not lead to the stat "
                          "being emitted or the stat may not be emitted on "
                          "all architectures.\n")
                    first = False
                if "deprecated" in self.json_data[item]:
                    deprecated = f"(deprecated in {self.json_data[item]['deprecated']})"
                else:
                    deprecated = ""
                print(f"{item} {deprecated}")

    def known_stat(self, stat_name):
        if stat_name in self.json_data:
            if not "found" in self.json_data[stat_name]:
                self.json_data[stat_name] = "found"
            return True

        return False

    # Stats may have suffixes appended which aren't included in the
    # description file (e.g. histograms, open metrics naming, etc.)
    def check_base_stat_name(self, stat_name):
        suffixes = ["_bucket", "_count", "_sum",
                    "_total", "_seconds", "_bytes"]

        for suffix in suffixes:
            if stat_name.endswith(suffix):
                if self.known_stat(stat_name[:-len(suffix)]):
                    return True

        return False

    def track_unknown(self, stat_name):
        if not stat_name in self.unknown_stats:
            self.unknown_stats.append(stat_name)

    def make_url(self, rest_endpoint):
        url = self.cluster
        if not url.startswith('http://'):
            url = 'http://' + url
        if not url.endswith('/'):
            url += '/'
        url = url + rest_endpoint
        return url

    def validate_stats(self, api):
        s = requests.Session()
        u = self.make_url(api)

        print(f"Starting to validate '{u}'")

        with s.get(url=u, headers=None,
                   auth=(self.user, self.password)) as resp:
            count = 0
            for raw_line in resp.iter_lines():
                if raw_line:
                    line = raw_line.decode("utf-8").strip()
                    if not line.startswith("#"):
                        # This should handle:
                        #    statABC{} 34
                        #    statDEF 45
                        #    statGHI {} 77
                        if " " in line:
                            line = line.split(" ", 1)[0]
                        if "{" in line:
                            stat_name = line.split("{", 1)[0]
                        else:
                            stat_name = line
                        if not self.known_stat(stat_name):
                            if not self.check_base_stat_name(stat_name):
                                self.track_unknown(stat_name)
                        count += 1
            print(f"Processed {count} stats from rest endpoint")


def main():
    parser = argparse.ArgumentParser(
        description='Validate stats from REST APIs')
    parser.add_argument('-c', '--cluster', dest='cluster', help='cluster',
                        required=True)
    parser.add_argument('-u', '--user', dest='user',
                        help='user', required=True)
    parser.add_argument('-p', '--password', dest='password', help='password',
                        required=True)
    parser.add_argument('-d', '--descriptors', dest='descriptors',
                        help='Name of file containing stats descriptions',
                        required=True)

    args = parser.parse_args()

    monitor = StatsValidator(args)
    monitor.validate_stats("_prometheusMetrics")
    monitor.validate_stats("_prometheusMetricsHigh")
    monitor.finalize()


if __name__ == '__main__':
    main()
