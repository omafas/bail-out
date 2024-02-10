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
"""Plot pressure stats on 7.2.x or earlier - if they are not already reported
to Prometheus (but are recorded in ns_server.stats.log).

./pressure_from_stats.py -l <path_to_ns_server.stats.log> -s '2023-06-20 18:00'
                         -e '2023-06-20 18:50' -n 'cen-s710.perf.couchbase.com'

You may want to specify each node's stats log to plot its own pressure data (to
minimize lag)."""

import argparse
from collections import defaultdict
from datetime import datetime
import re
import sys

import pandas as pd
import matplotlib.pyplot as plt

class PsiStatsParser:
    def __init__(self, args):
        if not self.contains_psi_stats(args.log_file):
            print(f"{args.log_file} does not contain pressure stats.")
            sys.exit(1)
        self.log_file = args.log_file
        self.node = args.node
        self.resource = args.resource
        self.ts_start = args.ts_start
        self.ts_end = args.ts_end
        self.list_entries = []

    def contains_psi_stats(self, log_file):
        with open(log_file,"r",encoding="utf8") as file:
            lines = file.readlines()
            for i, line in enumerate(lines):
                if "cpu_pressure" in line:
                    match = re.search(r'<<"some avg10=([0-9]+.[0-9]+) '
                                  'avg60=([0-9]+.[0-9]+) '
                                  'avg300=([0-9]+.[0-9]+) total=([0-9]+)',
                                  lines[i+1])
                    if not match:
                        return False
                    return True

        return False


    def parse_psi_stats(self):
        """
        Parse pressure stats from per node stats. Every node sends an update,
        which may contain pressure stats:
        {cpu_pressure,
            <<"some avg10=0.00 avg60=0.00 avg300=0.05 total=629242\n">>},
        {memory_pressure,
            <<"some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00
               avg60=0.00 avg300=0.00 total=0\n">>},
        {io_pressure,
            <<"some avg10=9.01 avg60=11.46 avg300=5.42 total=22559352\nfull
               avg10=9.01 avg60=11.44 avg300=5.39 total=22357587\n">>}
        """
        heard = defaultdict(lambda: "")
        stale=False

        with open(self.log_file,"r",encoding="utf8") as file:
            lines = file.readlines()
            for i, line in enumerate(lines):
                if "ns_doctor:debug," in line:
                    # Parse timestamp:
                    # ...ns_doctor:debug,2023-07-03T19:10:53...
                    match = re.search(r'ns_doctor:debug,([0-9]+-[0-9]+-[0-9]+T'
                                      '[0-9]+:[0-9]+:[0-9]+)', line)
                    index = datetime.strptime(match.group(1),
                                              '%Y-%m-%dT%H:%M:%S')

                elif "{'ns_1@" in line:
                    # Parse node name:
                    # '{ns_1@svc-d-node-001.9cbdee2pnijuve2.cloud.couchbase.com'
                    match = re.search(r"{'ns_1@([\S]+)'", line)
                    node = match.group(1)

                elif "last_heard" in line:
                    # Parse last_heard to determine if its a stale update:
                    # ...last_heard,-573178520153947910...
                    match = re.search(r'{last_heard,(-{0,1}[0-9]+)', line)
                    last_heard = match.group(1)
                    if heard[node] == last_heard:
                        stale = True
                    else:
                        heard[node] = last_heard
                        stale = False

                elif "_pressure" in line and node != '' and not stale:
                    match = re.search(r'{([\w]+)_pressure', line)
                    ptype = match.group(1)
                    row = {'node': node, 'type': ptype, 'ts': index}

                    match = re.search(r'<<"some avg10=([0-9]+.[0-9]+) avg60='
                                      '([0-9]+.[0-9]+) avg300=([0-9]+.[0-9]+) '
                                      'total=([0-9]+)', lines[i+1])
                    row['some_avg10'] = float(match.group(1))
                    row['some_avg60'] = float(match.group(2))
                    row['some_avg300'] = float(match.group(3))
                    row['some_total'] = int(match.group(4))

                    match = re.search(r'full avg10=([0-9]+.[0-9]+) avg60='
                                      '([0-9]+.[0-9]+) avg300=([0-9]+.[0-9]+) '
                                      'total=([0-9]+)', lines[i+1])
                    # cpu_pressure may not contain "full" stats.
                    if match:
                        row['full_avg10'] = float(match.group(1))
                        row['full_avg60'] = float(match.group(2))
                        row['full_avg300'] = float(match.group(3))
                        row['full_total'] = int(match.group(4))

                    self.list_entries.append(row)

    def get_vars_to_plot(self, frame, var, diff):
        vars_to_plot = []

        for quant in ['some_', 'full_']:
            # cpu data for "full" is absent (all values are NaN).
            if not frame[quant + var].isnull().all():
                var_name = quant + var

                if diff:
                    # Compute delta between adjacent rows. Must be a frame with
                    # data for a single node-resource combination. var must be
                    # of counter type.
                    delta = frame[var_name].diff()
                    var_name = var_name + "_diff"

                    # total counter values may reset if the node was rebooted -
                    # the diff will be negative at those instants. Reset to 0.
                    frame[var_name] = delta.where(delta.ge(0), 0)

                    # Convert from μs to ms.
                    assert var == 'total'
                    frame[var_name] = frame[var_name].div(1000)

                vars_to_plot.append(var_name)

        return vars_to_plot

    def plot_var(self, raw_df, var, desc, diff):
        for resource, df_by_resource in raw_df.groupby('type'):
            for node, df_by_resource_and_node in df_by_resource.groupby('node'):
                vars_to_plot = self.get_vars_to_plot(df_by_resource_and_node,
                                                     var, diff)
                df_by_resource_and_node.plot(kind='line', y=vars_to_plot,
                                    title=f'{node} {resource} pressure: {desc}')

                plt.minorticks_on()
                plt.grid(which='both',axis='both',color='0.95',
                         linestyle='dotted')
                plt.show()

    def plot_psi_stats(self):
        data = pd.DataFrame(self.list_entries)

        masks = []
        if self.node:
            masks.append(data['node'] == self.node)
        if self.resource:
            masks.append(data['type'] == self.resource)
        if self.ts_start:
            masks.append(data['ts'] >= self.ts_start)
        if self.ts_end:
            masks.append(data['ts'] <= self.ts_end)
        if masks:
            final_mask = masks[0]
            for mask in masks[1:]:
                final_mask = final_mask & mask

            data = data.loc[final_mask]

        data = data.set_index('ts')
        data.index = data.index.format(formatter=lambda x: x.strftime('%H:%M'))

        self.plot_var(data, 'avg60', "% of time stalled", False)
        self.plot_var(data, 'total', "diff in time stalled (ms)", True)

def main():
    parser = argparse.ArgumentParser(
        description='Parse pressure stats from ns_server.stats.log')
    parser.add_argument('-l', '--log', dest='log_file',
                        help='path to ns_server.stats.log file',
                        required=True)
    parser.add_argument('-n', '--node', dest='node',
                        help='node name to filter '
                        '(e.g. "cen-s709.perf.couchbase.com")')
    parser.add_argument('-r', '--resource', dest='resource',
                        help='resource type to filter '
                        '(one of "cpu", "memory" or "io")',
                        required=False)
    parser.add_argument('-s', '--start', dest='ts_start',
                        help='start time to filter (e.g. "2023-06-20 18:00")',
                        required=False)
    parser.add_argument('-e', '--end', dest='ts_end',
                        help='end time to filter (e.g. "2023-06-20 18:30")',
                        required=False)

    args = parser.parse_args()
    parser = PsiStatsParser(args)
    parser.parse_psi_stats()
    parser.plot_psi_stats()

if __name__ == '__main__':
    main()
