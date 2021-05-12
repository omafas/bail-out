#!/usr/bin/env python3

"""
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

import json
import sys
import matplotlib.pyplot as plot

payload = json.load(sys.stdin)

bucket = payload['bucket']
vbuckets = []
active_moves = []
replica_moves = []
backfills = []

for i, move in enumerate(payload['moves']):
    vbucket = move['vbucket']
    x = move['start']
    width = move['duration']
    backfill_width = move['backfillDuration']

    vbuckets.append(vbucket)
    move_tuple = (i, x, width)
    if move['type'] == 'active':
        active_moves.append(move_tuple)
    else:
        replica_moves.append(move_tuple)

    backfills.append((i, x, backfill_width))

plot.rcdefaults()
fig, ax = plot.subplots()

charts = [(active_moves, 'active moves', {'color': 'green'}),
          (replica_moves, 'replica moves', {'color': 'orange'}),
          (backfills, 'backfill phase', {'color': 'white', 'alpha': 0.5})]
for data, label, style in charts:
    if len(data) > 0:
        pos, lefts, widths = zip(*data)
        ax.barh(pos, left=lefts, width=widths, label=label, **style)

ax.set_yticks(range(len(vbuckets)))
ax.set_yticklabels(vbuckets)
ax.invert_yaxis()
ax.set_ylabel('VBucket')
ax.set_xlabel('Time')
ax.set_title(bucket)

ax.legend()
plot.show()
