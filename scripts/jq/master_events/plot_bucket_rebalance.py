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


def plot_rebalance(payload):
    bucket = payload['bucket']
    all_moves = payload['moves']
    if len(all_moves) == 0:
        if bucket == "":
            print("No vbucket moves found")
        else:
            print(f"No vbucket moves found for bucket '{bucket}'")
        return

    vbuckets = []
    active_moves = []
    replica_moves = []
    active_backfills = []
    replica_backfills = []
    end = 0

    for i, move in enumerate(all_moves):
        vbucket = move['vbucket']
        x = move['start']
        width = move['duration']
        if width is not None:
            end = max(end, x + width)
        backfill_width = move['backfillDuration']
        if backfill_width is not None:
            end = max(end, x + backfill_width)

        vbuckets.append(vbucket)
        move_tuple = (i, x, width)
        if move['type'] == 'active':
            active_moves.append(move_tuple)
            active_backfills.append((i, x, backfill_width))
        else:
            replica_moves.append(move_tuple)
            replica_backfills.append((i, x, backfill_width))

    all_move_groups = [active_moves,
                       replica_moves,
                       active_backfills,
                       replica_backfills]
    in_progress_moves = []

    for moves in all_move_groups:
        for i, move in enumerate(moves):
            if move[2] is None:
                moves[i] = (move[0], move[1], end - move[1])
                in_progress_moves.append(moves[i])


    plot.rcdefaults()
    fig, ax = plot.subplots()

    charts = [(active_moves, 'active moves', {'color': 'green'}),
              (replica_moves, 'replica moves', {'color': 'orange'}),
              (active_backfills, 'active backfill', {'color': '#5a5'}),
              (replica_backfills, 'replica backfill', {'color': '#fda'}),
              (in_progress_moves, "in-progress moves",
               {'fill': False, "edgecolor": 'red'})]
    for data, label, style in charts:
        if len(data) > 0:
            pos, lefts, widths = zip(*data)
            ax.barh(pos, left=lefts, width=widths, label=label, **style)

    ax.set_yticks(range(len(vbuckets)))
    ax.set_yticklabels(vbuckets)


    def format_y_coord(y):
        return vbuckets[min(len(vbuckets) - 1, max(0, round(y)))]


    ax.fmt_ydata = format_y_coord
    ax.yaxis.set_major_locator(plot.MaxNLocator(31, integer=True,
                                                min_n_ticks=1))

    ax.invert_yaxis()
    ax.set_ylabel('VBucket')
    ax.set_xlabel('Time (s)')
    ax.set_title(bucket)

    ax.legend()
    plot.show()


if __name__ == '__main__':
    payload = json.load(sys.stdin)
    plot_rebalance(payload)
