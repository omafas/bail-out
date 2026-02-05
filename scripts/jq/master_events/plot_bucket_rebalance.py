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


def plot_rebalance(payload, detailed=False):
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
    persistences = []
    takeovers = []
    snapshot_downloads = []
    snapshot_deks_imports = []
    snapshot_waitings = []
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

        # these 'detail' events are plotted based on the start of their own
        # event rather than the move as a whole as they don't necessarily
        # start as soon as something else ends.
        persistence_width = move['persistenceDuration']
        if persistence_width is not None:
            end = max(end, x + persistence_width)
            start = move['persistenceStart']
            persistences.append((i, start, persistence_width))
        takeover_width = move['takeoverDuration']
        if takeover_width is not None:
            end = max(end, x + takeover_width)
            start = move['takeoverStart']
            takeovers.append((i, start, takeover_width))

        snapshot_download_width = move['snapshotDownloadDuration']
        if snapshot_download_width is not None:
            end = max(end, x + snapshot_download_width)
            start = move['snapshotDownloadStart']
            snapshot_downloads.append((i, start, snapshot_download_width))
        snapshot_deks_import_width = move['snapshotDeksImportDuration']
        if snapshot_deks_import_width is not None:
            end = max(end, x + snapshot_deks_import_width)
            start = move['snapshotDeksImportStart']
            snapshot_deks_imports.append((i, start, snapshot_deks_import_width))
        snapshot_waiting_width = move['snapshotWaitingDuration']
        if snapshot_waiting_width is not None:
            end = max(end, x + snapshot_waiting_width)
            start = move['snapshotWaitingStart']
            snapshot_waitings.append((i, start, snapshot_waiting_width))

    all_move_groups = [active_moves,
                       replica_moves,
                       active_backfills,
                       replica_backfills,
                       persistences,
                       takeovers,
                       snapshot_downloads,
                       snapshot_deks_imports,
                       snapshot_waitings]
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

    if detailed:
        detailed_charts =\
            [(persistences, 'persistence', {'color': 'tab:purple'}),
             (takeovers, 'takeover', {'color': 'tab:olive'}),
             (snapshot_downloads, 'snapshot download', {'color': 'tab:blue'}),
             (snapshot_deks_imports, 'snapshot deks import',
              {'color': 'tab:red'}),
             (snapshot_waitings, 'snapshot waiting', {'color': 'tab:pink'})]
        for data, label, style in detailed_charts:
            if len(data) > 0:
                pos, lefts, widths = zip(*data)
                # For these charts we will set the height to half the default
                # height of 0.8 and align the bars to the bottom of the plot for
                # this move such that we can still see the rest of the plot
                # behind this detail event.
                ax.barh(pos, left=lefts, width=widths, label=label, **style,
                        height=0.4, align='edge')


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
