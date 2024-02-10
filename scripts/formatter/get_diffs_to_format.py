#!/user/bin/env python3

import re
import subprocess
import sys

# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

from collections import defaultdict


def get_git_diff(staged_only: bool):
    """
    Get the git diff from the current directory

    :type staged_only: bool
    :param staged_only: if we only want staged changes
    :return: "git diff -U0 --staged" output
    """
    # -U0 removes context lines to make parsing easier.
    # --diff-filter=d removes all deleted changes from the diff. This tool is
    # for formatting and we shouldn't attempt to format deleted files.
    args = ['git', 'diff', '-U0', '--diff-filter=d']
    if staged_only:
        # We only attempt to format staged changes.
        args += ['--staged']
    return subprocess.check_output(args)


def parse_git_diffs_for_formatting(diff_output: bytes):
    """
    Parses a git diff to get a list of the newly changed regions

    :type diff_output:  bytes
    :param diff_output: the output of the git diff
    :return: list of tuples of diffs ('file', start, end)
    """

    # b (bytes) is needed as we perform this on bytes objects
    # r (raw) string stops pep8 from warning about escape sequences
    file_pattern = re.compile(br'\+\+\+ b/(.*)')
    start_diff_pattern = re.compile(br'@@ -(\d*)[ ,]')
    newly_changed_line_pattern = re.compile(br'\+.*')

    current_file = ''
    line_counter = 0
    start_diff_line = 0
    calculating_diff = False
    diffs = defaultdict(list)

    for line in diff_output.splitlines():
        # Step 1) Find a match for the line containing the file name
        line_match = file_pattern.search(line)
        if line_match:
            # First match won't be tracking a diff yet
            if calculating_diff:
                calculating_diff = False
                diffs[current_file].append((start_diff_line, start_diff_line +
                                            line_counter - 1))
                line_counter = 0

            current_file = line_match.group(1)
            continue

        # Step 2) Find a match for the line containing the start of a changed
        #         region
        line_match = start_diff_pattern.search(line)
        if line_match:
            # First match won't be tracking a diff yet
            if calculating_diff:
                diffs[current_file].append((start_diff_line, start_diff_line +
                                            line_counter - 1))
                line_counter = 0

            start_diff_line = int(line_match.group(1))
            calculating_diff = True
            continue

        # Step 3) Find a match for a newly changed line (starting with '+')
        #         and count them
        if calculating_diff:
            line_match = newly_changed_line_pattern.search(line)
            if line_match:
                line_counter += 1

    # Step 4) Last diff isn't terminated by any other match, add it
    if calculating_diff:
        diffs[current_file].append((start_diff_line, start_diff_line +
                                    line_counter - 1))

    return diffs


if __name__ == "__main__":
    ret = parse_git_diffs_for_formatting(get_git_diff(False))
    for diff in ret:
        print(diff)
