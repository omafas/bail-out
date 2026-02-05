#!/user/bin/env python3

# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import argparse
import re
import subprocess
import os
import sys

from copy import copy

import get_diffs_to_format as git_diff
import format_erlang_diff_with_emacs as emacs_formatter

erlang_file_pattern =(
    re.compile(br'.*(\.erl|\.hrl|rebar.config.script|.app.src.script)'))
region_only_file_pattern =(
    re.compile(r'.*(rebar.config.script|.app.src.script)'))

def format_one_diff(args, start: int, end, file: str, output_file: str):
    """
    Format the given diff.

    :param args: for formatting
    :param start: start of the diff to format
    :param end: end of the diff to format (None if we should run to the end of
                the file
    :param file: file to format
    :param output_file: file to write to
    """
    if args.format_mode == 'region':
        start -= args.context_lines
        if end is not None:
            end += args.context_lines

    if not args.hook:
        print(f'Formatting {file} between {start} and {end}')

    emacs_formatter.format_erlang_diff_with_emacs(file,
                                                  output_file,
                                                  start, end,
                                                  args.debug,
                                                  args.format_mode,
                                                  args.emacs,
                                                  args.erlang)


def format_one_file(args, diffs, input_file: str, output_file=None):
    """
    Format all the diffs for the given file.

    :param args: for formatting
    :param diffs: list of tuples of diffs ('file', start, end)
    :param input_file: file to format
    :param output_file: file to write to. Overwrites input_file if not supplied
    """
    if output_file is None:
        output_file = input_file

    format_args = copy(args)
    if region_only_file_pattern.search(input_file):
        # We can only format this file in region mode, so we must modify
        # our args
        format_args.format_mode = 'region'
        format_one_diff(format_args, 0, None, input_file, output_file)
    else:
        for (start, end) in diffs:
            format_one_diff(args, start, end, input_file, output_file)


def check_mode(args, diffs):
    """
    Check the formatting of the given diffs conforms to the output of the
    formatter.

    :param args: for formatting
    :param diffs: list of tuples of diffs ('file', start, end)
    :return: boolean value indicating if the formatter would have changed any
             formatting
    """
    # To check formatting is correct we need to format the file into a new file
    # then run a 'git diff --no-index file1 file2' to compare it to the
    # original
    errors = False
    for bfile in diffs:
        if erlang_file_pattern.search(bfile):
            file = bfile.decode('utf-8')
            formatted_file = file + "_formatted"
            format_one_file(args, diffs[bfile], file, formatted_file)

            diff_cmd = ['git', 'diff', '--no-index']
            if args.hook:
                diff_cmd += ['--quiet']

            diff_cmd += [file, formatted_file]
            ret = subprocess.run(diff_cmd)

            # Tidy up the "_formatted" file now, we don't need it
            os.remove(formatted_file)
            errors |= ret.returncode != 0

    return errors


def has_unstaged_changes(file):
    return subprocess.run(['git', 'diff', '--quiet', file]).returncode != 0


def overwrite_mode(args, diffs):
    """
    Re-format the given diffs.

    :param args: Args for formatting
    :param diffs: list of tuples of diffs ('file', start, end)
    """
    if args.hook:
        for bfile in diffs:
            if erlang_file_pattern.search(bfile):
                file = bfile.decode('utf-8')
                if has_unstaged_changes(file):
                    print(f'File {file} has un-staged changes. '
                          f'Cannot format it. Please stage changes and try '
                          f'again.')
                    return 1

    for bfile in diffs:
        if erlang_file_pattern.search(bfile):
            file = bfile.decode('utf-8')

            if args.hook:
                # The commit hook reads files from stdout to determine which
                # files should be added to a commit, we need to print to
                # stdout here to facilitate that.
                print(file)

            format_one_file(args, diffs[bfile], file)

    return 0


def erlang_format(args):
    diffs = git_diff.parse_git_diffs_for_formatting(
                git_diff.get_git_diff(args.hook))

    if args.format_output_mode == 'check':
        if check_mode(args, diffs):
            print("Formatting does not conform")
            return 1
        return 0

    if args.format_output_mode == 'overwrite':
        return overwrite_mode(args, diffs)

    return 1


if __name__ == "__main__":
    argParser = argparse.ArgumentParser()
    emacs_formatter.add_common_args(argParser)
    argParser.add_argument('--context-lines', '-c',
                           type=int,
                           help='Context lines around which we format. Used '
                                'with the region mode. Defaults to 1',
                           default=1)
    argParser.add_argument('--hook',
                           action='store_true',
                           help='Commit hook mode prints different output and '
                                'suppresses emacs output for the hook to '
                                'function correctly')
    argParser.add_argument('--format-output-mode',
                           type=str,
                           choices=['overwrite', 'check'],
                           default='overwrite')

    sys.exit(erlang_format(argParser.parse_args()))
