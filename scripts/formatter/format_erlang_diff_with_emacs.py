#!/user/bin/env python3

# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import subprocess
import sys
import argparse
import os

default_mode = 'region'
default_emacs = 'emacs'
default_erlang = \
    '../build/tlm/deps/erlang.exploded/lib/erlang/lib/tools-*/emacs'


def build_region_formatter(start, end):
    # Work out how to format. If we don't supply start/end then we assume
    # point-min or point-max (the start and end of the buffer)
    formatter = ''
    if start is not None:
        formatter += f'(forward-line {start})'
        formatter += f'(setq start-pos (point))'
    else:
        formatter += f'(setq start-pos (point-min))'
        # Set start to 0 now, we will use it when calculating the end point, and
        # it is easier to set it to 0 than deal with None values
        start = 0

    if end is not None:
        diff = end - start
        if diff < 0:
            raise ValueError("End is less than start")
        formatter += f'(forward-line {diff})'
        formatter += f'(setq end-pos (point))'
    else:
        formatter += f'(setq end-pos (point-max))'

    formatter += '(erlang-indent-region start-pos end-pos)'
    formatter += f'(delete-trailing-whitespace)'
    return formatter


def build_function_formatter(start):
    formatter = ''
    formatter += f'(forward-line {start})'
    formatter += f'(erlang-indent-function)'
    return formatter


def format_erlang_diff_with_emacs(file, output_file, start, end, debug,
                                  format_mode=default_mode,
                                  emacs=default_emacs,
                                  erlang=default_erlang):
    """
    Format the given diff with emacs

    :param file: the file to format
    :param output_file: the file to write to
    :param start: the start of the region in need of formatting
    :param end: the end of the region in need of formatting
    :param debug: print extra debug information, in particular the output of
                  the emacs command
    :param format_mode: the mode in which to format - [region, function]
    :param emacs: emacs to use for formatting
    :param erlang: erlang to use for formatting
    :return:
    """

    # Maybe need to account for relative paths
    if not os.path.isabs(file):
        file = os.path.join(os.getcwd(), file)

    if not os.path.isabs(output_file):
        output_file = os.path.join(os.getcwd(), output_file)

    if not os.path.isabs(erlang):
        erlang = os.path.join(os.getcwd(), erlang)

    formatter = ''
    if format_mode == 'region':
        formatter += build_region_formatter(start, end)
    elif format_mode == 'function':
        formatter += build_function_formatter(start)

    formatter += f'(delete-trailing-whitespace)'

    # We run untabify over the entire buffer rather than just the diff that we
    # have changed because we may have fewer characters in the buffer after
    # running erlang-indent-region and delete-trailing-whitespace. We
    # /could/separate re-calculate our start-pos and end-pos but to do that
    # correctly we'd need to know the new extents of the diffs (they may have
    # changed). We shouldn't have tabs in our files anyway, so it's easier to
    # just untabify the entire buffer.
    formatter += f'(untabify (point-min) (point-max))'

    stderr = subprocess.DEVNULL
    if debug:
        stderr = None

    subprocess.check_output(
        f'{emacs} --batch --eval "'
        f'(progn (find-file \\"{file}\\")'
        f'(setq erlang-root-dir \\"{erlang}\\")'
        f'(setq load-path (cons (car '
        f'(file-expand-wildcards \\"{erlang}\\")) '
        f'load-path))'
        f'(setq load-path (cons \\"{erlang}\\" load-path))'
        f'(require \'erlang-start)'
        f'(erlang-mode)'
        + formatter +
        f'(write-region (point-min) (point-max) \\"{output_file}\\")'
        f'(kill-emacs))"', shell=True,
        stderr=stderr)


def add_common_args(arg_parser):
    arg_parser.add_argument('--emacs',
                            type=str,
                            help='Path to the emacs to use for formatting',
                            default=default_emacs)
    arg_parser.add_argument('--erlang',
                            type=str,
                            help='Path to the erlang emacs tools directory to '
                                 'use to format',
                            default=default_erlang)
    arg_parser.add_argument('--format-mode',
                            type=str,
                            choices=['region', 'function'],
                            default=default_mode)
    arg_parser.add_argument('--debug',
                            action='store_true',
                            help='Debug mode allows the emacs subprocess to '
                                 'pipe to stdout')


if __name__ == "__main__":
    argParser = argparse.ArgumentParser()
    add_common_args(argParser)
    argParser.add_argument('--file', '-f',
                           type=str,
                           required=True,
                           help='File to format')
    argParser.add_argument('--output', '-o',
                           type=str,
                           required=False,
                           help='File to output to')
    argParser.add_argument('--start', '-s',
                           type=int,
                           help='Line to start formatting from. Defaults to '
                                'start of file')
    argParser.add_argument('--end', '-e',
                           type=int,
                           help='Line to end formatting. Defaults to end of '
                                'file')

    args = argParser.parse_args()

    if args.output is None:
        args.output = args.file

    format_erlang_diff_with_emacs(args.file,
                                  args.output,
                                  args.start,
                                  args.end,
                                  args.debug,
                                  args.format_mode,
                                  args.emacs,
                                  args.erlang)
