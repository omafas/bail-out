#!/usr/bin/env python3
#
# @author Couchbase <info@couchbase.com>
# @copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import os.path
import subprocess

from installed_script_helpers import find_valid_binary, basedir

def run_config_remap_via_escript_wrapper(initargs,
                                         output_path,
                                         remap,
                                         log_level='info',
                                         capture_output=False,
                                         root_dir=basedir()):
    escript_path = find_valid_binary('escript', root_dir)
    escript_wrapper_path = find_valid_binary('escript-wrapper', root_dir)
    config_remap_path = find_valid_binary('config_remap', root_dir)

    initargs_path = ''
    for possible_initargs in initargs:
        if os.path.exists(possible_initargs):
            initargs_path = possible_initargs
            break
        raise RuntimeError("Did not find initargs")

    remap_args = []
    for remap_arg in remap:
        print(remap_arg)
        remap_args += ['--remap'] + remap_arg

    cmd = [escript_path,
           escript_wrapper_path,
           '--initargs-path', initargs_path,
           '--', config_remap_path,
           '--initargs-path', initargs_path,
           '--output-path', output_path,
           '--regenerate-cookie',
           '--regenerate-cluster-uuid',
           '--remove-alternate-addresses',
           '--disable-auto-failover',
           '--log-level', log_level] + remap_args

    pr = subprocess.run(cmd, capture_output=capture_output)
    pr.check_returncode()
