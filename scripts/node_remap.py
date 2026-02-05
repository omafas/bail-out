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

def run_config_remap(initargs,
                     output_path,
                     extra_args,
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

    cmd = [escript_path,
           escript_wrapper_path,
           '--initargs-path', initargs_path,
           '--', config_remap_path,
           '--initargs-path', initargs_path,
           '--output-path', output_path,
           '--log-level', log_level] + extra_args

    pr = subprocess.run(cmd, capture_output=capture_output)
    pr.check_returncode()

def run_config_remap_via_escript_wrapper(initargs,
                                         output_path,
                                         remap,
                                         kv_rewrites = [],
                                         log_level='info',
                                         capture_output=False,
                                         root_dir=basedir()):
    extra_args = []
    for remap_arg in remap:
        print(f"remap: {remap_arg}")
        extra_args += ['--remap'] + remap_arg

    for kv in kv_rewrites:
        print(f"kv: {kv}")
        extra_args += ['--rewrite-key-value'] + kv

    extra_args = ['--regenerate-cookie',
                  '--regenerate-cluster-uuid',
                  '--remove-alternate-addresses',
                  '--disable-auto-failover'] + extra_args

    run_config_remap(initargs, output_path, extra_args, log_level,
                     capture_output, root_dir)

def disable_afo_via_config_remap(initargs,
                                 output_path,
                                 log_level='info',
                                 capture_output=False,
                                 root_dir=basedir()):
    extra_args = ['--disable-auto-failover']
    run_config_remap(initargs, output_path, extra_args, log_level,
                     capture_output, root_dir)
