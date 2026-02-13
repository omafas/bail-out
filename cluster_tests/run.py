#!/usr/bin/env python3
#
# @author Couchbase <info@couchbase.com>
# @copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import os
import sys
import getopt
import shutil
import inspect
from datetime import datetime, timezone
from math import floor
from difflib import get_close_matches

import requests
import glob
import time
import random
import pprint
from copy import deepcopy
import builtins

# Pretty prints any tracebacks that may be generated if the process dies
from traceback_with_variables import activate_by_import

import testlib

sys.path.append(testlib.get_pylib_dir())

import cluster_run_lib
from testlib import UnmetRequirementsError, TestError
from testlib.cluster import InconsistentClusterError
from testlib import test_tag_decorator

from testsets import \
    authn_tests, \
    auto_failover_test, \
    sample_buckets, \
    ldap_tests, \
    tasks_test, \
    saml_tests, \
    bucket_deletion_test, \
    node_addition_tests, \
    users_backup_tests, \
    prom_sd_config_test, \
    serviceless_node_tests, \
    bucket_migration_test, \
    bucket_test, \
    internal_creds_rotation_tests, \
    pass_hashing_settings_tests, \
    secret_management_tests, \
    cert_load_tests, \
    alerting_tests, \
    resource_management_test, \
    stats_tests, \
    collection_tests, \
    web_server_tests, \
    cbauth_cache_config_tests, \
    hard_reset_test, \
    crud_tests, \
    settings_managers_tests, \
    users_tests, \
    web_settings_tests, \
    native_encryption_tests, \
    rest_eject_test, \
    node_remap_tests, \
    services_topology_tests, \
    cbcollect_tests, \
    config_remap_tests, \
    cli_integration_tests, \
    metakv2_tests, \
    app_telemetry_tests, \
    jwt_tests, \
    bucket_dirs_cleanup_tests, \
    fusion_tests, \
    metakv_tests, \
    rebalance_tests

tmp_cluster_dir = os.path.join(testlib.get_cluster_test_dir(),
                               "test_cluster_data")

log_collection_default_regex = (".*diags"
                                "|master events"
                                "|memcached.*"
                                "|Collecting .*/snapshots"
                                "|Chronicle dump")

USAGE_STRING = f"""
Usage: {{program_name}}
    [--cluster | -c <address>:<port>]
        Specify already started cluster to connect to.
    [--user | -u <admin>]
        Username to be used when connecting to an existing cluster.
        Default: Administrator. Only used with --cluster | -c
    [--password | -p <admin_password>]
        Password to be used when connecting to an existing cluster.
        Default: asdasd. Only used with --cluster | -c
    [--num-nodes | -n <num_nodes>]
        Number of nodes available for an existing cluster. Use when not all
        nodes are already connected, for tests that need this configuration.
        When unspecified, num_nodes is assumed to be equal to the number of
        connected nodes. Only used with --cluster | -c
    [--tests | -t <test_spec>[, <test_spec> ...]]
        <test_spec> := <test_class>[.test_name]
        Start only specified tests
    [--list | -l]
        List all available tests and exit
    [--with-tags <tag>[, <tag> ...]
        Run only tests with at least one of the specified tags
    [--without-tags <tag>[, <tag> ...]
        Run only tests with none of the specified tags
    [--ignore-unknown-tags]
        Don't give an error if tags are specified that aren't recognised.
        Used for commit-validation jobs across multiple branches which may have
        different known tags.
    [--keep-tmp-dirs | -k]
        Keep any test_cluster_data dirs after tests finish, even if they pass
    [--dont-intercept-output | -o]
        Display output from tests. By default, output is suppressed (unless the
        test fails). Setting this option forces output to be displayed even for
        successful test runs
    [--seed | -s <string>]
        Specify a seed to be set for python pseudo-random number generator
    [--verbose | -v]
        Print more debug information
    [--dry-run]
        Do not actually run tests (useful for framework debugging)
    [--dont-reuse-clusters]
        Start a separate cluster for each testset
    [--randomize-clusters]
        Randomize requirements that are not explicitly set
    [--random-order]
        Randomize order of tests
    [--testset-iterations=N]
        Run each testset N times (1 by default)
    [--test-iterations=M]
        Run each test M times (1 by default)
    [--start-index=N]
        Use N as a start-index for all started clusters
    [--stop-after-error]
        Stop running testsets after the first error
    [--collect-logs-after-error]
        Collect a zip of logs for each node in and out of the cluster after a
        failed test
    [--logs-task-regex=Regex]
        Specify the Regex for log collection after any errors.
        If no Regex is specified, the following regex will be used:
        {log_collection_default_regex}
    [--dont-report-time]
        Do not prepend any output with current time
    [--test-timeout=N]
        Set test timeout to N seconds
    [--no-res-alignment]
        Do not right align the result column in the output
    [--no-wrap]
        Do not wrap the output to the screen width
    [--help]
        Show this help
"""


def usage():
    print(USAGE_STRING.format(program_name=sys.argv[0]))


def bad_args_exit(msg):
    print(testlib.red(msg))
    usage()
    sys.exit(2)


def error_exit(msg):
    restore_print()
    print(testlib.red(msg))
    sys.exit(2)


def warning_exit(msg):
    print(testlib.yellow(msg))
    sys.exit(3)


def list_all_tests():
    testsets = discover_testsets()
    # Sort testsets alphabetically
    testsets.sort(key=lambda x: x[0])
    print("Available testsets and tests:")
    for name, _, tests, _ in testsets:
        tests.sort()
        print(f"\n{name}:")
        for test in tests:
            print(f"  - {test}")


def find_similar_tests(name, available_names, max_suggestions=1, cutoff=0.6,
                       context='test'):
    # Create lowercase versions for comparison
    name_lower = name.lower()
    available_lower = {n: n.lower() for n in available_names}
    lower_to_original = {v: k for k, v in available_lower.items()}

    # First try with case-insensitive matching
    similar_lower = get_close_matches(name_lower,
                                      list(lower_to_original.keys()),
                                      n=max_suggestions,
                                      cutoff=cutoff)
    if similar_lower:
        # Map back to original case
        similar = [lower_to_original[s] for s in similar_lower]
        return similar

    # Try without common suffixes if no matches found
    if context == 'testset':
        suffixes = ['Tests', 'Test']
    else:  # context == 'test'
        suffixes = ['_test']

    for suffix in suffixes:
        if name_lower.endswith(suffix.lower()):
            return []
        # User hasn't specified the suffix, try searching among tests without it
        available_without_suffix = []
        orig_to_stripped = {}

        for avail_name in available_names:
            if avail_name.endswith(suffix):
                stripped = avail_name[:-len(suffix)].lower()
                available_without_suffix.append(stripped)
                orig_to_stripped[stripped] = avail_name

        similar_stripped = get_close_matches(name_lower,
                                             available_without_suffix,
                                             n=max_suggestions,
                                             cutoff=cutoff)
        if similar_stripped:
            # Map back to original names
            similar = [orig_to_stripped[s] for s in similar_stripped]
            return similar

    return []


def remove_temp_cluster_directories():
    for dir in glob.glob(tmp_cluster_dir + "*"):
        testlib.maybe_print(f"Removing cluster dir {dir}...")
        shutil.rmtree(dir)


def main():
    # we use assert statements in tests, so make sure they are not disabled
    if not __debug__:
        raise RuntimeError("Assert statements are disabled")
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], "hkovc:u:p:n:t:s:l",
                                          ["help", "keep-tmp-dirs", "cluster=",
                                           "user=", "password=", "num-nodes=",
                                           "tests=", 'with-tags=', "list",
                                           'without-tags=',
                                           "dont-intercept-output",
                                           'ignore-unknown-tags',
                                           "seed=", "colors=", "verbose",
                                           "dry-run", 'dont-reuse-clusters',
                                           'randomize-clusters',
                                           'random-order',
                                           'testset-iterations=',
                                           'start-index=',
                                           'test-iterations=',
                                           'stop-after-error',
                                           'collect-logs-after-error',
                                           'logs-task-regex=',
                                           'dont-report-time',
                                           'test-timeout=',
                                           'no-res-alignment',
                                           'no-wrap'])
    except getopt.GetoptError as err:
        bad_args_exit(str(err))

    use_existing_server = False
    username = 'Administrator'
    password = 'asdasd'
    num_nodes = None
    address = '127.0.0.1'
    start_port = cluster_run_lib.base_api_port
    tests = None
    with_tags = None
    without_tags = None
    ignore_unknown_tags = False
    seed = testlib.random_str(16)
    reuse_clusters = True
    randomize_clusters = False
    random_order = False
    testset_iterations = 1
    test_iterations = 1
    start_index = 10
    stop_after_first_error = False
    collect_logs = False
    log_collection_regex = log_collection_default_regex

    for o, a in optlist:
        if o in ('--cluster', '-c'):
            tokens = a.rsplit(':', maxsplit=1)
            if len(tokens) != 2:
                bad_args_exit(f"Invalid format. Should be {o} <address>:<port>")
            address = tokens[0]
            start_port = int(tokens[1])
            use_existing_server = True
        elif o in ('--user', '-u'):
            if not use_existing_server:
                bad_args_exit(f"{o} is only supported with --cluster | -c")
            username = a
        elif o in ('--password', '-p'):
            if not use_existing_server:
                bad_args_exit(f"{o} is only supported with --cluster | -c")
            password = a
        elif o in ('--num-nodes', '-n'):
            if not use_existing_server:
                bad_args_exit(f"{o} is only supported with --cluster | -c")
            num_nodes = int(a)
        elif o in ('--tests', '-t'):
            tests = []
            for tokens in [t.strip().split(".") for t in a.split(",")]:
                if len(tokens) == 1:
                    tests.append((tokens[0], '*'))
                elif len(tokens) == 2:
                    tests.append((tokens[0], tokens[1]))
        elif o == '--with-tags':
            with_tags = list(map(test_tag_decorator.tag_from_str, a.split(",")))
        elif o == '--without-tags':
            without_tags = list(map(test_tag_decorator.tag_from_str,
                                    a.split(",")))
        elif o == '--ignore-unknown-tags':
            ignore_unknown_tags = True
        elif o in ('--keep-tmp-dirs', '-k'):
            testlib.config['keep_tmp_dirs'] = True
        elif o in ('--dont-intercept-output', '-o'):
            testlib.config['intercept_output'] = False
        elif o in ('--seed', '-s'):
            seed = a
        elif o == '--colors':
            testlib.config['colors'] = (int(a) == 1)
        elif o in ('--verbose', '-v'):
            testlib.config['verbose'] = True
        elif o == '--dry-run':
            testlib.config['dry_run'] = True
        elif o == '--dont-report-time':
            testlib.config['report_time'] = False
        elif o == '--dont-reuse-clusters':
            reuse_clusters = False
        elif o == '--randomize-clusters':
            randomize_clusters = True
        elif o == '--random-order':
            random_order = True
        elif o == '--testset-iterations':
            testset_iterations = int(a)
        elif o == '--test-iterations':
            test_iterations = int(a)
        elif o == '--start-index':
            start_index = int(a)
        elif o == '--stop-after-error':
            stop_after_first_error = True
        elif o == '--collect-logs-after-error':
            collect_logs = True
        elif o == "--logs-task-regex":
            log_collection_regex = a
        elif o == '--test-timeout':
            testlib.config['test_timeout'] = int(a)
        elif o == '--no-res-alignment':
            testlib.config['align_res'] = False
        elif o == '--no-wrap':
            testlib.config['wrap_output'] = False
        elif o in ('--list', '-l'):
            list_all_tests()
            exit(0)
        elif o in ('--help', '-h'):
            usage()
            exit(0)
        else:
            assert False, f"unhandled options: {o}"

    if ignore_unknown_tags:
        # Remove any unparsed tags
        if with_tags is not None:
            with_tags = [tag for tag in with_tags
                         if isinstance(tag, test_tag_decorator.Tag)]
        if without_tags is not None:
            without_tags = [tag for tag in without_tags
                            if isinstance(tag, test_tag_decorator.Tag)]
    else:
        if with_tags is not None:
            for tag in with_tags:
                if not isinstance(tag, test_tag_decorator.Tag):
                    raise ValueError(f"{tag} is not a valid Tag")
        if without_tags is not None:
            for tag in without_tags:
                if not isinstance(tag, test_tag_decorator.Tag):
                    raise ValueError(f"{tag} is not a valid Tag")

    override_print()

    random.seed(seed)
    discovered_tests = discover_testsets()

    errors = {}
    not_ran = []
    # Remove any testsets that didn't correctly specify requirements
    for discovered_test in discovered_tests:
        (name, _, _, configurations) = discovered_test
        reason = None
        if not isinstance(configurations, list):
            reason = configurations
            not_ran.append((name, reason))
        else:
            for configuration in configurations:
                if not isinstance(configuration, testlib.ClusterRequirements):
                    reason = configuration
                    not_ran.append((name, reason))
        if reason is not None:
            discovered_tests.remove(discovered_test)

    if len(not_ran) > 0:
        msg = "Some testsets did not correctly specify requirements:\n"
        for (name, reason) in not_ran:
            msg += f"{name} - {reason}\n"
        error_exit(msg)
    testsets_str = ", ".join([c for c, _, _, _ in discovered_tests])
    testlib.maybe_print(f"Discovered testsets: {testsets_str}")

    try:
        testsets_to_run = find_tests(tests, discovered_tests, with_tags,
                                     without_tags)
    except ValueError as e:
        error_exit(str(e))

    if not testsets_to_run:
        warning_exit("No tests matched the specified test/tag filters")

    total_num, testsets_grouped = \
        group_testsets(testsets_to_run, reuse_clusters,
                       randomize_clusters, random_order,
                       testset_iterations, test_iterations)
    testlib.maybe_print(f"Groupped testsets:")
    testlib.maybe_print(testsets_grouped, print_fun=pprint.pprint)

    cluster = None
    if use_existing_server:
        # Get provided cluster
        cluster = get_existing_cluster(address, start_port,
                                       (username, password), num_nodes)
        testlib.maybe_print(f"Discovered cluster: {cluster}")
    else:
        remove_temp_cluster_directories()

    executed = 0
    test_time = 0
    total_log_collection_time = 0
    start_ts = time.time_ns()
    not_ran = []
    for (configuration, testsets) in testsets_grouped:
        if stop_after_first_error and len(errors) > 0:
            for testset in testsets:
                not_ran.append(TestError(
                    name=testset['name'],
                    error=RuntimeError("prior testset failed"),
                    cluster_name=cluster.short_name()))
            continue
        # Get an appropriate cluster to satisfy the configuration
        if use_existing_server:
            unmet_requirements = configuration.get_unmet_requirements(cluster)
            if len(unmet_requirements) > 0:
                for testset in testsets:
                    reason = "Cluster provided does not satisfy test " \
                             f"requirements:\n" \
                             f"{[str(r) for r in unmet_requirements]}"
                    not_ran.append(TestError(
                        name=testset['name'],
                        error=RuntimeError(reason),
                        cluster_name=cluster.short_name()))
                continue
            cluster.set_requirements(configuration)
        else:
            cluster = testlib.get_appropriate_cluster(cluster,
                                                      (username, password),
                                                      configuration,
                                                      tmp_cluster_dir,
                                                      reuse_clusters,
                                                      start_index)
        testset_start_ts = time.time_ns()
        # Run the testsets on the cluster
        tests_executed, testset_errors, testset_not_ran, log_collection_time,\
            cluster = \
            run_testsets(cluster, testsets, total_num, log_collection_regex,
                         seed=seed,
                         stop_after_first_error=stop_after_first_error,
                         collect_logs=collect_logs)
        test_time += (time.time_ns() - testset_start_ts)
        executed += tests_executed
        for k in testset_errors:
            if k not in errors:
                errors[k] = []
            errors[k].extend(testset_errors[k])
        not_ran += testset_not_ran
        total_log_collection_time += log_collection_time

    restore_print()

    ns_in_sec = 1000000000
    total_time = time.time_ns() - start_ts
    total_time_s = total_time / ns_in_sec
    prep_time_s = (total_time - test_time) / ns_in_sec
    test_time_s = test_time / ns_in_sec
    log_collection_time_s = total_log_collection_time / ns_in_sec

    error_num = sum([len(errors[name]) for name in errors])
    errors_str = f"{error_num} error{'s' if error_num != 1 else ''}"
    if error_num == 0:
        colored = testlib.green
    else:
        colored = testlib.red

    def format_time(t):
        return f"{int(t//60)}m{t%60:.1f}s"

    print("\n" + "=" * testlib.config['screen_width'] + "\n" +
          colored(f"Tests finished ({executed} executed, {errors_str})\n") +
          f"Total time:               {format_time(total_time_s)}\n"
          f"Total clusters prep time: {format_time(prep_time_s)}\n"
          f"Test time (no prep):      {format_time(test_time_s)}")

    if executed > 0:
        print(
          f"Avg. test time:           {format_time(total_time_s/executed)}\n"
          f"Avg. test time (no prep): {format_time(test_time_s/executed)}")

    if log_collection_time_s > 0:
        print("Total log collection time: "
              f"{format_time(log_collection_time_s)}")

    print(f"\nSeed: {seed}\n")

    for name in errors:
        print(f"In {name}:")
        for testres in errors[name]:
            print(f"  {testres}")
        print()

    if len(not_ran) > 0:
        print(f"Couldn't run the following tests:")
        for testres in not_ran:
            print(f"  {testres}")
        print()

    if len(errors) > 0:
        error_exit("Tests finished with errors")
    elif len(not_ran) > 0:
        warning_exit("Some tests were skipped")
    elif not (testlib.config['keep_tmp_dirs'] or
              check_for_core_files() or
              cluster.is_existing_cluster()):
        # Kill any created nodes and possibly delete directories as we don't
        # need to keep around data from successful tests
        cluster.destroy()
        remove_temp_cluster_directories()


# If there are core files, the tests may have passed but something went wrong in
# erlang, so it is valuable to keep the logs in this case
def check_for_core_files():
    if keep := len(glob.glob("/tmp/core.*")) > 0:
        print("Core file(s) found. Keeping cluster logs")
    return keep


def group_testsets(testsets, reuse_clusters, randomize_clusters,
                   random_order, testset_iterations, test_iterations):
    # Group by requirements
    testsets_grouped = []
    for class_name, testset_class, test_names, configurations in testsets:
        for k in range(0, testset_iterations):
            for requirements in configurations:
                different = True
                iter_str = f'#{k+1}' if k != 0 else ''
                testset_name = f"{class_name}{iter_str} / {requirements}"
                test_list = []
                for n in test_names:
                    for i in range(0, test_iterations):
                        test_list.append({'name': n, 'iter': i})
                testset = {'name': testset_name,
                           'class': testset_class,
                           'test_name_list': test_list,
                           'requirements': requirements,
                           'iter': k}
                if reuse_clusters:
                    for i, (other_reqs, other_testsets) in \
                        enumerate(testsets_grouped):
                        succ, new_reqs = other_reqs.intersect(requirements)
                        if succ:
                            other_testsets.append(testset)
                            testsets_grouped[i] = (new_reqs, other_testsets)
                            different = False
                            break
                if different:
                    testsets_grouped.append((deepcopy(requirements),
                                            [testset]))

    for (req, testsets) in testsets_grouped:
        if randomize_clusters:
            req.randomize_unset_requirements()
        if random_order:
            random.shuffle(testsets)
            for t in testsets:
                random.shuffle(t['test_name_list'])

    """
    Sort testset groups by requirements string. The string lists immutable
    # requirements first, then mutable requirements. This ensures that any sets
    # of compatible configurations will be adjacent in the list. For example:
    (edition=Enterprise,num_nodes
    If there is no need to reuse cluster, there is no need to sort groups.
    """
    sorted_testsets_grouped = \
        sorted(testsets_grouped, key=lambda x: str(x[0])) \
        if reuse_clusters else testsets_grouped
    tests_count = 0
    for (req, testsets) in sorted_testsets_grouped:
        for t in testsets:
            t['#'] = tests_count + 1
            tests_count += 1

    return (tests_count, sorted_testsets_grouped)


def find_tests(test_names, discovered_list, with_tags, without_tags):
    if test_names is not None:
        discovered_list = get_testsets_by_names(test_names, discovered_list)
    if with_tags or without_tags:
        discovered_list = get_testsets_by_tags(discovered_list, with_tags,
                                               without_tags)
    return discovered_list


def get_testsets_by_names(test_names, discovered_list):
    results = {}
    discovered_dict = {n: (cl, t, cf) for n, cl, t, cf in discovered_list}
    test_list = list(discovered_dict.keys())
    test_list.sort()
    for class_name, test_name in test_names:
        if class_name not in discovered_dict:
            msg = f"Testset '{class_name}' is not found."
            similar = find_similar_tests(class_name, test_list,
                                         context='testset')
            if similar:
                if len(similar) == 1:
                    msg += f"\n\nDid you mean {similar[0]}?"
                else:
                    msg += f"\n\nDid you mean one of: {', '.join(similar)}?"
            msg += "\n\nAvailable testsets:\n"
            for t in test_list:
                msg += f"  - {t}\n"
            raise ValueError(msg)

        testset, tests, configurations = discovered_dict[class_name]
        if test_name == '*':
            results[class_name] = (testset, tests, configurations)
        else:
            if test_name not in tests:
                tests.sort()
                msg = f"Test '{test_name}' is not found " \
                      f"in testset '{class_name}'."
                similar = find_similar_tests(test_name, tests, context='test')
                if similar:
                    if len(similar) == 1:
                        msg += f"\n\nDid you mean: {similar[0]}?"
                    else:
                        msg += f"\n\nDid you mean one of: {', '.join(similar)}?"
                msg += f"\n\nAvailable tests in {class_name}:\n"
                for t in tests:
                    msg += f"  - {t}\n"
                raise ValueError(msg)

            if class_name in results:
                testlist = results[class_name][1]
                testlist.append(test_name)
                results[class_name] = (results[class_name][0], testlist,
                                       configurations)
            else:
                results[class_name] = (testset, [test_name], configurations)

    return [(k, results[k][0], results[k][1], results[k][2]) for k in results]


def get_testsets_by_tags(testset_list, with_tags, without_tags):
    def test_matches_tags(cl):
        def f(test):
            test_func = getattr(cl, test)
            if test_func is not None:
                tags = test_tag_decorator.get_tags(test_func)
                return ((with_tags is None or any(tag in with_tags
                                                  for tag in tags)) and
                        (without_tags is None or all(tag not in without_tags
                                                     for tag in tags)))
            return False
        return f
    testsets_filtered = []
    for (n, cl, tests, cf) in testset_list:
        new_tests = list(filter(test_matches_tags(cl), tests))
        if len(new_tests) > 0:
            testsets_filtered.append((n, cl, new_tests, cf))
    return testsets_filtered


def discover_testsets():
    testsets = []

    def add_testset(testset_name, testset_class, configuration):
        tests = [test for test in dir(testset) if test.endswith('_test') or \
                                                  test.endswith('_test_gen')]
        if len(tests) > 0:
            testsets.append((testset_name, testset_class, tests, configuration))

    for m in sys.modules.keys():
        if not hasattr(sys.modules[m], '__file__'):
            continue
        if sys.modules[m].__file__ is None:
            continue
        if os.path.normpath(os.path.join(testlib.get_cluster_test_dir(),
                                         "testsets")) != \
                os.path.normpath(os.path.dirname(sys.modules[m].__file__)):
            continue
        for name, testset in inspect.getmembers(sys.modules[m], inspect.isclass):
            if testset == testlib.BaseTestSet:
                continue
            if issubclass(testset, testlib.BaseTestSet):
                requirements, err, _ = testlib.safe_test_function_call(
                    testset, 'requirements', [], 0, dry_run=False)
                if err is not None:
                    return [(err.name, None, None, err.error)]
                if isinstance(requirements, list):
                    add_testset(name, testset, requirements)
                else:
                    add_testset(name, testset, [requirements])

    return testsets


def get_existing_cluster(address, start_port, auth, num_nodes):
    if num_nodes is None:
        # If a number of nodes was not provided, we assume that all required
        # nodes are already in the cluster at the provided address
        url = f"http://{address}:{start_port}"

        # Check that node is online
        pools_default = f"{url}/pools/default"
        try:
            response = requests.get(pools_default, auth=auth)
            if response.status_code == 200:
                # Retrieve the number of nodes
                num_nodes = len(response.json().get("nodes", []))
                if num_nodes == 0:
                    raise RuntimeError(f"Failed to find any nodes at "
                                       f"{pools_default}")
            else:
                raise RuntimeError(f"Failed to connect to {pools_default} "
                                   f"({response.status_code})\n"
                                   f"{response.text}")
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(f"Failed to connect to {pools_default}\n{e}")

    nodes = [testlib.Node(host=address,
                          port=start_port + i,
                          auth=auth)
             for i in range(num_nodes)]

    with testlib.no_output("connecting to existing cluster"):
        return testlib.cluster.get_cluster(0, start_port, auth, [], nodes, None)


# Run each testset on the same cluster, counting how many individual tests were
# ran, and keeping track of all errors
def run_testsets(cluster, testsets, total_num, log_collection_regex, seed=None,
                 stop_after_first_error=False, collect_logs=False):
    executed = 0
    errors = {}
    not_ran = []
    current_unmet_requirements = []
    cluster_is_unusable = False
    log_collection_time = 0
    for testset in testsets:

        def skip_this_testset(error):
            for skip_test in testset['test_name_list']:
                not_ran.append(TestError(
                    name=testlib.test_name(testset['class'],
                                           skip_test['name'],
                                           skip_test['iter']),
                    error=error,
                    cluster_name=cluster.short_name()))

        if cluster_is_unusable:
            skip_this_testset('Cluster is in incosistent state (can be '
                              'caused by a timed out rebalance)')
            continue

        if stop_after_first_error and len(errors) > 0:
            skip_this_testset("prior testset failed")
            continue

        # We should be able to reuse the cluster here (because we constructed
        # this cluster specifically for this testset), so make sure
        # requirements are still met for this testset (other tests could have
        # broken it).
        # Note that here we are testing cluster against specific testset
        # requirements, while below (after the testset run) we test cluster
        # against global cluster requirements (basically intersection of all
        # requirements in these testset group)
        unmet = cluster.repair_requirements(testset['requirements'])
        if len(unmet) > 0:
            unmet_str = ', '.join(str(r) for r in unmet)
            skip_this_testset('Cluster does not satisfy test requirements ' \
                              f'{unmet_str} (probably broken by previous ' \
                              'testsets)')
            continue

        testset_errors = []
        try:
            res = testlib.run_testset(
                testset, cluster, total_num,
                seed=seed,
                stop_after_first_error=stop_after_first_error)
            executed += res[0]
            testset_errors = res[1]
            not_ran += res[2]
            cluster = res[3]
        # We use a catch-all here because if we hit any errors while running
        # a testset, but not in a test, then it is likely an issue with the
        # cluster, so we should keep running more tests
        except Exception as e:
            testlib.print_traceback()
            testset_errors.append(TestError(
                name=f'Testset {testset["name"]} execution',
                error=e,
                cluster_name=cluster.short_name()))
            cluster_is_unusable = True

        try:
            unmet = cluster.maybe_repair_cluster_requirements()
            new_unmet = [r for r in unmet
                         if r not in current_unmet_requirements]
            current_unmet_requirements = unmet
            # If this testset has added new unmet requirements,
            # we should treat that testset as failure
            if len(new_unmet) > 0:
                error_msg = f'Test {testset["name"]} broke some ' \
                            'cluster requirements'
                test_error = UnmetRequirementsError(new_unmet,
                                                    message=error_msg)
                print(testlib.red(str(test_error)))
                testset_errors.append(TestError(
                    name=f'Requirements check after {testset["name"]}',
                    error=test_error,
                    cluster_name=cluster.short_name()))
        except InconsistentClusterError as e:
            print(testlib.red(str(e)))
            testset_errors.append(TestError(
                name='Cluster smog check',
                error=e,
                cluster_name=cluster.short_name()))
            cluster_is_unusable = True

        if len(testset_errors) > 0:
            if testset['name'] not in errors:
                errors[testset['name']] = []
            errors[testset['name']].extend(testset_errors)

    if collect_logs and len(errors) > 0:
        collect_start_time = time.time_ns()
        # Attempt a cbcollect for the cluster, in order to get all info
        # that might be useful for debugging
        with testlib.no_output("start log collection"):
            cluster.wait_nodes_up()

            for node in cluster._nodes:
                if node not in cluster.connected_nodes:
                    try:
                        testlib.wait_for_ejected_node(node)
                    except AssertionError:
                        print(f"Wait for ejected node {node} to be ejected "
                              f"timed-out, attempting to collect logs anyway")
                start_time = floor(datetime.now(timezone.utc).timestamp())
                testlib.start_log_collection(node,
                                             taskRegexp=log_collection_regex)

                path = testlib.wait_for_log_collection(node, start_time)
                print(f"Collected logs for {node.url}: {path}")

        log_collection_time += (time.time_ns() - collect_start_time)
    return executed, errors, not_ran, log_collection_time, cluster


def print_with_time(*args, show_time=True, **kwargs):
    forbidden = not testlib.config['report_time']
    if len(args) == 0 or not show_time or forbidden:
        __builtins__.__old_print_fun(*args, **kwargs)
        return

    cr_count = 0
    first_arg = args[0]

    if isinstance(first_arg, str):
        while True:
            # This is needed in order to handle calls like print("\nbla")
            # the following way: \n<time> bla
            # instead of: <time> \nbla
            if first_arg.startswith('\n'):
                first_arg = first_arg[1:]
                cr_count += 1
            else:
                break

    cr = '\n' * cr_count
    local_time = datetime.now().strftime(testlib.config['report_time_format'])
    prefix = f'{cr}{local_time}'
    __builtins__.__old_print_fun(prefix, first_arg, *(args[1:]), **kwargs)


def override_print():
    if not hasattr(builtins, '__old_print_fun'):
        builtins.__old_print_fun = builtins.print
        builtins.print = print_with_time


def restore_print():
    if hasattr(builtins, '__old_print_fun'):
        builtins.print = builtins.__old_print_fun
        del builtins.__old_print_fun


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nExecution interrupted')
        exit(2)
