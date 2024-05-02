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
import atexit
from datetime import datetime, timezone
from math import floor

import requests
import glob
import time
import random
import pprint
from copy import deepcopy

# Pretty prints any tracebacks that may be generated if the process dies
from traceback_with_variables import activate_by_import

scriptdir = sys.path[0]
pylib = os.path.join(scriptdir, "..", "pylib")
sys.path.append(pylib)

import cluster_run_lib
import testlib
from testlib import UnmetRequirementsError
from testlib.cluster import InconsistentClusterError

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
    rest_eject_test

tmp_cluster_dir = os.path.join(scriptdir, "test_cluster_data")

USAGE_STRING = """
Usage: {program_name}
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
        failed test.
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
    print(testlib.red(msg))
    sys.exit(2)


def warning_exit(msg):
    print(testlib.yellow(msg))
    sys.exit(3)


def remove_temp_cluster_directories():
    for dir in glob.glob(tmp_cluster_dir + "*"):
        testlib.maybe_print(f"Removing cluster dir {dir}...")
        shutil.rmtree(dir)


def kill_nodes(processes, urls, terminal_attrs):
    with testlib.no_output("kill nodes"):
        cluster_run_lib.kill_nodes(processes, terminal_attrs, urls)


def main():
    # we use assert statements in tests, so make sure they are not disabled
    if not __debug__:
        raise RuntimeError("Assert statements are disabled")
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], "hkovc:u:p:n:t:s:",
                                          ["help", "keep-tmp-dirs", "cluster=",
                                           "user=", "password=", "num-nodes=",
                                           "tests=", "dont-intercept-output",
                                           "seed=", "colors=", "verbose",
                                           "dry-run", 'dont-reuse-clusters',
                                           'randomize-clusters',
                                           'random-order',
                                           'testset-iterations=',
                                           'start-index=',
                                           'test-iterations=',
                                           'stop-after-error',
                                           'collect-logs-after-error'])
    except getopt.GetoptError as err:
        bad_args_exit(str(err))

    use_existing_server = False
    username = 'Administrator'
    password = 'asdasd'
    num_nodes = None
    address = '127.0.0.1'
    start_port = cluster_run_lib.base_api_port
    tests = None
    keep_tmp_dirs = False
    seed = testlib.random_str(16)
    reuse_clusters = True
    randomize_clusters = False
    random_order = False
    testset_iterations = 1
    test_iterations = 1
    start_index = 10
    stop_after_first_error = False
    collect_logs = False

    for o, a in optlist:
        if o in ('--cluster', '-c'):
            tokens = a.split(':')
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
        elif o in ('--keep-tmp-dirs', '-k'):
            keep_tmp_dirs = True
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
        elif o in ('--help', '-h'):
            usage()
            exit(0)
        else:
            assert False, f"unhandled options: {o}"

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

    if tests is None:
        testsets_to_run = discovered_tests
    else:
        testsets_to_run = find_tests(tests, discovered_tests)

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
                not_ran.append((testset['name'], "prior testset failed"))
            continue
        # Get an appropriate cluster to satisfy the configuration
        if use_existing_server:
            unmet_requirements = configuration.get_unmet_requirements(cluster)
            if len(unmet_requirements) > 0:
                for testset in testsets:
                    reason = "Cluster provided does not satisfy test " \
                             f"requirements:\n" \
                             f"{[str(r) for r in unmet_requirements]}"
                    not_ran.append((testset['name'], reason))
                continue
            cluster.set_requirements(configuration)
        else:
            cluster = testlib.get_appropriate_cluster(cluster,
                                                      (username, password),
                                                      configuration,
                                                      tmp_cluster_dir,
                                                      kill_nodes,
                                                      reuse_clusters,
                                                      start_index)
        testset_start_ts = time.time_ns()
        # Run the testsets on the cluster
        tests_executed, testset_errors, testset_not_ran, log_collection_time = \
            run_testsets(cluster, testsets, total_num,
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

    print("\n======================================="
          "=========================================\n" +
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
            print(f"  {testres[0]} failed: {testres[1]}")
        print()

    if len(not_ran) > 0:
        print(f"Couldn't run the following tests:")
        for name, reason in not_ran:
            print(f"  {name}: {reason}")
        print()

    if len(errors) > 0:
        error_exit("Tests finished with errors")
    elif len(not_ran) > 0:
        warning_exit("Some tests were skipped")
    elif not (keep_tmp_dirs or check_for_core_files()):
        # Kill any created nodes and possibly delete directories as we don't
        # need to keep around data from successful tests
        cluster.teardown()
        remove_temp_cluster_directories()
        # Unregister the kill nodes atexit handler as the nodes are now down
        atexit.unregister(kill_nodes)


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
                testset_name = f"{class_name}{iter_str}/{requirements}"
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


def find_tests(test_names, discovered_list):
    results = {}
    discovered_dict = {n: (cl, t, cf) for n, cl, t, cf in discovered_list}
    for class_name, test_name in test_names:
        assert class_name in discovered_dict, \
            f"Testset {class_name} is not found. "\
            f"Available testsets: {list(discovered_dict.keys())}"
        testset, tests, configurations = discovered_dict[class_name]
        if test_name == '*':
            results[class_name] = (testset, tests, configurations)
        else:
            assert test_name in tests, \
                f"Test {test_name} is not found in {class_name}. "\
                f"Available tests: {tests})"

            if class_name in results:
                testlist = results[class_name][1]
                testlist.append(test_name)
                results[class_name] = (results[class_name][0], testlist,
                                       configurations)
            else:
                results[class_name] = (testset, [test_name], configurations)

    return [(k, results[k][0], results[k][1], results[k][2]) for k in results]


def discover_testsets():
    testsets = []

    def add_testset(testset_name, testset_class, configuration):
        tests = [test for test in dir(testset) if test.endswith('_test')]
        if len(tests) > 0:
            testsets.append((testset_name, testset_class, tests, configuration))

    for m in sys.modules.keys():
        if not hasattr(sys.modules[m], '__file__'):
            continue
        if sys.modules[m].__file__ is None:
            continue
        if os.path.join(scriptdir, "testsets") != \
                os.path.dirname(sys.modules[m].__file__):
            continue
        for name, testset in inspect.getmembers(sys.modules[m], inspect.isclass):
            if testset == testlib.BaseTestSet:
                continue
            if issubclass(testset, testlib.BaseTestSet):
                requirements, err = testlib.safe_test_function_call(
                    testset, 'requirements', [], 0, dry_run=False)
                if err is not None:
                    name, req_error = err
                    return [(name, None, None, req_error)]
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
        return testlib.cluster.get_cluster(0, start_port, auth, [], nodes)


# Run each testset on the same cluster, counting how many individual tests were
# ran, and keeping track of all errors
def run_testsets(cluster, testsets, total_num, seed=None,
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
                not_ran.append((testlib.test_name(testset['class'],
                                                  skip_test['name'],
                                                  skip_test['iter']),
                                error))

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
        reuse, unmet = testlib.try_reuse_cluster(testset['requirements'],
                                                 cluster)

        if not reuse:
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
        # We use a catch-all here because if we hit any errors while running
        # a testset, but not in a test, then it is likely an issue with the
        # cluster, so we should keep running more tests
        except Exception as e:
            testlib.print_traceback()
            testset_errors.append((testset["name"], e))
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
                testset_errors.append((testset["name"], test_error))
        except InconsistentClusterError as e:
            print(testlib.red(str(e)))
            testset_errors.append((testset["name"], e))
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
                    testlib.wait_for_ejected_node(node)

                start_time = floor(datetime.now(timezone.utc).timestamp())
                testlib.start_log_collection(
                    node,
                    taskRegexp=".*diags"
                               "|master events"
                               "|memcached.*"
                               "|Collecting .*/snapshots"
                               "|Chronicle dump"
                )

                path = testlib.wait_for_log_collection(node, start_time)
                print(f"Collected logs for {node.url}: {path}")

        log_collection_time += (time.time_ns() - collect_start_time)
    return executed, errors, not_ran, log_collection_time


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nExecution interrupted')
        exit(2)
