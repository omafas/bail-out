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
import time
import shutil
import inspect
import atexit

scriptdir = os.path.dirname(os.path.realpath(__file__))
pylib = os.path.join(scriptdir, "..", "pylib")
sys.path.append(pylib)

import cluster_run_lib
import testlib
import dummy_test

tmp_cluster_dir = os.path.join(scriptdir, "test_cluster_data")

USAGE_STRING = """
Usage: {program_name}
    [--start-server | -s]
        Start its own couchbase-server for tests or will try connect to
        127.0.0.1:9000 by default. Note that default port can be changed by
        --start-index option.
    [--start-index <N>]
        Index of existing cluster_run node to connect to. Default: 0
    [--user | -u <admin>]
        Username to be used when connecting to an existing cluster.
        Mutually exclusive to --start-server. Default: Administrator
    [--password | -p <admin_password>]
        Password to be used when connecting to an existing cluster.
        Mutually exclusive to --start-server. Default: asdasd
    [--tests | -t <test_spec>[, <test_spec> ...]]
        <test_spec> := <test_class>[.test_name]
        Start only specified tests
    [--help]
        Show this help
"""

def usage():
    print(USAGE_STRING.format(program_name=sys.argv[0]))

def error_exit(msg):
    print(msg)
    usage()
    sys.exit(2)

def main():
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], "hsu:p:t:",
                                          ["help", "start-server",
                                           "user=", "password=",
                                           "start-index=","tests="])
    except getopt.GetoptError as err:
        error_exit(str(err))

    start_server = False
    username = 'Administrator'
    password = 'asdasd'
    start_index = 0
    tests = None

    for o, a in optlist:
        if o in ('--start-server', '-s'):
            start_server = True
        elif o in ('--user', '-u'):
            if start_server == True:
                error_exit(f"{o} is not supported when test cluster is started")
            username = a
        elif o in ('--password', '-p'):
            if start_server == True:
                error_exit(f"{o} is not supported when test cluster is started")
            password = a
        elif o == '--start-index':
            start_index = int(a)
        elif o in ('--tests','-t'):
            tests = []
            for tokens in [t.strip().split(".") for t in a.split(",")]:
                if len(tokens) == 1:
                    tests.append((tokens[0], '*'))
                elif len(tokens) == 2:
                    tests.append((tokens[0], tokens[1]))
        elif o in ('--help', '-h'):
            usage()
            exit(0)
        else:
            assert False, f"unhandled options: {o}"

    clusters = []
    processes = []

    if start_server:
        if os.path.isdir(tmp_cluster_dir):
            print(f"Removing cluster dir {tmp_cluster_dir}...")
            shutil.rmtree(tmp_cluster_dir)
        print("Starting couchbase server...")
        processes = cluster_run_lib.start_cluster(num_nodes=1,
                                                  start_index=start_index,
                                                  root_dir=tmp_cluster_dir,
                                                  wait_for_start=True,
                                                  nooutput=True)
        cluster_run_lib.connect(num_nodes=1,
                                start_index=start_index,
                                do_rebalance=False)

    url = f"http://127.0.0.1:{cluster_run_lib.base_api_port + start_index}"
    clusters.append(testlib.Cluster(urls=[url],
                                    processes=processes,
                                    auth=(username, password)))

    print(f"Available cluster configurations: {clusters}")

    discovered_tests = discover_testsets()

    print(f"Discovered testsets: {[c for c, _, _ in discovered_tests]}")

    testsets_to_run = []
    if tests is None:
        testsets_to_run = discovered_tests
    else:
        testsets_to_run = find_tests(tests, discovered_tests)

    errors = {}
    executed = 0
    for _, testset, test_names in testsets_to_run:
        res = testlib.run_testset(testset, test_names, clusters)
        executed += res[0]
        testset_errors = res[1]
        if len(testset_errors) > 0:
            errors[testset.__name__] = testset_errors

    error_num = sum([len(errors[name]) for name in errors])
    errors_str = "1 error" if error_num == 1 else f"{error_num} errors"

    print("\n======================================="\
          "=========================================\n"\
          f"Tests finished ({executed} executed, {errors_str})")

    for name in errors:
        print(f"In {name}:")
        for testres in errors[name]:
            print(f"  {testres[0]} failed: {testres[1]}")
    print()

    terminal_attrs = None

    try:
        import termios
        terminal_attrs = termios.tcgetattr(sys.stdin)
    except Exception:
        pass

    def kill_nodes():
        for c in clusters:
            cluster_run_lib.kill_nodes(c.processes, terminal_attrs)

    atexit.register(kill_nodes)

    if len(errors) > 0:
        sys.exit("Tests finished with errors")


def find_tests(test_names, discovered_list):
    results = {}
    discovered_dict = {n: (c, t) for n, c, t in discovered_list}
    for class_name, test_name in test_names:
        assert class_name in discovered_dict, \
            f"Testset {class_name} is not found. "\
            f"Available testsets: {list(discovered_dict.keys())}"
        testset, tests = discovered_dict[class_name]
        if test_name == '*':
            results[class_name] = (testset, tests)
        else:
            assert test_name in tests, \
                f"Test {test_name} is not found in {class_name}. "\
                f"Available tests: {tests})"

            if class_name in results:
                testlist = results[class_name][1]
                testlist.append(test_name)
                results[class_name] = (results[class_name][0], testlist)
            else:
                results[class_name] = (testset, [test_name])

    return [(k, results[k][0], results[k][1]) for k in results]


def discover_testsets():
    testsets = []

    for m in sys.modules.keys():
        if not hasattr(sys.modules[m], '__file__'):
            continue
        if sys.modules[m].__file__ is None:
            continue
        if scriptdir != os.path.dirname(sys.modules[m].__file__):
            continue
        for name, testset in inspect.getmembers(sys.modules[m], inspect.isclass):
            if testset == testlib.BaseTestSet:
                continue
            if issubclass(testset, testlib.BaseTestSet):
                tests = [m for m in dir(testset) if m.endswith('_test')]
                testsets.append((name, testset, tests))

    return testsets

if __name__ == '__main__':
    main()
