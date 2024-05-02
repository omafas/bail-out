# @author Couchbase <info@couchbase.com>
# @copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import atexit
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from math import floor

import requests
import string
import random
import time
import io
import sys
import contextlib
from traceback import format_exception_only
import traceback_with_variables as traceback
from ipaddress import ip_address, IPv6Address
import os

from testlib.node import Node

def support_colors():
    return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()

config={'colors': support_colors(),
        'verbose': False,
        'screen_width': 80,
        'dry_run': False,
        'intercept_output':True}


def try_reuse_cluster(requirements, cluster):
    # Attempt to satisfy the requirements with the existing cluster if
    # possible
    satisfiable, unsatisfied = requirements.is_satisfiable(cluster)
    if len(unsatisfied) == 0:
        return True, []
    if satisfiable:
        for requirement in unsatisfied:
            with no_output("make_met"):
                print(f'Trying to fix unmet requirement: {requirement}...')
                requirement.make_met(cluster)
        # We should not have unmet requirements at this point.
        # If we do, it is a bug in make_met() or in is_met()
        if len(unmet:=requirements.get_unmet_requirements(cluster)) > 0:
            raise RuntimeError(f'Internal error. Unmet requirements: {unmet}')
        return True, []
    return False, unsatisfied


def get_appropriate_cluster(cluster, auth, requirements,
                            tmp_cluster_dir, reuse_clusters,
                            first_node_index):
    cluster_index = 0
    if cluster is not None:
        if reuse_clusters:
            reuse, _ = try_reuse_cluster(requirements, cluster)
            if reuse:
                return cluster

        # Teardown the old cluster
        cluster.teardown()

        cluster_index = cluster.index + 1
        print()

    # Create a new cluster satisfying the requirements
    print(f"=== Starting cluster#{cluster_index} to satisfy requirements: " \
          f"{requirements}")
    cluster = requirements.create_cluster(auth, cluster_index,
                                          tmp_cluster_dir,
                                          first_node_index)
    maybe_print("\n======================================="
                "=========================================\n")
    return cluster


def run_testset(testset, cluster, total_testsets_num, seed=None,
                stop_after_first_error=False):
    errors = []
    not_ran = []
    executed = 0
    print(f"\nStarting testset[{testset['#']}/{total_testsets_num}]: " \
          f"{testset['name']}...")
    maybe_print(f'Using cluster: {repr(cluster)}')

    testset_instance = testset['class'](cluster)

    log_at_all_nodes(cluster, f'starting testset {testset["name"]}')

    testset_seed = apply_with_seed(random, 'randbytes', [16],
                                   seed + str(testset['iter']))
    teardown_seed = apply_with_seed(random, 'randbytes', [16], testset_seed)

    _, err = safe_test_function_call(testset_instance, 'setup', [], 0,
                                     seed=testset_seed)

    if err is not None:
        # If testset setup fails, all tests were not ran
        for not_ran_test in testset['test_name_list']:
            not_ran.append((test_name(testset_instance, not_ran_test['name'],
                                      not_ran_test['iter']),
                            "testset setup failed"))
        return 0, [err], not_ran

    try:
        for test_dict in testset['test_name_list']:
            test = test_dict['name']
            testiter = test_dict['iter']
            test_seed = apply_with_seed(random, 'randbytes', [16],
                                        testset_seed + str(testiter).encode())
            test_teardown_seed = apply_with_seed(random, 'randbytes', [16],
                                                 test_seed)
            executed += 1
            log_at_all_nodes(cluster,
                             f'starting test {test} from {testset["name"]}')
            _, err = safe_test_function_call(testset_instance, test,
                                             [], testiter, verbose=True,
                                             seed=test_seed)
            if err is not None:
                errors.append(err)

            _, err = safe_test_function_call(testset_instance, 'test_teardown',
                                             [], testiter,
                                             seed=test_teardown_seed)
            if err is not None:
                errors.append(err)
                # Don't try to run further tests as test_teardown failure will
                # likely cause additional test failures which are irrelevant
                for not_ran_test in testset['test_name_list'][executed:]:
                    not_ran.append((test_name(testset_instance,
                                              not_ran_test['name'],
                                              not_ran_test['iter']),
                                    "Earlier test_teardown failed"))
                break

            if len(errors) > 0 and stop_after_first_error:
                for not_ran_test in testset['test_name_list'][executed:]:
                    not_ran.append((test_name(testset_instance,
                                              not_ran_test['name'],
                                              not_ran_test['iter']),
                                    "Earlier test failed"))
                break
    finally:
        _, err = safe_test_function_call(testset_instance, 'teardown',
                                         [], 0,
                                         seed=teardown_seed)
        if err is not None:
            errors.append(err)

    return executed, errors, not_ran


def test_name(testset, testname, testiter):
    iter_str = f'#{testiter+1}' if testiter != 0 else ''
    if hasattr(testset, '__name__'):
        testname = f"{testset.__name__}.{testname}{iter_str}"
    else:
        testname = f"{type(testset).__name__}.{testname}{iter_str}"
    return testname


def safe_test_function_call(testset, testfunction, args, testiter,
                            verbose=False, seed=None, dry_run=None):
    if dry_run is None:
        dry_run = config['dry_run']
    res = None
    error = None
    testname = test_name(testset, testfunction, testiter)

    report_call = call_reported(testname, verbose=verbose,
                                res_on_same_line=config['intercept_output'])
    try:
        with no_output(testname, extra_context=report_call,
                       verbose=not config['intercept_output']):
            if not dry_run:
                res = apply_with_seed(testset, testfunction, args, seed)
    except Exception as e:
        print_traceback()
        error = (testname, e)
    return res, error


def print_traceback():
    cscheme = None if config['colors'] else traceback.ColorSchemes.none
    traceback.print_exc(fmt=traceback.Format(color_scheme=cscheme),
                        file_=sys.stdout)


def apply_with_seed(obj, func, args, seed):
    try:
        rand_state = random.getstate()
        random.seed(seed)
        return getattr(obj, func)(*args)
    finally:
        random.setstate(rand_state)


def timedelta_str(start):
    delta_s = time.time() - start
    if delta_s > 10:
        return red(f" [{round(delta_s)}s]")
    if delta_s > 5:
        return red(f" [{delta_s:.1f}s]")
    elif delta_s > 1:
        return f" [{delta_s:.1f}s]"
    elif delta_s > 0.1:
        return f" [{delta_s:.2f}s]"
    else:
        return f""


def red(str):
    return maybe_color(str, 31)


def green(str):
    return maybe_color(str, 32)


def yellow(str):
    return maybe_color(str, 33)


def maybe_color(str, code):
    if config['colors']:
        return f"\033[{code}m{str}\033[0m"
    else:
        return str


class BaseTestSet(ABC):
    """BaseTestSet is the abstract base class of all TestSets

    It should be used to group together a set of related tests which
    share a set of ClusterRequirements, a setup function, a teardown
    function, and optionally a test_teardown function. The TestSet will
    be executed on a cluster satisfying its requirements, with no other
    guarantees. In order for the TestSets in a test module to be
    executed, its module must be imported in cluster_tests/run.py
    """

    def __init__(self, cluster):
        """Constructor for TestSet

        :param cluster: Cluster available to the TestSet

        If this is overriden, then super().__init__(cluster) must be
        called in the child's constructor.
        This is just for object instantiation, so it should not be used
        for anything that can just go in setup.
        """
        self.cluster = cluster

    @staticmethod
    @abstractmethod
    def requirements():
        """Return the requirements of the TestSet

        The requirements are defined as an instance of (or list of
        instances of) ClusterRequirements. These specify the criteria
        that the cluster(s) that the TestSet is executed on will be
        guaranteed to satisfy at the start of the test. If a list of
        instances of ClusterRequirements is provided, the TestSet will
        be executed on a cluster satisfying each one.
        Since we may reuse clusters (to reduce the number of clusters
        started/stopped), the TestSet author should make sure that they
        specify all pertinent requirements, such that an incompatible
        cluster will not be used.
        """
        raise NotImplementedError()

    @abstractmethod
    def setup(self):
        """Prepare the TestSet and its cluster before executing tests

        The setup function will be executed once before any of the
        TestSet's tests are executed, and it can be used to perform any
        required preparation of the cluster or anything else needed for
        the tests.
        If the setup function gives an exception, this will be treated
        as a test failure, and the tests will not be executed.
        While a TestSet could use this to assert certain details about
        the cluster, creating ClusterRequirements for these details
        should be done instead where possible. If an assert is used
        instead of a requirement, this delays resolving that
        requirement until a later date, when it will be more work to
        solve than if it had been resolved at the start.
        """
        raise NotImplementedError()

    @abstractmethod
    def teardown(self):
        """Tear down any changes that the tests made to the cluster

        The teardown function is executed once all tests in the TestSet
        have executed, and it should revert the cluster to an
        equivalent state to before the TestSet was entered.
        This may not be simple for some tests, so it is worth
        considering this before implementing the test itself, so that
        the test can be designed for easy teardown.
        If the teardown function fails to reset the cluster to the
        original state, this may manifest in another test failing only
        when executed on the same cluster. Since that failure may be
        difficult to debug, it is vital that the teardown function is
        sufficiently thorough.
        If certain edge cases cannot be reasonably handled, rather than
        just hoping they never happen, it is better to add asserts in
        teardown to confirm that they haven't happened. This should not
        be used unless it is believed that the edge case should never
        occur, and handling it would be a significant amount of work.
        """
        raise NotImplementedError()

    def test_teardown(self):
        """(optional) Tear down an individual test in the TestSet

        This will be executed after every test in the TestSet, so it
        should be used to execute any common cleanup for each test.
        If it gives an exception, it will count as a test failure and
        none of the other tests in the TestSet will execute, so asserts
        may be added to confirm that the cluster is still in a state
        where there is a point in running subsequent tests.
        """
        pass


def delete_config_key(cluster, key):
    return post_succ(cluster, '/diag/eval', data=f'ns_config:delete({key})')


def set_config_key(cluster, key, value):
    if type(value) == str:
        value_str = f'"{value}"'
    else:
        value_str = str(value)
    return diag_eval(cluster, f'ns_config:set({key}, {value_str}).')


def request(method, cluster_or_node, path, expected_code=None, https=False,
            verbose=True, **kwargs):
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 60
    kwargs_with_auth = set_default_auth(cluster_or_node, **kwargs)
    if isinstance(cluster_or_node, Node):
        node = cluster_or_node
    else:
        node = cluster_or_node.connected_nodes[0]

    if https:
        url = node.https_url() + path
        if 'verify' not in kwargs_with_auth:
            server_ca_file = os.path.join(node.data_path(),
                                          'config', 'certs', 'ca.pem')
            kwargs_with_auth['verify'] = server_ca_file
    else:
        url = node.url + path

    if verbose:
        print(f'sending {method} {url} {kwargs} ' \
              f'(expected code {expected_code})')
    res = requests.request(method, url, **kwargs_with_auth)
    if verbose:
        print(f'result: {res.status_code}')
    if expected_code is not None:
        assert_http_code(expected_code, res),
    return res


def put_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('PUT', cluster_or_node, path, expected_code, **kwargs)


def patch_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('PATCH', cluster_or_node, path, expected_code, **kwargs)


def post_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('POST', cluster_or_node, path, expected_code, **kwargs)


def post_fail(cluster_or_node, path, expected_code, **kwargs):
    return request('POST', cluster_or_node, path, expected_code, **kwargs)


def patch_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('PATCH', cluster_or_node, path, expected_code, **kwargs)


def patch_fail(cluster_or_node, path, expected_code=200, **kwargs):
    return request('PATCH', cluster_or_node, path, expected_code, **kwargs)


def post(cluster_or_node, path, **kwargs):
    return request('POST', cluster_or_node, path, None, **kwargs)


def get_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('GET', cluster_or_node, path, expected_code, **kwargs)


def get_fail(cluster_or_node, path, expected_code, **kwargs):
    return request('GET', cluster_or_node, path, expected_code, **kwargs)


def get(cluster_or_node, path, **kwargs):
    return request('GET', cluster_or_node, path, None, **kwargs)


def ensure_deleted(cluster, path, expected_codes=None, **kwargs):
    if expected_codes is None:
        expected_codes = [200, 404]
    res = delete(cluster, path, **kwargs)
    code = res.status_code
    assert code in expected_codes, format_http_error(res, expected_codes)
    return res


def delete(cluster_or_node, path, **kwargs):
    return request('DELETE', cluster_or_node, path, None, **kwargs)


def delete_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('DELETE', cluster_or_node, path, expected_code, **kwargs)


def set_default_auth(cluster_or_node, **kwargs):
    if 'auth' not in kwargs and 'cert' not in kwargs:
        new_kwargs = kwargs.copy()
        new_kwargs.update({'auth': cluster_or_node.auth})
        return new_kwargs
    return kwargs


def assert_http_code(expected_code, res):
    code = res.status_code
    assert code == expected_code, format_http_error(res, [expected_code])


def format_http_error(res, expected_codes):
    expected_codes_str = " or ".join([str(c) for c in expected_codes])
    return f"{res.request.method} {res.url} " \
           f"returned {res.status_code} {res.reason} " \
           f"(expected {expected_codes_str})" \
           f", response body: {res.text}"


def assert_json_key(expected_key, json, context):
    assert expected_key in json.keys(), \
        f"({context}) '{expected_key}' missing in json: {json}"
    return json[expected_key]


def assert_eq(got, expected, name='value'):
    assert expected == got, f'unexpected {name}: {got}, expected: {expected}'


def assert_gt(got, lower_bound, name='value'):
    assert got > lower_bound, \
        f'unexpected {name}: {got}, expected: > {lower_bound}'


def assert_in(what, where):
    assert what in where, f'"{what}" is missing in "{where}"'


def assert_not_in(what, where):
    assert what not in where, f'"{what}" is in "{where}"'


def random_str(n):
    return ''.join(random.choices(string.ascii_lowercase +
                                  string.digits, k=n))


def json_response(response, error):
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        assert False, error


def delete_all_buckets(cluster, **kwargs):
    buckets = get_succ(cluster, "/pools/default/buckets", **kwargs)
    for bucket in buckets.json():
        cluster.delete_bucket(bucket['name'])


def get_otp_nodes(cluster):
    info = json_response(get(cluster, "/nodeStatuses"),
                         "/nodeStatuses response was not json")
    return {k: info[k]['otpNode'] for k in info}

def poll_for_condition(fun, sleep_time, attempts=None, timeout=None,
                       verbose=False, msg="poll for condition",
                       retry_value=False):

    assert (attempts is not None) or (timeout is not None)
    assert sleep_time > 0, "non-positive sleep_time specified"
    start_time = time.time()
    sleep_time_str = f"{sleep_time:.2f}s"

    attempt_count = 0
    while (attempts is None) or (attempt_count < attempts):
        if timeout is not None:
            assert (time.time() - start_time) < timeout, \
                   f"{msg}: timed-out (timeout: {timeout}s)"
        value = fun()
        if value is not retry_value:
            maybe_print(f"Time taken for condition to complete: "
                        f"{time.time() - start_time: .2f}s", verbose=verbose)
            return value
        maybe_print(f"Sleeping for {sleep_time_str}", verbose=verbose)
        time.sleep(sleep_time)
        attempt_count += 1
    assert False, f"{msg} didn't complete in: {attempts} attempts, " \
                  f"sleep_time: {sleep_time_str}"

def metakv_get_succ(cluster, key, **kwargs):
    return get_succ(cluster, f"/_metakv{key}", **kwargs)

def metakv_get(cluster, key, **kwargs):
    return get(cluster, f"/_metakv{key}", **kwargs)

def metakv_put_succ(cluster, key, value, **kwargs):
    return put_succ(cluster, f"/_metakv{key}", data={'value': value}, **kwargs)

def metakv_delete_succ(cluster, key, **kwargs):
    return delete_succ(cluster, f"/_metakv{key}", **kwargs)

def diag_eval(cluster, code, **kwargs):
    return post_succ(cluster, '/diag/eval', data=code, **kwargs)


@contextlib.contextmanager
def no_output(name, verbose=None, extra_context=contextlib.nullcontext()):
    """
    Executes context body with all the output redirected to a string.
    If something crashes, it prints that output, otherwise it ignores it.
    If verbose is true, it doesn't redirect anything.
    If extra_context is provided, the body is executed in that context, but the
    extra_context output is not redirected. The main purpose of
    the extra_context param, is to have an ability to print something (the
    result of execution) before this function starts dumping the redirected
    output (in case of a crash)
    """
    if verbose is None:
        verbose = config['verbose']

    if verbose:
        with extra_context:
            yield
        return

    f = io.StringIO()
    try:
        with extra_context:
            with contextlib.redirect_stdout(f):
                yield
    except Exception as e:
        output = f.getvalue()
        if len(output) > 0:
            extra_cr = '\n' if output[-1] != '\n' else ''
            print(
                f"================== {name} output begin =================\n"
                f"{output}{extra_cr}"
                f"=================== {name} output end ==================\n")

        raise e


@contextlib.contextmanager
def call_reported(name, succ_str="ok", fail_str="failed", verbose=False,
                  res_on_same_line=True):
    """
    Executes context body and reports result in the following format:
      <name>...           <succ_str> [<time_taken>]
    or
      <name>...           <fail_str> [<time_taken>]
    if context body throws exception.
    If verbose is false, prints only unsuccessful result in slightly different
    format.
    If res_on_same_line is false, puts result on the next line.
    """

    start = time.time()
    try:
        str_to_print = f"  {name}... " + ('\n' if not res_on_same_line else '')
        width_taken = len(str_to_print)
        if verbose:
            print(str_to_print, end='', flush=True)
        yield
        if verbose:
            if res_on_same_line:
                res = right_aligned(succ_str, taken=width_taken)
            else:
                res = succ_str
            print(green(res) + timedelta_str(start))
    except Exception as e:
        short_exception = red('\n'.join(format_exception_only(type(e), e))
                              .strip('\n'))
        if verbose:
            if res_on_same_line:
                res = right_aligned(fail_str, taken=width_taken)
            else:
                res = fail_str
            print(red(res) + timedelta_str(start))
            print(f'    {short_exception}')
        else:
            print(red(f"{name} {fail_str} ({short_exception})"))
        raise e


def right_aligned(s, taken=0, width=config['screen_width']):
    corrected_width = max(0, width - taken)
    return f'{s: >{corrected_width}}'


def no_output_decorator(f):
    def wrapped_f(*args, **kwargs):
        with no_output(f.__name__):
            return f(*args, **kwargs)
    return wrapped_f


def maybe_print(s, verbose=None, print_fun=print):
    if verbose is None:
        verbose = config['verbose']
    if verbose:
        print_fun(s)

def log_at_all_nodes(cluster, msg):
    for n in cluster._nodes:
        diag_eval(n, f'\"{msg}\".', verbose=config['verbose'])


def maybe_add_brackets(addr):
    if addr[0] == '[':
        return addr
    try:
        if type(ip_address(addr)) is IPv6Address:
            return f'[{addr}]'
        else:
            return addr
    except ValueError:
        # addr is fqdn
        return addr


# Rebalance has finished, but ejected nodes can still
# (a) think they are part of the cluster
# (b) be starting web server after leaving
# Here we wait for one ejected node to leave the cluster and
# start web server.
def wait_for_ejected_node(ejected_node):
    def ejected_node_is_up(node):
        try:
            resp = get(node, '/pools/default')
            return 404 == resp.status_code and \
                   '"unknown pool"' == resp.text
        except Exception as e:
            print(f'got exception: {e}')
            return False

    poll_for_condition(
        lambda: ejected_node_is_up(ejected_node),
        sleep_time=1, timeout=60,
        msg=f'wait for ejected node {ejected_node} to be up')


class UnmetRequirementsError(Exception):
    def __init__(self, unmet_requirements,
                 message='Cluster doesn\'t satisfy requirements'):
        unmet_str = ', '.join(str(r) for r in unmet_requirements)
        msg = f'{message}: {unmet_str}'
        super().__init__(msg)
        self.unmet_requirements = unmet_requirements


def toggle_client_cert_auth(node, enabled=True, mandatory=True, prefixes=None):
    state = 'disable'
    if enabled:
        state = 'mandatory' if mandatory else 'enable'
    if prefixes is None:
        prefixes = [{'delimiter': '',
                     'path': 'subject.cn',
                     'prefix': ''}]
    r = post(node, '/settings/clientCertAuth',
             json={'prefixes': prefixes,
                   'state': state})
    # Node: It actually affects whole cluster (if node is part of a cluster)
    #       If 202 is returned, it means that the configuration is written to
    #       configu but it is not guaranteed that all web server (at all nodes)
    #       have applied new settings.
    #       At the same time, it is guaranteed that https server on 'node'
    #       has applied new settings
    expected = [200, 202]
    assert r.status_code in expected, format_http_error(r, expected)


def start_log_collection(node, **kwargs):
    # Start log collection for a node. Only triggers it for that node, not the
    # whole cluster, as we want to avoid OOM issues as all nodes are on the same
    # machine
    print(f"Collecting logs from {node}...")
    post_succ(node, "/controller/startLogsCollection",
              data={"nodes": node.otp_node(), **kwargs})


def wait_for_log_collection(node, start_time):
    # Wait until log collection is complete
    return poll_for_condition(
        lambda: log_collection_complete(node, start_time),
        sleep_time=1, timeout=600)


def log_collection_complete(node, start_time):
    tasks = get_succ(node, "/pools/default/tasks",
                     verbose=config['verbose']).json()
    for task in tasks:
        timestamp = task.get("ts")
        if (task.get("type") == "clusterLogsCollection" and
                timestamp is not None):
            #
            if (datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                        .timestamp() >= start_time):
                per_node = task.get('perNode').get(node.otp_node())
                if (per_node is not None and
                        per_node.get("status") == "collected"):
                    return per_node.get('path')
    return False
