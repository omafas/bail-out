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
from prometheus_client.parser import text_string_to_metric_families

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
import signal
from types import MethodType
from dataclasses import dataclass, field
from testlib.node import Node
import shutil

THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
NS_SERVER_DIR = os.path.join(THIS_FILE_DIR, '..', '..')

def support_colors():
    return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()

def get_terminal_width():
    try:
        columns, _ = shutil.get_terminal_size(fallback=(100, 24))
        return min(300, columns)
    except:
        return 100

config={'colors': support_colors(),
        'verbose': False,
        'screen_width': get_terminal_width(),
        'align_res': True,
        'wrap_output': True,
        'dry_run': False,
        'intercept_output': True,
        'keep_tmp_dirs': False,
        'report_time': True,
        'report_time_format': '%H:%M:%S',
        'test_timeout': 600}

@dataclass
class TestError:
    name: str
    error: Exception
    cluster_name: str
    timestamp: datetime = field(default_factory=datetime.now)

    def __str__(self):
        return f'[{self.timestamp.strftime("%H:%M:%S")}] {self.cluster_name} ' \
               f'{self.name}: {self.error}'


def get_appropriate_cluster(cluster, auth, requirements,
                            tmp_cluster_dir, reuse_clusters,
                            first_node_index):
    cluster_index = 0
    if cluster is not None:
        if reuse_clusters:
            cluster.update_requirements(requirements)
            unmet = cluster.maybe_repair_cluster_requirements()
            if len(unmet) == 0:
                return cluster

        # Teardown the old cluster
        cluster.destroy()

        cluster_index = cluster.index + 1
        print()

    # Create a new cluster satisfying the requirements
    print_wrapped(f"=== Starting cluster#{cluster_index} to satisfy " \
                  f"requirements: {requirements}",
                  max_width=config['screen_width'])
    cluster = requirements.create_cluster(auth, cluster_index,
                                          tmp_cluster_dir,
                                          first_node_index)
    maybe_print("\n" + "=" * config['screen_width'] + "\n")
    return cluster


def run_testset(testset, cluster, total_testsets_num, seed=None,
                stop_after_first_error=False):
    errors = []
    not_ran = []
    executed = 0
    print_wrapped(f"\nStarting testset[{testset['#']}/{total_testsets_num}]: " \
                  f"{testset['name']}",
                  max_width=config['screen_width'])
    maybe_print(f'Using cluster: {repr(cluster)}')

    testset_instance = testset['class'](cluster)

    errors += log_at_all_nodes(cluster, f'starting testset {testset["name"]}')

    testset_seed = apply_with_seed(random, 'randbytes', [16],
                                   seed + str(testset['iter']))
    teardown_seed = apply_with_seed(random, 'randbytes', [16], testset_seed)

    test_seed = lambda i: apply_with_seed(random, 'randbytes', [16],
                                          testset_seed + str(i).encode())

    _, err, _ = safe_test_function_call(testset_instance, 'setup', [], 0,
                                        report_name=True,
                                        seed=testset_seed)

    if err is not None:
        # If testset setup fails, all tests were not ran
        for not_ran_test in testset['test_name_list']:
            not_ran.append(TestError(
                name=test_name(testset_instance, not_ran_test['name'],
                               not_ran_test['iter']),
                error=RuntimeError("testset setup failed"),
                cluster_name=cluster.short_name()))
        return 0, [err], not_ran, cluster

    try:
        tests_to_run = []
        for test_dict in testset['test_name_list']:
            if test_dict['name'].endswith('_test_gen'):
                generated, err, _ = safe_test_function_call(
                                      testset_instance,
                                      test_dict['name'],
                                      [], test_dict['iter'],
                                      report_name=True,
                                      seed=test_seed(test_dict['iter']))
                if generated is None:
                    generated = [] # happens when --dry-run is used
                if err is not None:
                    errors.append(err)
                    break
                maybe_print(f'generated {len(generated)} tests: {generated}')
                for n in generated:
                    tests_to_run.append({'name': n,
                                         'iter': test_dict['iter'],
                                         'fun': generated[n]})
            else:
                tests_to_run.append(test_dict)

        for test_dict in tests_to_run:
            test = test_dict['name']
            testiter = test_dict['iter']
            test_teardown_seed = apply_with_seed(random, 'randbytes', [16],
                                                 test_seed(testiter))
            executed += 1
            errors += log_at_all_nodes(
                        cluster,
                        f'starting test {test} from {testset["name"]}')

            if 'fun' in test_dict: # this test is generated
                if hasattr(testset_instance, test):
                    # testset already has this test, skipping...
                    not_ran.append(TestError(
                        name=test_name(testset_instance,
                                       test,
                                       testiter),
                        error=RuntimeError("test already exists"),
                        cluster_name=cluster.short_name()))
                    break
                setattr(testset_instance, test,
                        MethodType(test_dict['fun'], testset_instance))

            _, err, tdown_err = safe_test_function_call(
                                  testset_instance, test, [], testiter,
                                  teardown_function='test_teardown',
                                  teardown_seed=test_teardown_seed,
                                  report_name=True,
                                  seed=test_seed(testiter))

            if 'fun' in test_dict: # this test is generated
                delattr(testset_instance, test)

            cluster = testset_instance.cluster

            if err is not None:
                errors.append(err)

            if tdown_err is not None:
                errors.append(tdown_err)
                # Don't try to run further tests as test_teardown failure will
                # likely cause additional test failures which are irrelevant
                for not_ran_test in tests_to_run[executed:]:
                    not_ran.append(TestError(
                        name=test_name(testset_instance,
                                       not_ran_test['name'],
                                       not_ran_test['iter']),
                        error=RuntimeError("Earlier test_teardown failed"),
                        cluster_name=cluster.short_name()))
                break

            if len(errors) > 0 and stop_after_first_error:
                for not_ran_test in tests_to_run[executed:]:
                    not_ran.append(TestError(
                        name=test_name(testset_instance,
                                       not_ran_test['name'],
                                       not_ran_test['iter']),
                        error=RuntimeError("Earlier test failed"),
                        cluster_name=cluster.short_name()))
                break
    finally:
        _, err, _ = safe_test_function_call(testset_instance, 'teardown',
                                            [], 0,
                                            report_name=True,
                                            seed=teardown_seed)
        if err is not None:
            errors.append(err)

    return executed, errors, not_ran, cluster


def test_name(testset, testname, testiter, short_form=False):
    iter_str = f'#{testiter+1}' if testiter != 0 else ''
    prefix = ''
    if not short_form:
        if hasattr(testset, '__name__'):
            prefix = f'{testset.__name__}.'
        else:
            prefix = f'{type(testset).__name__}.'
    return f'{prefix}{testname}{iter_str}'


def safe_test_function_call(testset, testfunction, args, testiter,
                            teardown_function=None, teardown_seed=None,
                            report_name=False, seed=None, dry_run=None,
                            timeout=None):
    if timeout is None:
        timeout = config['test_timeout']
    if dry_run is None:
        dry_run = config['dry_run']
    res = None
    error = None
    teardown_error = None
    testname = test_name(testset, testfunction, testiter)
    short_testname = test_name(testset, testfunction, testiter,
                                short_form=True)

    def call(n, f, args, seed, err_callback):
        with no_output(n, verbose=not config['intercept_output'],
                       error_callback=err_callback):
            if not dry_run:
                return apply_with_seed(testset, f, args, seed)
            return None

    def timeout_handler(snum, frame):
        print(f'{testname} timed out (timeout: {timeout}s)')
        raise TimeoutError('timed out')

    if timeout is not None:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)

    test_successful = False
    end_report = start_report(testname, short_testname, report_name=report_name,
                              single_line=config['intercept_output'])
    start_time = time.time()
    test_error_report = lambda e: end_report(e, None, time.time() - start_time,
                                             None)

    try:
        res = call(testname, testfunction, args, seed, test_error_report)

        finish_time = time.time()
        test_successful = True
        teardown_error_report = \
            lambda e: end_report(None, e, time.time() - start_time, None)

        if teardown_function is not None:
            call(teardown_function, teardown_function, [], teardown_seed,
                 teardown_error_report)

        end_report(None, None, finish_time - start_time,
                   time.time() - finish_time)
    except Exception as e:
        print_traceback()
        if hasattr(testset, 'cluster'):
            cluster_name = testset.cluster.short_name()
        else:
            cluster_name = "(no cluster)"
        if test_successful: # this is actually a teardown exception
            teardown_error = TestError(name=testname + ' (teardown)',
                                       error=e,
                                       cluster_name=cluster_name)

        else: # this is a test exception, we need to run teardown
            error = TestError(name=testname,
                              error=e,
                              cluster_name=cluster_name)

            if teardown_function is not None:
                try:
                    error_callback = \
                        lambda e: print(f'{teardown_function} failed: {e}')
                    call(teardown_function, teardown_function, [],
                         teardown_seed, error_callback)
                except Exception as e2:
                    print_traceback()
                    teardown_error = TestError(name=testname + ' (teardown)',
                                               error=e2,
                                               cluster_name=cluster_name)
    finally:
        if timeout is not None:
            signal.alarm(0)
    return res, error, teardown_error


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


def format_test_times(test_time, teardown_time, display_threshold=0.5):
    if teardown_time is None:
        total_time = test_time
    else:
        total_time = test_time + teardown_time

    if total_time < display_threshold:
        return ""

    base_res = timedelta_str(total_time)

    if teardown_time is None or teardown_time < display_threshold:
        return " " + base_res

    return " " + base_res + " (" + timedelta_str(teardown_time) + " td)"


def timedelta_str(delta_s):
    # 9.95 is used to avoid seeing 10.0s for values between 9.95 and 10.0,
    # we should see 10s instead
    if delta_s > 9.95:
        return red(f"{round(delta_s)}s")
    if delta_s > 5:
        return red(f"{delta_s:.1f}s")
    return f"{delta_s:.1f}s"


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
    if isinstance(value, bool):
        value_str = "true" if value else "false"
    elif isinstance(value, str):
        value_str = f'"{value}"'
    else:
        value_str = str(value)
    return diag_eval(cluster, f'ns_config:set({key}, {value_str}).')


def request(method, cluster_or_node, path, https=False, session=None,
            service=None, **kwargs):
    kwargs_with_auth = set_default_auth(cluster_or_node, **kwargs)
    if isinstance(cluster_or_node, Node):
        node = cluster_or_node
    else:
        node = cluster_or_node.get_available_cluster_node()

    if https:
        url = node.https_service_url(service) + path
        if 'verify' not in kwargs_with_auth:
            server_ca_file = os.path.join(node.data_path(),
                                          'config', 'certs', 'ca.pem')
            kwargs_with_auth['verify'] = server_ca_file
    else:
        url = node.service_url(service) + path

    print_session = True
    if session is None:
        session = node.get_default_session()
        print_session = False

    return http_request(method, url, session=session,
                        print_session=print_session, **kwargs_with_auth)

def http_request(method, url, expected_code=None, verbose=True, session=None,
                 print_session=True, **kwargs):
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 60

    session_str = ''
    if session is not None and print_session:
        session_str = f'(session {id(session)}) '

    if verbose:
        print(f'sending {session_str}{method} {url} {kwargs} ' \
              f'(expected code {expected_code})')
    if session is not None:
        res = session.request(method, url, **kwargs)
    else:
        res = requests.request(method, url, **kwargs)
    if verbose:
        text = ''
        if hasattr(res, 'text') and res.text is not None:
            max_len = 40
            if len(res.text) > max_len:
                text = f' {res.text[0:max_len]}...'
            else:
                text = f' {res.text}'
        print(f'result: {res.status_code}{text}')
    if expected_code is not None:
        assert_http_code(expected_code, res),
    return res


def put_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('PUT', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)

def put_fail(cluster_or_node, path, expected_code, **kwargs):
    return request('PUT', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)

def put_fail(cluster_or_node, path, expected_code, **kwargs):
    return request('PUT', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)

def patch_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('PATCH', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)


def post_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('POST', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)


def post_fail(cluster_or_node, path, expected_code, **kwargs):
    return request('POST', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)


def patch_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('PATCH', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)


def patch_fail(cluster_or_node, path, expected_code, **kwargs):
    return request('PATCH', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)


def post(cluster_or_node, path, **kwargs):
    return request('POST', cluster_or_node, path, **kwargs)


def get_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('GET', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)


def get_fail(cluster_or_node, path, expected_code, **kwargs):
    return request('GET', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)


def get(cluster_or_node, path, **kwargs):
    return request('GET', cluster_or_node, path, **kwargs)


def ensure_deleted(cluster, path, expected_codes=None, **kwargs):
    if expected_codes is None:
        expected_codes = [200, 404]
    res = delete(cluster, path, **kwargs)
    code = res.status_code
    assert code in expected_codes, format_http_error(res, expected_codes)
    return res


def delete(cluster_or_node, path, **kwargs):
    return request('DELETE', cluster_or_node, path, **kwargs)


def delete_succ(cluster_or_node, path, expected_code=200, **kwargs):
    return request('DELETE', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)


def delete_fail(cluster_or_node, path, expected_code, **kwargs):
    return request('DELETE', cluster_or_node, path, expected_code=expected_code,
                   **kwargs)

def delete_fail(cluster_or_node, path, expected_code, **kwargs):
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

def assert_http_body_string(expected, res):
    assert res.text == expected, format_res_info(res) + \
        f" (expected body: {expected})"

def format_res_info(res):
    return f"{res.request.method} {res.url} " \
           f"returned {res.status_code} {res.reason}" \
           f", response body: {res.text}"

def format_error(resp, error):
    if resp == None:
        return "Error: " + error
    return format_res_info(resp) + " Error: " + error

def format_http_error(res, expected_codes=None):
    expected_codes_str = " or ".join([str(c) for c in expected_codes])
    return format_res_info(res) + f" (expected codes: {expected_codes_str})"

def assert_json_key(expected_key, json, context):
    assert expected_key in json.keys(), \
        f"({context}) '{expected_key}' missing in json: {json}"
    return json[expected_key]


def assert_eq(got, expected, name='value', resp=None):
    assert expected == got, \
        format_error(resp, f'unexpected {name}: {got}, expected: {expected}')

def assert_gt(got, lower_bound, name='value'):
    assert got > lower_bound, \
        f'unexpected {name}: {got}, expected: > {lower_bound}'


def assert_in(what, where, resp=None):
    assert what in where, \
        format_error(resp, f'"{what}" is missing in "{where}"')

def assert_not_in(what, where, resp=None):
    assert what not in where, format_error(resp, f'"{what}" is in "{where}"')


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
                       retry_value=False,
                       retry_on_assert=False):

    assert (attempts is not None) or (timeout is not None)
    assert sleep_time > 0, "non-positive sleep_time specified"
    start_time = time.time()
    sleep_time_str = f"{sleep_time:.2f}s"

    attempt_count = 0
    exception_obj = None
    while (attempts is None) or (attempt_count < attempts):
        if timeout is not None and ((time.time() - start_time) >= timeout):
            error_msg = f"{msg}: timed-out (timeout: {timeout}s)"
            if exception_obj is None:
                assert False, error_msg
            else:
                maybe_print(error_msg, verbose=verbose)
                raise exception_obj
        if retry_on_assert:
            try:
                value = fun()
            except AssertionError as e:
                maybe_print(f"retrying because assertion failed: {e}",
                            verbose=verbose)
                exception_obj = e
                value = retry_value
        else:
            value = fun()
        if value is not retry_value:
            maybe_print(f"Time taken for condition to complete: "
                        f"{time.time() - start_time: .2f}s", verbose=verbose)
            return value
        maybe_print(f"Sleeping for {sleep_time_str}", verbose=verbose)
        time.sleep(sleep_time)
        attempt_count += 1

    error_msg = f"{msg} didn't complete in: {attempts} attempts, " \
                f"sleep_time: {sleep_time_str}"
    if exception_obj is None:
        assert False, error_msg
    else:
        maybe_print(error_msg, verbose=verbose)
        raise exception_obj

def metakv_get_succ(cluster, key, **kwargs):
    return get_succ(cluster, f"/_metakv{key}", **kwargs)

def metakv_get(cluster, key, **kwargs):
    return get(cluster, f"/_metakv{key}", **kwargs)

def metakv_put_succ(cluster, key, **kwargs):
    return put_succ(cluster, f"/_metakv{key}", **kwargs)

def metakv_put_fail(cluster, key, expected_code, **kwargs):
    return put_fail(cluster, f"/_metakv{key}", expected_code, **kwargs)

def metakv_delete_succ(cluster, key, **kwargs):
    return delete_succ(cluster, f"/_metakv{key}", **kwargs)

def metakv_delete_fail(cluster, key, expected_code, **kwargs):
    return delete_fail(cluster, f"/_metakv{key}", expected_code, **kwargs)

def diag_eval(cluster, code, **kwargs):
    return post_succ(cluster, '/diag/eval', data=code, **kwargs)

def get_prometheus_metrics(cluster):
    r = get_succ(cluster, '/metrics')
    return parse_prometheus_to_label_value_dict(r.text)

def parse_prometheus_to_label_value_dict(prometheus_text: str):
    result = {}
    metric_families = text_string_to_metric_families(prometheus_text)

    for family in metric_families:
        metric_name = family.name

        metric_entry = {
            "TYPE": family.type,
            "HELP": family.documentation,
            "VALUES": {}
        }

        for sample in family.samples:
            labels_key = tuple(sorted(sample.labels.items()))
            metric_entry["VALUES"][labels_key] = sample.value

        if metric_entry["VALUES"] or metric_entry["TYPE"] or metric_entry[
                "HELP"]:
           result[metric_name] = metric_entry

    return result

@contextlib.contextmanager
def no_output(name, verbose=None, error_callback=None):
    """
    Executes context body with all the output redirected to a string.
    If something crashes, it prints that output, otherwise it ignores it.
    If verbose is true, it doesn't redirect anything.
    """
    if verbose is None:
        verbose = config['verbose']

    if error_callback is None:
        error_callback = lambda _: None

    if verbose:
        try:
            yield
        except Exception as e:
            error_callback(e)
            raise e
        return

    f = io.StringIO()
    try:
        with contextlib.redirect_stdout(f):
            yield
    except Exception as e:
        error_callback(e)
        output = f.getvalue()
        if len(output) > 0:
            extra_cr = '\n' if output[-1] != '\n' else ''
            print(
                f"================== {name} output begin =================\n"
                f"{output}{extra_cr}"
                f"=================== {name} output end ==================\n",
                show_time=False)

        raise e


def format_exception(e):
    return red('\n'.join(format_exception_only(type(e), e)).strip('\n'))


def start_report(full_name, name,  report_name=False, single_line=True):
    if report_name:
        return start_verbose_report(name, single_line=single_line)
    return start_silent_report(full_name)


def start_verbose_report(name, single_line=True):
    prefix = "*** "
    space_reserved_for_res = 7
    space_reserved_for_time = 15
    if single_line:
        space_reserved = space_reserved_for_res + space_reserved_for_time
        width_taken = print_wrapped(name, indent=2, suffix="... ",
                                    max_width=config['screen_width'] -
                                              space_reserved,
                                    end='', flush=True)
    else:
        print(f"\n{prefix}Starting: {name}...")

    def end_report(test_e, teardown_e, time_delta, teardown_time_delta):
        times_str = format_test_times(time_delta, teardown_time_delta)
        if test_e is None and teardown_e is None:
            if single_line:
                res = right_aligned("ok", taken=width_taken,
                                    width=config['screen_width'] -
                                          space_reserved_for_time)
                print(green(res) + times_str, show_time=False)
            else:
                print(f"{prefix}Finished: " + green("ok") + times_str)
            return

        res_prefix = 'teardown ' if test_e is None else ''

        if single_line:
            res = right_aligned(res_prefix + "failed", taken=width_taken,
                                width=config['screen_width'] -
                                      space_reserved_for_time)
            print(red(res) + times_str, show_time=False)
        else:
            res = res_prefix + "failed"
            print(f"{prefix}Finished: " + red(res) + times_str)

        if test_e is not None:
            print_wrapped(f'{format_exception(test_e)}', indent=2,
                          max_width=config['screen_width'])
        if teardown_e is not None:
            print_wrapped(f'{red("teardown exception:")} ' \
                          f'{format_exception(teardown_e)}', indent=2,
                          max_width=config['screen_width'])
    return end_report


def start_silent_report(full_name):
    def end_report(test_e, teardown_e, time_delta, teardown_time_delta):
        if test_e is not None:
            print(red(f"{full_name} failed ({format_exception(test_e)})"))
        if teardown_e is not None:
            print(red(f"teardown exception: {format_exception(teardown_e)}"))

    return end_report


def right_aligned(s, taken=0, width=None):
    if not config['align_res']:
        return s
    if width is None:
        width = config['screen_width']
    corrected_width = max(0, width - taken)
    return f'{s: >{corrected_width}}'


def no_output_decorator(f):
    def wrapped_f(*args, **kwargs):
        with no_output(f.__name__):
            return f(*args, **kwargs)
    return wrapped_f


def maybe_print(s, verbose=None, print_fun=None):
    if print_fun is None:
        print_fun = print
    if verbose is None:
        verbose = config['verbose']
    if verbose:
        print_fun(s)


def log_at_all_nodes(cluster, msg):
    errors = []
    for n in cluster._nodes:
        try:
            diag_eval(n, f'\"{msg}\".', verbose=config['verbose'])
        except Exception as e:
            errors.append(TestError(
                name=f'Log at {n}',
                error=e,
                cluster_name=cluster.short_name()
            ))
    return errors


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
    print(f"Waiting for ejected node {ejected_node} to reset itself...")
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
        sleep_time=1, timeout=180,
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


def get_cluster_test_dir():
    return os.path.join(THIS_FILE_DIR, '..')


def get_install_dir():
    return os.path.join(NS_SERVER_DIR, '..', 'install')


def get_bin_dir():
    return os.path.join(get_install_dir(), 'bin')


def get_utility_path(utility_name):
    return os.path.join(get_bin_dir(), utility_name)


def get_ns_server_dir():
    return NS_SERVER_DIR


def get_resources_dir():
    return os.path.join(THIS_FILE_DIR, '..', 'resources')


def get_pylib_dir():
    return os.path.join(NS_SERVER_DIR, 'pylib')


def get_scripts_dir():
    return os.path.join(NS_SERVER_DIR, 'scripts')


def print_wrapped(str_to_print, indent=0, suffix="", max_width=None,
                  separators=None, **kwargs):
    if separators is None:
        separators = [' ', '-', '_', '.', '/', '\\']

    if kwargs.get('show_time', True) and config['report_time']:
        local_time = datetime.now().strftime(config['report_time_format'])
        time_width = len(local_time) + 1 # +1 for space
    else:
        time_width = 0
    prefix_str = " " * indent
    overhead = len(prefix_str) + len(suffix) + time_width

    if max_width is None:
        no_wrap = True
    else:
        max_line_length = max(1, max_width - overhead)
        no_wrap = (len(str_to_print) <= max_line_length)

    if not config['wrap_output'] or no_wrap:
        # Name fits on one line or no wrapping is requested
        print_str = f"{prefix_str}{str_to_print}{suffix}"
        print(print_str, **kwargs)
        return len(print_str) + time_width

    remaining_str = str_to_print
    first_line = True
    extra_prefix = ""
    while len(remaining_str) > max_line_length:
        # Try to find a word boundary within the line limit
        candidate_part = remaining_str[:max_line_length]

        # Find the rightmost separator
        best_pos = -1
        best_sep = None
        for sep in separators:
            pos = candidate_part.rfind(sep)
            if pos > best_pos:
                best_pos = pos
                best_sep = sep

        if best_pos != -1:
            # Found a separator, break at word boundary
            trailing_chars = 0 if best_sep == ' ' else 1 # skip trailing space
            line_part = remaining_str[:best_pos+trailing_chars]
            remaining_str = remaining_str[best_pos + 1:]
        else:
            # No separator found, do hard cut
            line_part = candidate_part
            remaining_str = remaining_str[max_line_length:]

        print(f"{extra_prefix}{prefix_str}{line_part}", show_time=first_line)
        first_line = False
        extra_prefix = "" if first_line else " " * time_width

    # Print the last line
    left_to_print = f"{extra_prefix}{prefix_str}{remaining_str}{suffix}"
    print(left_to_print, show_time=False, **kwargs)
    return len(left_to_print)
