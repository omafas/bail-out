# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib

import time
import itertools
from copy import copy


class TasksBase:
    class Task:
        def __init__(self, task_id, task_type, status, extras):
            self.task_id = task_id
            self.task_type = task_type
            self.status = status
            self.extras = extras

    def __init__(self):
        self.addr_pools_default = "/pools/default"
        self.addr_tasks = self.addr_pools_default + "/tasks"

    def get_status_address(self, *task_ids):
        return f"{self.addr_tasks}?" + \
               "&".join([f"taskId={task_id}" for task_id in task_ids])

    def get_task_statuses(self, *task_ids):
        status_address = self.get_status_address(*task_ids)
        # Fetch the task status
        r = testlib.get_succ(self.cluster, status_address)

        # Attempt to decode the response
        tasks = testlib.json_response(r, "Task status was not json")

        assert len(tasks) == len(task_ids), \
            f"Unexpected number of tasks in response to '{status_address}': " \
            f"{len(tasks)}"

        return tasks

    # Wait for the task identified by task_id to satisfy is_task_done or reach
    # the timeout limit
    def wait_for_task(self, task_id, is_task_done, timeout,
                      timeout_msg=None):
        def get_status():
            return self.get_task_statuses(task_id)[0]
        # Wait until timeout reached
        time_start = time.time()
        timeout_time = time_start + timeout
        while not is_task_done(last_task_status := get_status()) and \
                time.time() < timeout_time:
            # Check twice a second
            time.sleep(0.5)

        if timeout_msg is None:
            timeout_msg = f"Task status check timed out after {timeout}s.\n"

        assert last_task_status is not None, \
            f"{timeout_msg}" \
            f"No task found with task_id '{task_id}'"

        assert is_task_done(last_task_status), \
            f"{timeout_msg}" \
            f"Last task status:\n{last_task_status}"

        return last_task_status

    @staticmethod
    def assert_tasks_equal(found_task, expected_task):
        # Check the task type, status, bucket, and extras are correct
        assert found_task.get("task_id") == expected_task.task_id, \
            f"Task id '{found_task.get('task_id')}' found, expected " \
            f"'{expected_task.task_id}'"
        assert found_task.get("type") == expected_task.task_type, \
            f"Task type '{found_task.get('type')}' found, expected " \
            f"'{expected_task.task_type}'"
        assert found_task.get("status") == expected_task.status, \
            f"Task status '{found_task.get('status')}' found, expected " \
            f"'{expected_task.status}'"
        for key, value in expected_task.extras.items():
            assert found_task.get(key) == value, \
                f"Task {key} {found_task.get(key)} found, expected {value}"

    # Assert that the task can be fetched with ?taskId= and that the status is
    # as expected
    def assert_task(self, expected_task):
        found_task = self.get_task_statuses(expected_task.task_id)[0]
        self.assert_tasks_equal(found_task, expected_task)

        # Assert whether or not the task shows up in the
        # /pools/default/tasks endpoint
        if self.is_default_task(expected_task):
            self.assert_task_in_default_list(expected_task)
        else:
            self.assert_task_not_in_default_list(expected_task)

    # Assert that a list of tasks can be fetched from /pools/default/tasks
    # with ?taskId=id1&taskId=id2...
    def assert_tasks(self, expected_tasks):
        expected_task_ids = [task.task_id for task in expected_tasks]

        # Fetch the task statuses all at once
        found_tasks = self.get_task_statuses(*expected_task_ids)

        # The lists must be sorted as the tasks are not guaranteed to be in the
        # same order as requested
        found_tasks.sort(key=lambda task: task.get("task_id", None))
        expected_tasks.sort(key=lambda task: task.task_id)

        for found_task, expected_task in zip(found_tasks, expected_tasks):
            self.assert_tasks_equal(found_task, expected_task)

            # Assert whether or not the task shows up in the
            # /pools/default/tasks endpoint
            if self.is_default_task(expected_task):
                self.assert_task_in_default_list(expected_task)
            else:
                self.assert_task_not_in_default_list(expected_task)

    @staticmethod
    # Determines whether a task should be expected in the default response of
    # /pools/default/tasks
    def is_default_task(task):
        return task.status == "running"

    # Assert that can the expected task can be found in the tasks list given by
    # /pools/default/tasks
    def assert_task_in_default_list(self, expected_task):
        expected_id = expected_task.task_id

        # Fetch tasks
        r = testlib.get_succ(self.cluster, self.addr_tasks)

        # Attempt to decode the response
        tasks = testlib.json_response(r, "Tasks list was not json")

        for found_task in tasks:
            if found_task.get("task_id") == expected_id:
                self.assert_tasks_equal(found_task, expected_task)
                return

        assert False, f"Task id '{expected_id} not found in tasks list:\n" \
                      f"{tasks}"

    # Assert that a task is not included in the tasks list given by
    # /pools/default/tasks
    def assert_task_not_in_default_list(self, unexpected_task):
        unexpected_id = unexpected_task.task_id

        # Fetch tasks
        r = testlib.get_succ(self.cluster, self.addr_tasks)

        # Attempt to decode the response
        tasks = testlib.json_response(r, "Tasks list was not json")

        for found_task in tasks:
            assert found_task.get("task_id") != unexpected_id, \
                f"Unexpected task found with task_id '{unexpected_id}"

    # Get the current tasks list version
    def get_tasks_version(self):
        r = testlib.get_succ(self.cluster, self.addr_pools_default)
        json_resp = testlib.json_response(r, f"{self.addr_pools_default} "
                                             f"response was not json")
        tasks = testlib.assert_json_key("tasks", json_resp,
                                        self.addr_pools_default)
        uri = testlib.assert_json_key("uri", tasks, self.addr_pools_default)

        # Tasks version is provided by the 'v' key in the uri's query string
        assert "?v=" in uri, "Missing tasks hash in uri"
        return uri.split("?v=")[1]

    def assert_tasks_version_changed(self, old_version):
        new_version = self.get_tasks_version()
        assert new_version != old_version, \
            f"Tasks version was not changed when expected"
        # Return the new version, for subsequent comparisons without re-fetching
        return new_version

    def assert_tasks_version_same(self, old_version):
        new_version = self.get_tasks_version()
        assert new_version == old_version, \
            f"Unexpected change of tasks version"


# Convert a python list of strings to an erlang list
def list_to_erlang(elements):
    return "[" + ",".join(elements) + "]"


def extras_to_erlang(extras):
    return list_to_erlang([f"{{{key}, \"{value}\"}}" if key == "bucket" else
                           f"{{{key}, <<\"{value}\">>}}"
                           for key, value in extras.items()])


# Convert a new task to an erlang record, for creating the task with /diag/eval
def task_create_to_erlang(task):
    return f"{{global_task, <<\"{task.task_id}\">>,{task.task_type}," \
           f"{task.status},{extras_to_erlang(task.extras)}}}"


# Convert a list of tasks to a list of erlang records, for creating the task
# with /diag/eval
def task_creates_to_erlang(tasks):
    return list_to_erlang(task_create_to_erlang(task) for task in tasks)


# Convert a task update to an erlang record, for updating the task with
# /diag/eval
def task_update_to_erlang(task):
    return (f"fun(T0) ->"
            f" T1 = lists:keyreplace(status, 1, T0,"
            f"  {{status, {task.status}}}),"
            f" lists:keyreplace(extras, 1, T1,"
            f"  {{extras, {extras_to_erlang(task.extras)}}})"
            f"end")


# Convert a list of task updates to an erlang function, for updating the task
# with /diag/eval
def task_updates_to_erlang(tasks):
    return (f"fun(T0) -> "
            f" case global_tasks:task_id(T0) of "
            + ";".join(
             [f" <<\"{task.task_id}\">> -> "
              f"  T1 = lists:keyreplace(status, 1, T0,"
              f"   {{status, {task.status}}}), "
              f"  lists:keyreplace(extras, 1, T1,"
              f"   {{extras, {extras_to_erlang(task.extras)}}}) "
              for task in tasks] + [f" _ -> T0 "]) +
            f" end end")


class TasksTestSet(testlib.BaseTestSet, TasksBase):
    def __init__(self, cluster):
        testlib.BaseTestSet.__init__(self, cluster)
        TasksBase.__init__(self)
        self.addr_diag_eval = "/diag/eval"
        self.task_types = ["loadingSampleBucket"]
        self.statuses = ["queued", "running", "completed", "failed"]
        self.extras_values = [{},
                              {"bucket": "test",
                               "bucket_uuid": "test_uuid"}]

    def setup(self):
        pass

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(min_memsize=1024)

    def teardown(self):
        testlib.post_succ(self.cluster, self.addr_diag_eval,
                          data="""
                          chronicle_compat:transaction([tasks],
                          fun (Snapshot) -> {commit, [{set, tasks, []}]}
                          end)
                          """)

    # Generate a task with random task id
    def generate_task(self, task_type, status, extras):
        return self.Task(testlib.random_str(8), task_type, status, extras)

    # Manually create a task by diag eval, to test global_tasks generically
    def create_task(self, task):
        testlib.post_succ(self.cluster,
                          self.addr_diag_eval,
                          data=f"global_tasks:update_task("
                               f"{task_create_to_erlang(task)}).")

    # Manually create a list of tasks by diag eval
    def create_tasks(self, tasks):
        testlib.post_succ(self.cluster,
                          self.addr_diag_eval,
                          data=f"global_tasks:update_tasks("
                               f"{task_creates_to_erlang(tasks)}).")

    # Manually update a task by diag eval
    def update_task(self, task):
        testlib.post_succ(self.cluster,
                          self.addr_diag_eval,
                          data=f"global_tasks:update_task("
                               f"<<\"{task.task_id}\">>,"
                               f"{task_update_to_erlang(task)}).")

    # Manually update a list of tasks by diag eval
    def update_tasks(self, tasks):
        task_ids = list_to_erlang(f"<<\"{task.task_id}\">>" for task in tasks)
        testlib.post_succ(self.cluster,
                          self.addr_diag_eval,
                          data=f"global_tasks:update_tasks({task_ids},"
                               f"{task_updates_to_erlang(tasks)}).")

    # Generate valid task updates by modifying the status in all possible ways,
    # then modifying the extras field in all possible ways
    def generate_task_updates(self, task):
        # Copy the task so that we can modify it without impacting the original
        new_task = copy(task)
        # Yield a new task with each status other than the existing one
        for status in self.statuses:
            if status != new_task.status:
                new_task.status = status
                yield copy(new_task)
        # Yield a new task with each value of extras other than the existing one
        for extras in self.extras_values:
            if extras != new_task.extras:
                new_task.extras = extras
                yield copy(new_task)

    # Attempt to update a task with a sequence of valid changes
    def test_updating_task(self, initial_task, version):
        last_task = initial_task
        for task_update in self.generate_task_updates(initial_task):
            self.update_task(task_update)
            self.assert_task(task_update)

            # If the task_update and last_task are either both default tasks
            # or both not default tasks, then version should be the same
            if self.is_default_task(task_update) == \
                    self.is_default_task(last_task):
                # Assert that the tasks version is unaffected by an update to an
                # existing task
                self.assert_tasks_version_same(version)
            else:
                # If exactly one of task_update and last_task are default tasks
                # then the task will either be added or removed from the
                # default tasks list, so the version should change
                version = self.assert_tasks_version_changed(version)
            last_task = task_update

        # Return the final version for use in subsequent comparisons
        return version

    # Generate a variety of tasks
    def generate_tasks(self):
        test_tasks = itertools.product(self.task_types, self.statuses,
                                       self.extras_values)
        for task_type, status, extras in test_tasks:
            yield self.generate_task(task_type, status, extras)

    def simple_test(self):
        version = self.get_tasks_version()
        for task in self.generate_tasks():
            # Add a new task and assert that it was added
            self.create_task(task)
            self.assert_task(task)

            if self.is_default_task(task):
                # If we add a new task to the default tasks list, then the
                # version should change
                version = self.assert_tasks_version_changed(version)
                # Test that subsequent task updates correctly impact the version
                version = self.test_updating_task(task, version)

    # Test creating and updating multiple tasks at once
    def multiple_tasks_test(self):
        # Generate and create a list of tasks
        tasks = list(self.generate_tasks())
        self.create_tasks(tasks)
        self.assert_tasks(tasks)
        # Generate one task update for each task
        task_updates = [next(self.generate_task_updates(task))
                        for task in tasks]
        self.update_tasks(task_updates)
        self.assert_tasks(task_updates)
