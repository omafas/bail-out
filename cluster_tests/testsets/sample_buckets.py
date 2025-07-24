# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import time

import testlib
from testsets.tasks_test import TasksBase


# 60 second timeout for running to completed. cbimport shouldn't take any
# longer than 10s, but jenkins sometimes takes much longer so 60s is used
# to avoid false failed tests
CBIMPORT_TIMEOUT = 60
# 10 second timeout for task to go from queued to running/completed, once the
# task is at the front of the queue, and there is no other task running.
# Typically, this takes <50ms, but it has sometimes taken more than 1s
START_TASK_TIMEOUT = 10

class SampleBucketTasksBase(TasksBase):
    def __init__(self):
        self.addr_get = "/sampleBuckets"
        self.addr_post = self.addr_get + "/install"
        self.addr_tasks = "/pools/default/tasks"

    # Extract the task descriptions from the response to a request to the
    # /sampleBuckets/install endpoint
    def get_task_descs_from_response(self, response, num_samples=1):
        json = testlib.json_response(response, f"{self.addr_post} "
                                               "returned invalid json")

        tasks = testlib.assert_json_key("tasks", json, self.addr_post)

        # Assert that the number of tasks returned matches the number of samples
        assert len(tasks) == num_samples, \
            f"Unexpected number of task objects in " \
            f"{self.addr_post} response: {len(json)}"
        return tasks

    # To determine that the sample was loaded, we wait for the task status
    # to become "completed"
    def assert_loaded_sample(self, response, timeout):
        task_desc = self.get_task_descs_from_response(response)[0]
        # First check that the sample immediately starts loading
        self.assert_sample_load_started(task_desc)
        # Then check that the sample load completes within timeout
        self.assert_task_status(task_desc, "completed", timeout)

    # Check that all samples immediately start loading, then wait for them all
    # to complete
    def assert_loaded_samples(self, response, timeout, num_samples):
        # Fetch task descriptions
        tasks = self.get_task_descs_from_response(response, num_samples)

        # Assert that all sample buckets in the batch have started loading
        for task_desc in tasks:
            self.assert_sample_load_started(task_desc)

        # Assert that each sample loading task becomes completed
        for task_desc in tasks:
            self.assert_task_status(task_desc, "completed", timeout)

    # To determine that the sample failed to load, we wait for the task status
    # to become "failed"
    def assert_sample_load_failed(self, response, timeout):
        task_desc = self.get_task_descs_from_response(response)[0]
        self.assert_task_status(task_desc, "failed", timeout)

    # Assert that the task is currently running or completed
    def assert_sample_load_started(self, task_desc):
        task_id = testlib.assert_json_key("taskId", task_desc, self.addr_post)

        def loading_done(task):
            if task is None:
                return False
            else:
                return task.get("status") in ["running", "completed"]

        # Wait for the task to satisfy loading_done
        self.wait_for_task(
            task_id, loading_done, START_TASK_TIMEOUT,
            timeout_msg=f"Waiting for sample loading task to become 'running' "
                        f"or 'completed' timed out after {START_TASK_TIMEOUT}s."
                        f"\n")

    # Wait for the task's status to become expected_last_status, failing the
    # test if timeout is reached
    def assert_task_status(self, task_desc, expected_last_status, timeout):
        # Generate the expected final task status from the task description
        task_id = testlib.assert_json_key("taskId", task_desc, self.addr_post)
        sample_bucket = testlib.assert_json_key("bucket", task_desc,
                                                self.addr_post)
        expected_task = self.Task(task_id, "loadingSampleBucket",
                                  expected_last_status,
                                  {"bucket": sample_bucket})

        def loading_done(task):
            if task is None:
                return False
            else:
                return task.get("status") == expected_last_status

        # Wait for the task to satisfy loading_done
        fetched_task = self.wait_for_task(task_id, loading_done, timeout)

        # Assert that the final task is as expected
        self.assert_tasks_equal(fetched_task, expected_task)

    def load_sample_bucket(self, cluster, sample_bucket):
        payload = [sample_bucket]
        return testlib.post_succ(cluster, self.addr_post, 202,
                                     json=payload)

    def load_and_assert_sample_bucket(self, cluster, sample_bucket):
        response = self.load_sample_bucket(cluster, sample_bucket)

        # Double the timeout to allow for bucket creation
        self.assert_loaded_sample(response, CBIMPORT_TIMEOUT * 2)

class SampleBucketTestSet(testlib.BaseTestSet, SampleBucketTasksBase):

    def __init__(self, cluster):
        super().__init__(cluster)
        SampleBucketTasksBase.__init__(self)
        self.addr_buckets = None

    @staticmethod
    def requirements():
        return [testlib.ClusterRequirements(edition="Enterprise",
                                            min_num_nodes=2,
                                            min_memsize=600,
                                            buckets=[],
                                            num_vbuckets=16),
                testlib.ClusterRequirements(edition="Serverless",
                                            min_num_nodes=2,
                                            min_memsize=600,
                                            buckets=[],
                                            num_vbuckets=16)]

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        # Kill any remaining sample loads
        testlib.post_succ(self.cluster, "/diag/eval",
                          data="gen_server:stop(samples_loader_tasks).")
        # Deleting any remaining buckets
        testlib.delete_all_buckets(self.cluster)

    # Create a bucket with name and ram_quota specified
    def create_bucket(self, name, ram_quota=200, storage_backend='magma'):
        bucket = {"name": name,
                  "ramQuota": ram_quota,
                  "storageBackend": storage_backend}
        self.cluster.create_bucket(bucket)

    # Test the /sampleBuckets endpoint for fetching the list of sample buckets
    def get_test(self):
        response = testlib.get_succ(self.cluster, self.addr_get)
        samples = testlib.json_response(response,
                                        "/sampleBuckets returned invalid json")
        for sample in samples:
            for key in ["name", "installed", "quotaNeeded"]:
                testlib.assert_json_key(key, sample, self.addr_get)

    # Set the concurrency limit
    def set_concurrency(self, concurrency):
        testlib.post_succ(self.cluster, "/settings/serverless",
                          data={"maxConcurrentSampleLoads": concurrency})

    # Test that we can load into a new bucket, which will have the same name as
    # the specified sample
    def post_without_existing_bucket_test(self):
        self.load_and_assert_sample_bucket(self.cluster, "travel-sample")

    # Test loading into an existing bucket
    def post_with_existing_bucket_test(self):
        bucket_name = "test1"
        self.create_bucket(bucket_name)

        sample_bucket = "travel-sample"
        payload = [{"sample": sample_bucket,
                    "bucket": bucket_name}]
        response = testlib.post_succ(self.cluster, self.addr_post, 202,
                                     json=payload)

        self.assert_loaded_sample(response, CBIMPORT_TIMEOUT)

    # Test loading from http(s):// sample into an existing bucket
    def post_to_http_with_existing_bucket_test(self):
        bucket_name = "test2"
        self.create_bucket(bucket_name)

        # Provide a dummy http(s):// address to attempt to download from
        sample_bucket = "https://dummy-sample-address"
        payload = [{"sample": sample_bucket,
                    "bucket": bucket_name,
                    "http_cache_directory": "/tmp/cache"}]
        response = testlib.post_succ(self.cluster, self.addr_post, 202,
                                     json=payload)

        # We check for task failed instead of completed, because it will
        # immediately fail since we have only provided a dummy http(s)://
        # address
        self.assert_sample_load_failed(response, CBIMPORT_TIMEOUT)

    # Confirm that loading a sample bucket fails when there is insufficient
    # total remaining ram quota
    def post_with_insufficient_remaining_ram_quota_test(self):
        # Create bucket taking up all space
        bucket_name = "test3"
        self.create_bucket(bucket_name, self.cluster.memory_quota())

        # Loading a sample bucket should fail when the cluster has insufficient
        # ram quota
        sample_bucket = "travel-sample"
        payload = [sample_bucket]
        testlib.post_fail(self.cluster, self.addr_post, 400, json=payload)

    # Confirm that loading sample data into an existing bucket fails when the
    # bucket has insufficient ram quota
    def post_to_existing_with_insufficient_ram_quota_test(self):
        bucket_name = "test4"
        self.create_bucket(bucket_name, 100)

        # Loading a sample bucket should fail when the bucket has insufficient
        # ram quota
        sample_bucket = "travel-sample"
        payload = [{"sample": sample_bucket,
                    "bucket": bucket_name}]
        testlib.post_fail(self.cluster, self.addr_post, 400, json=payload)

    def post_with_couchdb_sample_test(self):
        sample_bucket = "gamesim-sample"
        # Create a couchstore bucket, so that views are imported
        self.create_bucket(sample_bucket,
                           ram_quota=200,
                           storage_backend='couchstore')
        payload = [{"sample": sample_bucket,
                    "bucket": sample_bucket}]
        if self.cluster.is_serverless:
            # Confirm that loading gamesim-sample fails in serverless, as
            # couchdb is disabled
            testlib.post_fail(self.cluster, self.addr_post, 400, json=payload)
        else:
            # Confirm that loading gamesim-sample succeeds when not in
            # serverless, as couchdb is enabled
            response = testlib.post_succ(self.cluster, self.addr_post, 202,
                                         json=payload)
            # Double the timeout to allow for bucket creation
            self.assert_loaded_sample(response, CBIMPORT_TIMEOUT * 2)

    # Test loading multiple sample buckets sequentially
    def post_multiple_buckets_sequential_test(self):
        # Create 3 buckets (with total ram quota 3*200MiB = 600MiB)
        bucket_count = 3
        bucket_names = [f"test{i}" for i in range(bucket_count)]
        for bucket_name in bucket_names:
            self.create_bucket(bucket_name, 200)

        # Load the buckets, 1 at a time
        responses = []
        for bucket_name in bucket_names:
            sample_bucket = "travel-sample"
            payload = [{"sample": sample_bucket,
                        "bucket": bucket_name}]
            responses.append(testlib.post_succ(self.cluster, self.addr_post,
                                               202, json=payload))

        # Assert that each bucket gets loaded, in the correct order. We test
        # this to make sure that later buckets don't jump ahead in
        # the queue, delaying earlier sample bucket loads
        for response in responses:
            self.assert_loaded_sample(response, timeout=CBIMPORT_TIMEOUT)

    # Test loading multiple sample buckets concurrently
    def post_multiple_buckets_concurrent_test(self):
        # Set concurrency limit to 2
        concurrency = 2
        self.set_concurrency(concurrency)
        try:
            # Create bucket
            bucket_names = [f"test{i}" for i in range(concurrency)]
            for bucket_name in bucket_names:
                self.create_bucket(bucket_name, 200)

            # Attempt to load buckets with sample data
            sample_bucket = "travel-sample"
            payload = [{"sample": sample_bucket,
                        "bucket": bucket_name} for bucket_name in bucket_names]
            response = testlib.post_succ(self.cluster, self.addr_post, 202,
                                         json=payload)

            # We multiply the timeout by the number of requests that may be
            # running at once, as this is the worst case slow down from
            # concurrency.
            timeout = CBIMPORT_TIMEOUT * concurrency

            # Check that all sample buckets start loading immediately, and all
            # complete successfully
            self.assert_loaded_samples(response, timeout, len(payload))
        finally:
            # Reset the concurrency to 1
            self.set_concurrency(1)

## This test loads a sample bucket with n2n encryption and mandatory client
## certs. Prior to the fix for MB-67026 this used to fail.
class SampleBucketTLSTestSet(testlib.BaseTestSet, SampleBucketTasksBase):

    def __init__(self, cluster):
        super().__init__(cluster)
        SampleBucketTasksBase.__init__(self)

    @staticmethod
    def requirements():
        return [testlib.ClusterRequirements(edition="Enterprise",
                                            min_num_nodes=1,
                                            min_memsize=600,
                                            buckets=[],
                                            encryption=True,
                                            num_vbuckets=16)]

    def setup(self):
        # Disable auto-failover
        testlib.post_succ(self.cluster, '/settings/autoFailover',
                          data={'enabled': 'false'})

        # Save current cluster encryption level to restore on teardown
        settings = testlib.get_succ(self.cluster, '/settings/security').json()
        self.prev_encryption_level = settings['clusterEncryptionLevel']

        # cluster encryption level strict
        testlib.post_succ(self.cluster, '/settings/security',
                          data={'clusterEncryptionLevel': 'strict'})

        # Wait for the web service to restart
        self.cluster.wait_for_web_service()

        # client cert auth state mandatory
        testlib.toggle_client_cert_auth(self.cluster, enabled=True,
                                        mandatory=True)

    def teardown(self):
        pass

    def test_teardown(self):
        testlib.toggle_client_cert_auth(self.cluster, enabled=False)
        data = {'clusterEncryptionLevel': f'{self.prev_encryption_level}'}
        testlib.post_succ(self.cluster, '/settings/security', data=data)
        self.cluster.wait_for_web_service()

        # Kill any remaining sample loads
        testlib.post_succ(self.cluster, "/diag/eval",
                          data="gen_server:stop(samples_loader_tasks).")
        # Deleting any remaining buckets
        testlib.delete_all_buckets(self.cluster)

    # Test that we can load into a new bucket, which will have the same name as
    # the specified sample
    def post_without_existing_bucket_test(self):
        self.load_and_assert_sample_bucket(self.cluster, "travel-sample")
