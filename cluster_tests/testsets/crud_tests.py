# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
from testlib import json_response, assert_json_key, assert_eq, assert_gt
from testlib.util import Service

bucket_name = "test"
doc = "test_doc"
doc_addr = f"/pools/default/buckets/{bucket_name}/docs/{doc}"


class CrudTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(
            include_services=[Service.KV],
            num_vbuckets=16,
            buckets=[{"name": bucket_name,
                      "ramQuota": 100}])

    def setup(self):
        # Make sure that we can successfully write to the cluster
        testlib.poll_for_condition(self.cluster.can_write(bucket_name, doc),
                                   sleep_time=0.5, attempts=120)
        testlib.ensure_deleted(self.cluster, doc_addr,
                               expected_codes=[200])

    def teardown(self):
        # Restore default internalSettings
        testlib.post_succ(self.cluster, "/internalSettings",
                          data={"maxDocsSkip": 1000,
                                "maxDocsLimit": 1000})

    def test_teardown(self):
        testlib.ensure_deleted(self.cluster, doc_addr,
                               expected_codes=[200, 400])

    def expiry_default_test(self):
        # Expiry gets default value of 0 when doc created
        testlib.post_succ(self.cluster, doc_addr)
        # Expiration is 0 as expiry is not enabled
        assert_eq(self.get_expiration(), 0)

        # Expiry can be modified
        testlib.post_succ(self.cluster, doc_addr,
                          data={
                              "expiry": 10_000  # Document expires in 10,000s
                          })
        # Expiration will be non-zero, as the document will expire at some time
        assert_gt(self.get_expiration(), 0)

        # Expiry gets reset to default value of 0 when doc updated
        testlib.post_succ(self.cluster, doc_addr)
        # Expiration is 0 as expiry has been disabled
        assert_eq(self.get_expiration(), 0)

    def preserve_ttl_test(self):
        # Expiry gets specified value when doc created with preserveTTL=true
        testlib.post_succ(self.cluster, doc_addr,
                          data={
                              "expiry": 1000,
                              "preserveTTL": "true"
                          })
        # Expiration is >0 as expiry is enabled
        assert_gt(self.get_expiration(), 0)

        # Expiry gets reset to default value of 0 when doc updated with
        # preserveTTL=false
        testlib.post_succ(self.cluster, doc_addr,
                          data={
                              "preserveTTL": "false"
                          })
        # Expiration is 0 as expiry has been disabled
        assert_eq(self.get_expiration(), 0)

        # Expiry is not modified when preserveTTL=true
        testlib.post_succ(self.cluster, doc_addr,
                          data={
                              "expiry": 10_000,  # Document expires in 10,000s
                              "preserveTTL": "true"
                          })
        # Expiration is 0 as expiry is not enabled
        assert_eq(self.get_expiration(), 0)

        # Expiry can be modified when preserveTTL=false
        testlib.post_succ(self.cluster, doc_addr,
                          data={
                              "expiry": 10_000,  # Document expires in 10,000s
                              "preserveTTL": "false"
                          })
        # Expiration will be non-zero, as the document will expire at some time
        assert_gt(self.get_expiration(), 0)

        # Expiry does not get reset to default value of 0 when doc updated with
        # preserveTTL=true
        testlib.post_succ(self.cluster, doc_addr,
                          data={
                              "preserveTTL": "true"
                          })
        # Expiration will be non-zero, as the document will expire at some time
        assert_gt(self.get_expiration(), 0)

    def get_expiration(self):
        j = json_response(testlib.get_succ(self.cluster, doc_addr),
                          f"{doc_addr} response was not valid json")
        meta = assert_json_key("meta", j, doc_addr)
        return assert_json_key("expiration", meta, doc_addr)

    def skip_and_limit_test(self):
        def test_one(cmd, val, expected_code):
            url = f"/pools/default/buckets/{bucket_name}/docs/?{cmd}={val}"
            r = testlib.get(self.cluster, url)
            testlib.assert_http_code(expected_code, r)

        test_one('skip', 100, 200)
        test_one('skip', 1001, 400)
        test_one('skip', -1, 400)
        test_one('limit', 1000, 200)
        test_one('limit', 3000, 400)
        test_one('limit', -222, 400)

        # Increase the limits and what previously failed should now
        # succeed. And new highter limits should fail.
        testlib.post_succ(self.cluster, "/internalSettings",
                          data={"maxDocsSkip": 2000,
                                "maxDocsLimit": 3000})
        test_one('skip', 1001, 200)
        test_one('skip', 2001, 400)
        test_one('limit', 3000, 200)
        test_one('limit', 3001, 400)

        # Now decrease the limits.
        testlib.post_succ(self.cluster, "/internalSettings",
                          data={"maxDocsSkip": 200,
                                "maxDocsLimit": 300})
        test_one('skip', 200, 200)
        test_one('skip', 201, 400)
        test_one('limit', 300, 200)
        test_one('limit', 301, 400)

    def content_type_test(self):
        url = f"/pools/default/buckets/{bucket_name}/docs/test"
        # Correct content-type
        testlib.post_succ(self.cluster, url,
                          data={"value": ""},
                          headers={"content-type":
                                   "application/x-www-form-urlencoded"})
        # Correct content-type with additional detail after ";"
        testlib.post_succ(self.cluster, url,
                          data={"value": ""},
                          headers={"content-type":
                                   "application/x-www-form-urlencoded; other"})
        # Invalid content-type specified
        testlib.post_fail(self.cluster, url,
                          data={"value": ""},
                          headers={"content-type": "other"},
                          expected_code=400)
