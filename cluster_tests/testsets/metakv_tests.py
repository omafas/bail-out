# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
import base64

TESTKEY = "/test_key"

class MetakvTests(testlib.BaseTestSet):
    def __init__(self, cluster):
        super().__init__(cluster)

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        testlib.metakv_delete_succ(self.cluster, TESTKEY)

    def metakv_test(self):
        # Test basic flow without revision (should work)
        testlib.metakv_put_succ(self.cluster, TESTKEY,
                                data = {'value': 'value'})
        testlib.metakv_get_succ(self.cluster, TESTKEY)
        testlib.metakv_delete_succ(self.cluster, TESTKEY)

        testlib.metakv_put_succ(self.cluster, TESTKEY,
                                data = {'value': 'value'})

        # Test CAS behavior - successful update with matching revision
        response = testlib.metakv_get_succ(self.cluster, TESTKEY)
        rev = response.json()["rev"]
        testlib.metakv_put_succ(
            self.cluster, TESTKEY, data={"value": "value1", "rev": rev}
        )

        # Test CAS behavior - failed update with stale revision
        testlib.metakv_put_fail(
            self.cluster,
            TESTKEY,
            409,
            data={"value": "should_fail", "rev": rev},
        )

        # Test successful update with current revision
        response = testlib.metakv_get_succ(self.cluster, TESTKEY)
        current_rev = response.json()["rev"]
        testlib.metakv_put_succ(
            self.cluster, TESTKEY, data={"value": "value2", "rev": current_rev}
        )

        # Test delete with revision
        response = testlib.metakv_get_succ(self.cluster, TESTKEY)
        final_rev = response.json()["rev"]
        testlib.metakv_delete_succ(
            self.cluster, TESTKEY, params={"rev": final_rev}
        )

        # Test delete with stale revision (should fail with 409)
        testlib.metakv_put_succ(self.cluster, TESTKEY, data={"value": "value3"})
        response = testlib.metakv_get_succ(self.cluster, TESTKEY)
        current_rev = response.json()["rev"]

        # Try to delete with stale revision
        testlib.metakv_delete_fail(
            self.cluster, TESTKEY, 409, params={"rev": final_rev}
        )

        # Delete with current revision should succeed
        testlib.metakv_delete_succ(
            self.cluster, TESTKEY, params={"rev": current_rev}
        )
