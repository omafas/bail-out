# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib


class RestEjectTest(testlib.BaseTestSet):
    def __init__(self, cluster):
        super().__init__(cluster)

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes=3)

    def setup(self):
        testlib.delete_all_buckets(self.cluster)
        bucket = {"name": "testbucket", "ramQuota": "200"}

        # Wait for the bucket to be ready on all nodes to be able to
        # handle an immediate failover of a node.
        self.cluster.create_bucket(bucket, sync=True)

    def teardown(self):
        testlib.delete_all_buckets(self.cluster)

    def rest_reject_test(self):
        failover_node = self.cluster.connected_nodes[0]
        otp_name = failover_node.otp_node()

        self.cluster.failover_node(failover_node, graceful=False)
        # Eject failed over node via REST endpoint and verify it can be
        # added back in after ejection
        data = {"otpNode": f"{otp_name}"}
        self.cluster.eject_node(failover_node, self.cluster.connected_nodes[1])
        self.cluster.rebalance(wait=True)
        self.cluster.add_node(failover_node)
        self.cluster.rebalance(wait=True)

        # The previously failed over ejected node was added back in
        # and should not be allowed to be ejected because it is active
        testlib.post_fail(self.cluster.connected_nodes[0],
                          '/controller/ejectNode',
                          expected_code=400, data=data)
