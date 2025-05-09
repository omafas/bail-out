# @author Couchbase <info@couchbase.com>
# @copyright 2025 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import testlib
import os
import shutil
import random

from testlib import ClusterRequirements
from testlib.util import Service

class FusionTests(testlib.BaseTestSet):

    def setup(self):
        node = self.cluster.connected_nodes[0]
        tmp_dir = node.tmp_path() + '/logstore'

        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        self.cluster.logstore_dir = tmp_dir

    def teardown(self):
        if not os.path.exists(self.cluster.logstore_dir):
            shutil.rmtree(self.cluster.logstore_dir)

    def test_teardown(self):
        testlib.delete_all_buckets(self.cluster)

        # Rebalance the cluster and remove all but one node
        self.cluster.rebalance(self.cluster.connected_nodes[1:], wait=True,
                               verbose=True)
        self.cluster.wait_nodes_up()

    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise", services=[Service.KV],
                                    min_num_nodes=2, num_connected=1,
                                    num_vbuckets=16, buckets=[])]

    def empty_bucket_1_replica_smoke_test(self):
        self.empty_bucket_smoke_test_code(1, 16)

    def empty_bucket_0_replicas_smoke_test(self):
        self.empty_bucket_smoke_test_code(0, 8)

    def empty_bucket_smoke_test_code(self, num_replicas, expected_num_volumes):
        second_node = self.cluster.spare_node()

        self.cluster.create_bucket(
            {'name': 'test', 'ramQuota': 100, 'bucketType': 'membase',
             'storageBackend': 'magma',
             'replicaNumber': num_replicas,
             'fusionLogstoreURI': 'local://' + self.cluster.logstore_dir},
            sync=True)

        self.cluster.add_node(second_node, services=[Service.KV])

        otp_nodes = testlib.get_otp_nodes(self.cluster)
        second_otp_node = otp_nodes[second_node.hostname()]
        keep_nodes_string = ",".join(otp_nodes.values())

        testlib.post_fail(self.cluster, "/controller/fusion/prepareRebalance",
                          expected_code=400)

        resp = testlib.post_succ(self.cluster,
                                 "/controller/fusion/prepareRebalance",
                                 data={'keepNodes': keep_nodes_string})
        acc_plan = resp.json()

        assert isinstance(acc_plan, dict)
        assert "planUUID" in acc_plan
        plan_uuid = acc_plan["planUUID"]

        assert "nodes" in acc_plan
        plan_nodes = acc_plan["nodes"]

        assert isinstance(plan_nodes, dict)
        assert len(plan_nodes) == 1
        assert second_otp_node in plan_nodes
        volumes = plan_nodes[second_otp_node]
        len_volumes = len(volumes)
        assert len_volumes == expected_num_volumes

        # not enough nodes
        resp = testlib.post_fail(
            self.cluster,
            f"/controller/fusion/uploadMountedVolumes?planUUID={plan_uuid}",
            expected_code=400,
            json=generate_nodes_volumes(["n34@wrong"]))

        assert_json_error(resp.json(), "nodes", "Absent nodes")

        # too many nodes
        resp = testlib.post_fail(
            self.cluster,
            f"/controller/fusion/uploadMountedVolumes?planUUID={plan_uuid}",
            expected_code=400,
            json=generate_nodes_volumes(otp_nodes.values()))

        assert_json_error(resp.json(), "nodes", "Unneeded nodes")

        correct_nodes = [second_otp_node]

        # wrong planUUID
        resp = testlib.post_fail(
            self.cluster,
            f"/controller/fusion/uploadMountedVolumes?planUUID=12345",
            expected_code=400,
            json=generate_nodes_volumes(correct_nodes))

        assert_json_error(resp.json(),
                          "planUUID", "Doesn't match stored plan id")

        # success
        resp = testlib.post_succ(
            self.cluster,
            f"/controller/fusion/uploadMountedVolumes?planUUID={plan_uuid}",
            json=generate_nodes_volumes(correct_nodes))

        self.cluster.rebalance(plan_uuid = plan_uuid)

        testlib.post_succ(self.cluster, "/controller/fusion/syncLogStore")

        resp = testlib.get_succ(self.cluster, "/fusion/activeGuestVolumes")
        volumes = resp.json()

        for node in otp_nodes.values():
            assert node in volumes

def assert_json_error(json, field, prefix):
    assert isinstance(json, dict)
    assert len(json) == 1
    assert "errors" in json

    errors = json["errors"]
    assert isinstance(errors, dict)
    assert len(errors) == 1
    assert field in errors

    value = errors[field]
    assert isinstance(value, str)
    assert value.startswith(prefix)


def generate_nodes_volumes(nodes):
    gv_paths = ["/tmp/data1", "/tmp/data2", "/tmp/data3"]
    nodes_volumes = [{'name': n,
                      'guestVolumePaths': random.sample(gv_paths,
                                                        random.randint(1, 2))}
                     for n in nodes]
    return {'nodes': nodes_volumes}
