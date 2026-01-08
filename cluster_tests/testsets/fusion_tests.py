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
import json
import os
import shutil
import random
import re

from testlib import ClusterRequirements
from testlib.util import Service

class FusionTests(testlib.BaseTestSet):

    def setup(self):
        node = self.cluster.connected_nodes[0]
        tmp_dir = node.tmp_path() + '/logstore'
        backup_dir = node.tmp_path() + '/backup'

        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        self.cluster.logstore_dir = tmp_dir
        self.cluster.backup_dir = backup_dir

    def teardown(self):
        if not os.path.exists(self.cluster.logstore_dir):
            shutil.rmtree(self.cluster.logstore_dir)

    def test_teardown(self):
        testlib.delete_all_buckets(self.cluster)

        # Rebalance the cluster and remove all but one node
        self.cluster.rebalance(self.cluster.connected_nodes[1:], wait=True,
                               verbose=True)
        self.cluster.wait_nodes_up()
        testlib.diag_eval(self.cluster,
                          'chronicle_kv:delete(kv, fusion_config).')
        testlib.diag_eval(self.cluster,
                          'chronicle_kv:delete(kv, fusion_storage_snapshots).')

    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise",
                                    memsize=1024,
                                    include_services=[Service.KV],
                                    min_num_nodes=3, num_connected=1,
                                    num_vbuckets=16, buckets=[])]

    def get_status(self):
        resp = testlib.get_succ(self.cluster, '/fusion/status')
        status = resp.json()
        assert isinstance(status, dict)
        return status

    def assert_state(self, expected):
        assert self.get_status()['state'] == expected

    def wait_for_state(self, intermediate, final):
        def got_state():
            status = self.get_status()
            if status['state'] == final:
                return True
            else:
                assert status['state'] == intermediate
                return False

        testlib.poll_for_condition(got_state, 1, attempts=60,
                                   msg=f"Wait for state to become {final}")

    def assert_bucket_state(self, name, expected):
        bucket = self.cluster.get_bucket(name)
        if expected == 'disabled':
            assert 'fusionState' not in bucket
        else:
            assert bucket['fusionState'] == expected

    def empty_bucket_1_replica_smoke_test(self):
        self.empty_bucket_smoke_test_code(1, 16)

    def empty_bucket_0_replicas_smoke_test(self):
        self.empty_bucket_smoke_test_code(0, 8)

    def init_fusion(self):
        self.assert_state('disabled')
        testlib.post_succ(
            self.cluster, '/settings/fusion',
            json={'logStoreURI': 'local://' + self.cluster.logstore_dir,
                  'enableSyncThresholdMB': 1024})

    def create_bucket(self, name, num_replicas, overrides = {},
                      expected_code=202):
        return self.cluster.create_bucket(
            {'name': name, 'ramQuota': 100, 'bucketType': 'membase',
             'storageBackend': 'magma', 'flushEnabled' : 1,
             'replicaNumber': num_replicas} | overrides,
            sync=True, expected_code=expected_code)

    def prepare_rebalance(self, keep_nodes):
        keep_nodes_string = ",".join(keep_nodes)

        resp = testlib.post_succ(self.cluster,
                                 "/controller/fusion/prepareRebalance",
                                 data={'keepNodes': keep_nodes_string})
        acc_plan = resp.json()
        assert isinstance(acc_plan, dict)
        assert "planUUID" in acc_plan
        assert "nodes" in acc_plan
        assert "logicalSize" in acc_plan
        assert "storageSize" in acc_plan

        plan_nodes = acc_plan["nodes"]
        assert isinstance(plan_nodes, dict)

        total_logical_size = 0
        total_storage_size = 0

        for vbucket_list in plan_nodes.values():
            for vbucket_info in vbucket_list:
                assert "logicalSize" in vbucket_info
                assert "storageSize" in vbucket_info
                total_logical_size += vbucket_info["logicalSize"]
                total_storage_size += vbucket_info["storageSize"]

        assert total_logical_size == acc_plan["logicalSize"]
        assert total_storage_size == acc_plan["storageSize"]

        return acc_plan

    def empty_bucket_smoke_test_code(self, num_replicas, expected_num_volumes):
        self.init_fusion()

        otp_nodes = testlib.get_otp_nodes(self.cluster)
        testlib.post_fail(self.cluster,
                          "/controller/fusion/prepareRebalance",
                          data={'keepNodes': otp_nodes.values()},
                          expected_code=412)

        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')

        second_node = self.cluster.spare_node()
        self.create_bucket('test', num_replicas)

        self.cluster.add_node(second_node, services=[Service.KV])

        otp_nodes = testlib.get_otp_nodes(self.cluster)
        second_otp_node = otp_nodes[second_node.hostname()]

        testlib.post_fail(self.cluster, "/controller/fusion/prepareRebalance",
                          expected_code=400)

        acc_plan = self.prepare_rebalance(otp_nodes.values())
        plan_uuid = acc_plan["planUUID"]
        plan_nodes = acc_plan["nodes"]

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

        self.check_uploaders('test', otp_nodes.values())

        testlib.post_succ(self.cluster, "/controller/fusion/syncLogStore")

        resp = testlib.get_succ(self.cluster, "/fusion/activeGuestVolumes")
        volumes = resp.json()

        for node in otp_nodes.values():
            assert node in volumes

    def empty_volumes_smoke_test(self):
        self.init_fusion()
        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')

        second_node = self.cluster.spare_node()
        self.create_bucket('test', 1)

        self.cluster.add_node(second_node, services=[Service.KV])

        otp_nodes = testlib.get_otp_nodes(self.cluster)
        second_otp_node = otp_nodes[second_node.hostname()]

        acc_plan = self.prepare_rebalance(otp_nodes.values())
        plan_uuid = acc_plan["planUUID"]

        nodes_volumes_json = {'nodes': [{'name': second_otp_node,
                                         'guestVolumePaths': []}]}

        # success
        resp = testlib.post_succ(
            self.cluster,
            f"/controller/fusion/uploadMountedVolumes?planUUID={plan_uuid}",
            json=nodes_volumes_json)

        self.cluster.rebalance(plan_uuid = plan_uuid)

        self.check_uploaders('test', otp_nodes.values())

        resp = testlib.get_succ(self.cluster, "/fusion/activeGuestVolumes")
        volumes = resp.json()
        assert volumes[second_otp_node] == []

    def bucket_flush_smoke_test(self):
        self.init_fusion()
        self.create_bucket('test', 1)
        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')

        resp = testlib.post_succ(
            self.cluster,
            f"/pools/default/buckets/test/controller/doFlush")

    def initial_configuration_test(self):
        testlib.post_fail(self.cluster, '/fusion/enable', expected_code=503)
        self.assert_state('disabled')
        testlib.get_fail(self.cluster, '/settings/fusion', expected_code=404)
        testlib.post_fail(self.cluster, '/settings/fusion', expected_code=400,
                          json={}),
        testlib.post_fail(self.cluster, '/settings/fusion', expected_code=400,
                          json={'something': 'something'}),
        testlib.post_fail(self.cluster, '/settings/fusion', expected_code=400,
                          json={'logStoreURI': 'http://something'}),
        testlib.post_succ(self.cluster, '/settings/fusion',
                          json={'logStoreURI': 's3://something'}),
        testlib.post_succ(self.cluster, '/settings/fusion',
                          json={'logStoreURI': 's3://something/else'}),
        resp = testlib.get_succ(self.cluster, '/settings/fusion')
        config = resp.json()
        assert config == {'logStoreURI': 's3://something/else',
                          'enableSyncThresholdMB': 1024 * 100}
        testlib.post_fail(self.cluster, '/settings/fusion', expected_code=400,
                          json={'enableSyncThresholdMB': 'something'}),
        testlib.post_fail(self.cluster, '/settings/fusion', expected_code=400,
                          json={'enableSyncThresholdMB': 10}),
        testlib.post_fail(self.cluster, '/settings/fusion', expected_code=400,
                          json={'enableSyncThresholdMB': 1024 * 1024 * 11}),
        testlib.post_succ(self.cluster, '/settings/fusion',
                          json={'logStoreURI': 's3://something',
                                'enableSyncThresholdMB': 5000}),
        resp = testlib.get_succ(self.cluster, '/settings/fusion')
        config = resp.json()
        assert config == {'logStoreURI': 's3://something',
                          'enableSyncThresholdMB': 5000}

        self.assert_state('disabled')

        testlib.post_succ(self.cluster, '/fusion/enable')
        self.assert_state('enabling')

        testlib.post_fail(self.cluster, '/fusion/enable', expected_code=503)

    def get_backup_bucket_params(self):
        return {'continuousBackupEnabled': 'true',
                'continuousBackupLocation': self.cluster.backup_dir,
                'historyRetentionSeconds': 8,
                'historyRetentionBytes': 2147483649}

    def enabling_fusion_for_buckets_errors_test(self):
        self.create_bucket('magma', 1)
        self.create_bucket('couchstore', 1, {'bucketType': 'membase',
                                             'storageBackend': 'couchstore'})
        self.create_bucket('PiTR', 1, self.get_backup_bucket_params())
        resp = testlib.post_fail(self.cluster, '/fusion/enable',
                                 expected_code=400)

        assert_buckets_error(resp.json(),
                             {'PiTR': 'bucket with continuous backup enabled'})

        resp = testlib.post_fail(
            self.cluster, '/fusion/enable',
            data={'buckets': 'unknown,couchstore,magma,PiTR'},
            expected_code=400)

        assert_buckets_error(resp.json(),
                             {'PiTR': 'bucket with continuous backup enabled',
                              'unknown': 'not found',
                              'couchstore': 'not a Magma bucket'})

    def mutually_exclusive_bucket_params_test(self):
        self.init_fusion()
        self.create_bucket('test', 1, self.get_backup_bucket_params())
        self.cluster.delete_bucket('test')

        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')

        resp = self.create_bucket(
            'test', 1, self.get_backup_bucket_params(), expected_code=400)
        get_json_error(resp.json(), 'continuousBackupEnabled')

        self.create_bucket(
            'test', 1,
            self.get_backup_bucket_params() | {'fusionEnabled': 'false'})

        self.create_bucket('fusion', 1)

        resp = self.cluster.update_bucket({'name': 'fusion'} |
                                          self.get_backup_bucket_params(),
                                          expected_code=400)
        get_json_error(resp.json(), 'continuousBackupEnabled')

    def abort_rebalance_test(self):
        self.init_fusion()
        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')

        second_node = self.cluster.spare_node()
        self.create_bucket('test', 1)

        self.cluster.add_node(second_node, services=[Service.KV])

        otp_nodes = testlib.get_otp_nodes(self.cluster)
        second_otp_node = otp_nodes[second_node.hostname()]

        testlib.post_fail(
            self.cluster,
            f"/controller/fusion/abortPreparedRebalance?planUUID=wrong",
            expected_code=404)

        acc_plan = self.prepare_rebalance(otp_nodes.values())
        plan_uuid = acc_plan["planUUID"]

        testlib.post_fail(
            self.cluster,
            f"/controller/fusion/abortPreparedRebalance?planUUID=wrong",
            expected_code=400)

        testlib.post_succ(
            self.cluster,
            f"/controller/fusion/abortPreparedRebalance?planUUID={plan_uuid}")

        testlib.post_fail(
            self.cluster,
            f"/controller/fusion/abortPreparedRebalance?planUUID={plan_uuid}",
            expected_code=404)

    def get_namespaces(self, node):
        resp = testlib.diag_eval(
            node,
            '{ok, Json} = ns_memcached:get_fusion_namespaces(' +
            '      fusion_uploaders:get_metadata_store_uri()),' +
            '{Parsed} = ejson:decode(Json),' +
            'Namespaces = proplists:get_value(<<"namespaces">>, Parsed),' +
            'Res =' +
            '  lists:map(' +
            '    fun (Namespace) ->' +
            '      [_, UUID] = string:tokens(binary_to_list(Namespace), "/"),' +
            '      BinUUID = list_to_binary(UUID),' +
            '      {ok, BucketName} = ns_bucket:uuid2bucket(BinUUID),' +
            '      list_to_binary(BucketName)' +
            '    end, Namespaces),' +
            '{json, lists:sort(Res)}.')
        return json.loads(resp.text)

    def assert_namespaces(self, expected):
        nspaces0 = self.get_namespaces(self.cluster.connected_nodes[0])
        nspaces1 = self.get_namespaces(self.cluster.connected_nodes[1])
        assert nspaces0 == expected
        assert nspaces1 == expected

    def enable_disable_stop_test(self):
        self.init_fusion()

        self.create_bucket('test', 1)
        self.create_bucket('test1', 1)

        second_node = self.cluster.spare_node()
        self.cluster.add_node(second_node, services=[Service.KV])
        self.cluster.rebalance()
        self.assert_namespaces([])

        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')
        self.assert_bucket_state('test', 'enabled')
        self.assert_bucket_state('test1', 'enabled')
        self.assert_namespaces(['test', 'test1'])

        testlib.post_succ(self.cluster, '/fusion/disable')
        self.wait_for_state('disabling', 'disabled')
        self.assert_bucket_state('test', 'disabled')
        self.assert_bucket_state('test1', 'disabled')
        self.assert_namespaces([])

        testlib.post_succ(self.cluster, '/fusion/enable',
                          data={'buckets': 'test'})
        self.wait_for_state('enabling', 'enabled')
        self.assert_bucket_state('test', 'enabled')
        self.assert_bucket_state('test1', 'disabled')
        self.assert_namespaces(['test'])

        testlib.post_succ(self.cluster, '/fusion/stop')
        self.wait_for_state('stopping', 'stopped')
        self.assert_bucket_state('test', 'stopped')
        self.assert_bucket_state('test1', 'disabled')
        self.assert_namespaces(['test'])

        testlib.post_succ(self.cluster, '/fusion/enable',
                          data={'buckets': 'test'})
        self.wait_for_state('enabling', 'enabled')
        self.assert_bucket_state('test', 'enabled')
        self.assert_bucket_state('test1', 'disabled')
        self.assert_namespaces(['test'])

        testlib.post_succ(self.cluster, '/fusion/stop')
        self.wait_for_state('stopping', 'stopped')
        self.assert_bucket_state('test', 'stopped')
        self.assert_bucket_state('test1', 'disabled')
        self.assert_namespaces(['test'])

        testlib.post_succ(self.cluster, '/fusion/disable')
        self.wait_for_state('disabling', 'disabled')
        self.assert_bucket_state('test', 'disabled')
        self.assert_bucket_state('test1', 'disabled')
        self.assert_namespaces([])

        metrics = testlib.get_prometheus_metrics(self.cluster)
        verify_gauge_metric(metrics, "cm_fusion_state_timestamp_seconds",
                            [(('state', 'disabled'),),
                             (('state', 'disabling'),),
                             (('state', 'enabled'),),
                             (('state', 'enabling'),),
                             (('state', 'stopped'),),
                             (('state', 'stopping'),)])

        verify_gauge_metric(metrics, "cm_fusion_bucket_state_timestamp_seconds",
                            [(('bucket', 'test'), ('state', 'disabling')),
                             (('bucket', 'test'), ('state', 'disabled')),
                             (('bucket', 'test'), ('state', 'enabled')),
                             (('bucket', 'test'), ('state', 'stopping'),),
                             (('bucket', 'test'), ('state', 'stopped'),),
                             (('bucket', 'test1'), ('state', 'enabled')),
                             (('bucket', 'test1'), ('state', 'disabling')),
                             (('bucket', 'test1'), ('state', 'disabled'))])

    def get_snapshot_uuids(self):
        resp = testlib.diag_eval(
            self.cluster,
            'List = fusion_uploaders:get_stored_snapshot_uuids(),' +
            'JsonList = [{[{plan_uuid, PlanUUID},' +
            '              {bucket_uuid, BucketUUID},' +
            '              {num_vbuckets, NVBuckets}]} ||' +
            '                  {PlanUUID, BucketUUID, NVBuckets} <- List],' +
            '{json, JsonList}.')
        return json.loads(resp.text)

    def get_uploaders(self, bucket):
        resp = testlib.diag_eval(
            self.cluster,
            f'Res = ns_bucket:get_fusion_uploaders("{bucket}"),' +
            '{json, [{[{node, N}, {term, T}]} || {N, T} <- Res]}.')
        return json.loads(resp.text)

    def check_uploaders(self, bucket, nodes):
        uploaders = self.get_uploaders(bucket)
        uploader_nodes = set(uploader['node'] for uploader in uploaders)
        assert all(uploader['node'] in nodes for uploader in uploaders)
        assert all(node in uploader_nodes for node in nodes)

    def snapshot_management_test(self):
        self.init_fusion()
        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')

        second_node = self.cluster.spare_node()
        self.create_bucket('test', 1)
        bucket_uuid = self.cluster.get_bucket_uuid('test')

        self.cluster.add_node(second_node, services=[Service.KV])

        otp_nodes = testlib.get_otp_nodes(self.cluster)

        acc_plan = self.prepare_rebalance(otp_nodes.values())
        plan_uuid = acc_plan["planUUID"]
        plan_nodes = acc_plan["nodes"]

        uuids = self.get_snapshot_uuids()

        assert len(uuids) == 1
        snapshot_uuid = uuids[0]
        assert snapshot_uuid["plan_uuid"] == plan_uuid
        assert snapshot_uuid["bucket_uuid"] == bucket_uuid
        assert snapshot_uuid["num_vbuckets"] == 16

    def swap_rebalance_test(self):
        self.init_fusion()

        self.create_bucket('test', 1)

        disconnected_nodes = self.cluster.disconnected_nodes()
        second_node = disconnected_nodes[0]
        third_node = disconnected_nodes[1]

        self.cluster.add_node(second_node, services=[Service.KV])
        self.cluster.rebalance()

        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')

        self.cluster.add_node(third_node, services=[Service.KV])
        keep_nodes = [self.cluster.connected_nodes[0].otp_node(),
                      third_node.otp_node()]
        acc_plan = self.prepare_rebalance(keep_nodes)
        plan_uuid = acc_plan["planUUID"]

        resp = testlib.post_succ(
            self.cluster,
            f"/controller/fusion/uploadMountedVolumes?planUUID={plan_uuid}",
            json=generate_nodes_volumes([third_node.otp_node()]))

        self.cluster.rebalance(ejected_nodes=[second_node],
                               plan_uuid = plan_uuid,
                               wait = True)
        self.check_uploaders('test', keep_nodes)

    def prepare_2_nodes_one_bucket(self):
        self.init_fusion()
        self.create_bucket('test', 1)

        first_node = self.cluster.connected_nodes[0]

        disconnected_nodes = self.cluster.disconnected_nodes()
        second_node = disconnected_nodes[0]

        self.cluster.add_node(second_node, services=[Service.KV])
        self.cluster.rebalance()

        testlib.post_succ(self.cluster, '/fusion/enable')
        self.wait_for_state('enabling', 'enabled')

        return first_node, second_node

    def rebalance_out_the_last_replica_test(self):
        first_node, second_node = self.prepare_2_nodes_one_bucket()
        first_node_otp = first_node.otp_node()

        acc_plan = self.prepare_rebalance([first_node_otp])

        plan_nodes = acc_plan["nodes"]
        assert len(plan_nodes) == 0

        plan_uuid = acc_plan["planUUID"]

        resp = testlib.post_succ(
            self.cluster,
            f"/controller/fusion/uploadMountedVolumes?planUUID={plan_uuid}",
            json={'nodes': []})

        self.cluster.rebalance(ejected_nodes=[second_node],
                               plan_uuid = plan_uuid,
                               wait = True)

        self.check_uploaders('test', [first_node_otp])

    def rebalance_with_trivial_moves_test(self):
        first_node, second_node = self.prepare_2_nodes_one_bucket()
        self.cluster.failover_node(second_node, graceful=True)
        self.cluster.eject_node(second_node, second_node)
        self.cluster.rebalance(wait = True)
        self.check_uploaders('test', [first_node.otp_node()])

def assert_buckets_error(json, expected):
    error_message = get_json_error(json, 'buckets')

    prefix = "Fusion cannot be enabled on the following buckets: "
    assert error_message.startswith(prefix), (
        f"Error message should start with '{prefix}', got: {error_message}"
    )
    content = error_message[len(prefix):].strip()
    # match substrings in a comma-separated list where individual
    # items follow a "key - value" format,
    pattern = r'([^,]+?)\s*-\s*(.+?)(?:,\s*|$)'
    matches = re.findall(pattern, content)

    actual = {}
    for bucket, reason in matches:
        bucket = bucket.strip()
        reason = reason.strip()
        actual[bucket] = reason

    assert actual == expected, (
        f"Parsed errors don't match.\nExpected: {expected}\nGot: {actual}"
    )

def get_json_error(json, field):
    assert isinstance(json, dict)
    assert "errors" in json

    errors = json["errors"]
    assert isinstance(errors, dict)
    assert field in errors

    value = errors[field]
    assert isinstance(value, str)
    return value

def assert_json_error(json, field, prefix):
    value = get_json_error(json, field)
    assert value.startswith(prefix)


def generate_nodes_volumes(nodes):
    gv_paths = ["/tmp/data1", "/tmp/data2", "/tmp/data3"]
    nodes_volumes = [{'name': n,
                      'guestVolumePaths': random.sample(gv_paths,
                                                        random.randint(1, 2))}
                     for n in nodes]
    return {'nodes': nodes_volumes}

def verify_gauge_metric(metrics, metric, labels):
    assert metric in metrics
    mdict = metrics[metric]
    assert mdict["TYPE"] == "gauge"
    assert mdict["HELP"] != "Help is missing"
    vdict = mdict["VALUES"]

    for label_tuple in labels:
        assert label_tuple in vdict
