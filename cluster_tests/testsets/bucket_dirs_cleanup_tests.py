# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
import binascii
import shutil
import os
from pathlib import Path
from testlib import ClusterRequirements
from testlib.test_tag_decorator import tag, Tag
from testlib.util import Service
from testsets.node_addition_tests import assert_cluster_size


bucket_name = "bucket-test"

class BucketDirsCleanupTests(testlib.BaseTestSet):
    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise",
                                    num_nodes=3, num_connected=1,
                                    buckets=[{"name": bucket_name,
                                              "ramQuota": 100}])]

    def setup(self):
        self.fake_bucket_dirs = [
            # This one looks like a bucket name (pre-8.0 bucket dir)
            testlib.random_str(8),
            # This one looks like a bucket UUID (post-8.0 bucket dir)
            binascii.hexlify(os.urandom(16)).decode()
        ]
        self.to_garbage_collect = []

    def teardown(self):
        pass

    def test_teardown(self):
        print("Removing all but the first node")
        # Rebalance the cluster and remove all but one node
        self.cluster.rebalance(self.cluster.connected_nodes[1:], wait=True,
                               verbose=True)

        assert_cluster_size(self.cluster, 1)
        self.cluster.wait_nodes_up()

        cleanup_fake_bucket_dirs(self.cluster, self.fake_bucket_dirs)
        for file in self.to_garbage_collect:
            if file.is_dir():
                shutil.rmtree(file)
            else:
                file.unlink(missing_ok=True)

    @tag(Tag.LowUrgency)
    def cleanup_during_rebalance_test(self):
        assert_cluster_size(self.cluster, 1)
        cluster_node = self.cluster.connected_nodes[0]
        new_node1 = self.cluster.disconnected_nodes()[0]
        new_node2 = self.cluster.disconnected_nodes()[1]
        bucket_uuid = self.cluster.get_bucket_uuid(bucket_name)
        # This dir should not exist after test finishes
        # (these nodes will be removed from the cluster eventually)
        self.to_garbage_collect.append(Path(new_node1.dbdir()) / bucket_uuid)
        self.to_garbage_collect.append(Path(new_node2.dbdir()) / bucket_uuid)

        # Create fake bucket dirs on all nodes, those dirs are supposed to be
        # removed during rebalance
        create_fake_bucket_dirs(self.cluster, self.fake_bucket_dirs)

        existing_bucket_dir = Path(cluster_node.dbdir()) / bucket_uuid
        testlib.poll_for_condition(
            lambda: existing_bucket_dir.exists(), timeout=30, sleep_time=0.5
        )

        self.create_marker_file(cluster_node.dbdir(), bucket_uuid)
        self.create_marker_file(new_node1.dbdir(), bucket_uuid)
        self.create_marker_file(new_node2.dbdir(), bucket_uuid)

        self.cluster.add_node(new_node1, services=[Service.KV])
        self.cluster.add_node(new_node2, services=[Service.INDEX])
        self.cluster.rebalance(wait=True)
        assert_cluster_size(self.cluster, 3)

        # The file should exist on the nodes that existed before the rebalance
        # and on the new node that is not a kv node
        self.assert_marker_file_exists(cluster_node.dbdir(), bucket_uuid)
        self.assert_marker_file_not_exists(new_node1.dbdir(), bucket_uuid)
        self.assert_marker_file_exists(new_node2.dbdir(), bucket_uuid)

        # Verify that the previous bucket dirs have been removed
        # during node addition + rebalance
        assert_unused_bucket_dirs_removed(self.cluster, self.fake_bucket_dirs)

    def create_marker_file(self, where, bucket_uuid):
        bucket_dir = Path(where) / bucket_uuid
        bucket_dir.mkdir(parents=True, exist_ok=True)
        marker_file = bucket_dir / "test_marker_file"
        self.to_garbage_collect.append(marker_file)
        marker_file.touch()

    def assert_marker_file_exists(self, where, bucket_uuid):
        f = Path(where) / bucket_uuid / "test_marker_file"
        assert f.exists(), f"File {f} should exist in the bucket dir"

    def assert_marker_file_not_exists(self, where, bucket_uuid):
        f = Path(where) / bucket_uuid / "test_marker_file"
        assert not f.exists(), f"File {f} should not exist in the bucket dir"


def create_fake_bucket_dirs(cluster, fake_bucket_dirs):
    for node in cluster._nodes:
        for fake_bucket_dir in fake_bucket_dirs:
            full_dir_path = Path(node.dbdir()) / fake_bucket_dir
            full_dir_path.mkdir(parents=True, exist_ok=True)
            # Create a file in the bucket dir to make it non-empty
            (full_dir_path / 'test_data_file').touch()
            print(f'Created fake bucket dir {full_dir_path}')


def assert_unused_bucket_dirs_removed(cluster, fake_bucket_dirs):
    for node in cluster._nodes:
        for fake_bucket_dir in fake_bucket_dirs:
            full_dir_path = Path(node.dbdir()) / fake_bucket_dir
            print(f'Checking if {full_dir_path} exists')
            assert not full_dir_path.exists(), \
                f'{full_dir_path} should have been removed during rebalance'


def cleanup_fake_bucket_dirs(cluster, fake_bucket_dirs):
    for node in cluster._nodes:
        for fake_bucket_dir in fake_bucket_dirs:
            full_dir_path = Path(node.dbdir()) / fake_bucket_dir
            if full_dir_path.exists():
                shutil.rmtree(full_dir_path)

