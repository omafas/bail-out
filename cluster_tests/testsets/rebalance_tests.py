#!/usr/bin/env python3
#
# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
import testlib.util

from testlib import ClusterRequirements, Service
from testsets import native_encryption_tests
from testsets.sample_buckets import SampleBucketTasksBase


class KVRebalanceTests(testlib.BaseTestSet):
    bucket_name = 'default'
    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise",
                                    min_num_nodes=3, num_connected=2,
                                    balanced=True,
                                    buckets=[],
                                    exact_services=[Service.KV])]

    def __init__(self, cluster):
        super().__init__(cluster)

    def setup(self):
        pass

    def teardown(self):
        if (len(self.cluster.connected_nodes) < 2):
            # We should never have 0 nodes, so just add one back.
            self.cluster.add_node(self.cluster.disconnected_nodes()[0],
                                  services=[Service.KV],
                                  wait=True)
        elif (len(self.cluster.connected_nodes) > 2):
            # Remove all but 2 nodes, bringing us back to the original state.
            self.cluster.rebalance(
                ejected_nodes = self.cluster.connected_nodes[2:],
                wait=True)

    # This is really 3 tests:
    # 1. Rebalance in
    # 2. Rebalance out
    # 3. Swap rebalance
    #
    # This is run as a single test because we do not want to reset the cluster
    # between each test, it wastes time and /should/ not be needed.
    def rebalance_test(self):
        RebalanceSuccess = self.cluster.get_counter("rebalance_success")

        NodeToAdd = self.cluster.disconnected_nodes()[0]
        self.cluster.add_node(NodeToAdd, do_rebalance=True)
        RebalanceSuccess += 1
        self.cluster.poll_for_counter_value("rebalance_success",
                                            RebalanceSuccess)

        self.cluster.smog_check()

        NodeToRemove = self.cluster.connected_nodes[2]
        self.cluster.rebalance(ejected_nodes=[NodeToRemove], wait=True)
        RebalanceSuccess += 1
        self.cluster.poll_for_counter_value("rebalance_success",
                                            RebalanceSuccess)
        self.cluster.smog_check()

        NodeToAdd = self.cluster.disconnected_nodes()[0]
        NodeToRemove = self.cluster.connected_nodes[1]
        self.cluster.add_node(NodeToAdd)
        self.cluster.rebalance(ejected_nodes=[NodeToRemove], wait=True)

        RebalanceSuccess += 1
        self.cluster.poll_for_counter_value("rebalance_success",
                                            RebalanceSuccess)

        self.cluster.smog_check()


class KVFileBasedRebalanceTests(KVRebalanceTests):
    def setup(self):
        super().setup()
        testlib.post_succ(self.cluster, "/internalSettings",
                          data={"dataServiceFileBasedRebalanceEnabled": "true"})

        self.cluster.create_bucket({'name': self.bucket_name,
                                    'storageBackend': 'magma',
                                    'replicaNumber': 1,
                                    'ramQuotaMB': '100'}, sync=True)

    def teardown(self):
        testlib.post_succ(self.cluster, "/internalSettings",
                          data={"dataServiceFileBasedRebalanceEnabled":
                                "false"})

        testlib.delete_all_buckets(self.cluster)
        super().teardown()


class KVFileBasedRebalanceEncryptionTests(KVFileBasedRebalanceTests):
    def setup(self):
        super().setup()
        self.setup_encryption()

    def setup_encryption(self):
        secret_json = native_encryption_tests.cb_managed_secret(
            name='Test Secret')
        secret_id = native_encryption_tests.create_secret(
            self.cluster.connected_nodes[0],
            secret_json)

        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestKeyId': secret_id})

        native_encryption_tests.force_bucket_encryption(self.cluster,
                                                        self.bucket_name)

        native_encryption_tests.poll_verify_cluster_bucket_dek_info(
            self.cluster, self.bucket_name, data_statuses=['encrypted'])

    def teardown(self):
        testlib.delete_all_buckets(self.cluster)
        native_encryption_tests.delete_all_secrets(self.cluster)
        super().teardown()


class KVFileBasedRebalanceSampleBucketEncryptionTests(
        KVFileBasedRebalanceEncryptionTests, SampleBucketTasksBase):
    def __init__(self, cluster):
        super().__init__(cluster)
        SampleBucketTasksBase.__init__(self)

    def setup(self):
        self.bucket_name = 'travel-sample'
        self.load_and_assert_sample_bucket(self.cluster, self.bucket_name)

        self.setup_encryption()
