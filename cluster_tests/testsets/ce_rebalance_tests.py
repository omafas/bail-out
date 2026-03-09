#!/usr/bin/env python3
#
# @author Couchbase <info@couchbase.com>
# @copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

from testlib import BaseTestSet, ClusterRequirements, Service


class CERebalanceTests(BaseTestSet):
    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Community",
                                    num_nodes=6,
                                    num_connected=5,
                                    balanced=True,
                                    buckets=[],
                                    exact_services=[Service.KV])]

    def __init__(self, cluster):
        super().__init__(cluster)

    def setup(self):
        pass

    def teardown(self):
        pass

    # This is a combination of three rebalance tests on an all-CE cluster:
    #   1. Rebalance failure when a 6th node is added
    #   2. Swap rebalance success when the resulting cluster state has 5 nodes
    #   3. Rebalance success when the 5th node is in a failed over state and
    #      a 6th node is added
    #
    # This is run as a single test because there is no need to reset the cluster
    # between tests since we want to stay at the boundary case of 5 connected
    # nodes for each test and this condition is never disrupted

    REBALANCE_ERR = "Rebalance was called with 6 Community Edition servers; "\
                    "Cannot rebalance with more than 5 such servers in the "\
                    "cluster."

    def ce_rebalance_test(self):
        RebalanceSuccess = self.cluster.get_counter("rebalance_success")

        # Rebalance should fail when a 6th node is added
        NodeToAdd = self.cluster.disconnected_nodes()[0]
        self.cluster.add_node(NodeToAdd, do_rebalance=False)
        self.cluster.rebalance(initial_code=400,
                               initial_expected_error=self.REBALANCE_ERR)
        self.cluster.poll_for_counter_value("rebalance_success",
                                            RebalanceSuccess)

        self.cluster.smog_check()

        # Swap rebalance
        NodeToRemove = self.cluster.connected_nodes[-1]
        self.cluster.rebalance(ejected_nodes=[NodeToRemove], wait=True)
        RebalanceSuccess += 1
        self.cluster.poll_for_counter_value("rebalance_success",
                                            RebalanceSuccess)

        self.cluster.smog_check()

        # Add a node to a 5 node cluster with one node failed over
        NodeToAdd = self.cluster.disconnected_nodes()[0]
        NodeToFailover = self.cluster.connected_nodes[-1]
        self.cluster.failover_node(NodeToFailover)
        self.cluster.add_node(NodeToAdd, do_rebalance=True)
        RebalanceSuccess += 1
        self.cluster.poll_for_counter_value("rebalance_success",
                                            RebalanceSuccess)

        self.cluster.smog_check()
