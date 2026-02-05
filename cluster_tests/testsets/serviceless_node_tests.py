# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
from testlib import ClusterRequirements
import time

from testlib.test_tag_decorator import tag, Tag


# Testing of serviceless nodes (nodes that don't have any services, e.g.
# kv, index, etc. configured).


class ServicelessNodeTests(testlib.BaseTestSet):

    def __init__(self, cluster):
        super().__init__(cluster)
        self.req_num_nodes = None

    def setup(self):
        self.req_num_nodes = len(self.cluster._nodes)

    def teardown(self):
        pass

    def test_teardown(self):
        print("Tearing down self.cluster")
        # Rebalance the cluster and remove all but one node
        self.cluster.rebalance(self.cluster.connected_nodes[1:], wait=True,
                               verbose=True)
        # Wait for the number of remaining nodes in the cluster to reach one.
        retries = 60
        while retries > 0:
            resp = testlib.get_succ(self.cluster, "/pools/default")
            current_nodes = [node["hostname"] for node in resp.json()["nodes"]]
            print(f"Nodes currently in cluster: {current_nodes}")
            if len(resp.json()["nodes"]) == 1:
                self.cluster.wait_nodes_up()
                return

            print(f'More than one node in cluster after removing all but one '
                  f'node. Retrying up to {retries} more times')
            time.sleep(0.5)
            retries -= 1

        raise RuntimeError("More than one node in cluster after test teardown")

    @staticmethod
    def requirements():
        return ClusterRequirements(
            edition="Enterprise",
            min_num_nodes=3,
            num_connected=1,
            afamily="ipv4",
            num_vbuckets=16,
            buckets=[{"name": "testbucket",
                      "ramQuota": 200}])

    def joinNodes(self):
        for node in self.cluster.disconnected_nodes():
            print(f'Joining {node.hostname()} to cluster')
            self.cluster.do_join_cluster(node, services=[])
        self.cluster.rebalance(wait=True)
        resp = testlib.get_succ(self.cluster, "/pools/default")

        return len(resp.json()["nodes"]) == self.req_num_nodes, \
            f'Length of nodes: {len(resp.json()["nodes"])}'

    def addNodes(self):
        for node in self.cluster.disconnected_nodes():
            print(f'Adding {node.hostname()} to cluster')
            self.cluster.add_node(node, services=[])
        self.cluster.rebalance(wait=True)
        resp = testlib.get_succ(self.cluster, "/pools/default")

        return len(resp.json()["nodes"]) == self.req_num_nodes, \
            f'Length of nodes: {len(resp.json()["nodes"])}'

    def failover_and_recover_node(self):
        node = self.cluster.connected_nodes[-1]
        print(f'Failover {node.hostname()} from cluster')
        # Graceful failovers are not supported for serviceless nodes. We
        # don't do a rebalance as that would remove the node.
        resp = self.cluster.failover_node(node, graceful=False, verbose=True)

        print(f'Re-adding {node.hostname()} back into cluster')

        resp = self.cluster.recover_node(node, recovery_type="full",
                                         do_rebalance=True, verbose=True)

    def remove_orchestrator_node(self):
        orchestrator_node = self.cluster.wait_for_orchestrator()
        self.cluster.rebalance(ejected_nodes=[orchestrator_node], wait=True,
                               verbose=True)

    def verify_orchestrator_node(self, must_be_serviceless_node=True):
        retries = 60
        while retries > 0:
            orchestrator_hostname, is_serviceless = \
                self.cluster.get_orchestrator_node()
            if orchestrator_hostname != "" and \
               is_serviceless == must_be_serviceless_node:
                return
            time.sleep(0.5)
            retries -= 1

        if orchestrator_hostname == "":
            raise RuntimeError("Failed to determine orchestator")
        else:
            reason = "not" if must_be_serviceless_node else ""
            raise RuntimeError(f"Orchestrator node {orchestrator_hostname} is "
                               f"unexpectedly {reason} serviceless")

    # These tests are based on a cluster which has the initial node with
    # services and two additional serviceless nodes.
    @tag(Tag.LowUrgency)
    def addnode_test(self):
        self.verify_orchestrator_node(False)
        self.addNodes()
        self.verify_orchestrator_node(True)
        self.failover_and_recover_node()
        self.verify_orchestrator_node(True)
        # Removing the orchestrator node should result in the remaining
        # serviceless node becoming the orchestrator
        self.remove_orchestrator_node()
        self.verify_orchestrator_node(True)

    @tag(Tag.LowUrgency)
    def joincluster_test(self):
        self.verify_orchestrator_node(False)
        self.joinNodes()
        self.verify_orchestrator_node(True)
        self.failover_and_recover_node()
        self.verify_orchestrator_node(True)
        # Removing the orchestrator node should result in the remaining
        # serviceless node becoming the orchestrator
        self.remove_orchestrator_node()
        self.verify_orchestrator_node(True)
