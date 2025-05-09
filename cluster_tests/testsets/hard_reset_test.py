# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import os
import signal
import testlib
import time
from testlib.requirements import Service


class HardResetTests(testlib.BaseTestSet):
    services_to_run = [Service.KV, Service.INDEX]

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(
            num_nodes=2,
            num_connected=2,
            services=HardResetTests.services_to_run,
            balanced=True)

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        # Verify that the cluster has the correct number of nodes.
        # Note that if this fails then we will cause future testsets to fail.
        assert_cluster_size(self.cluster, 2)
        self.cluster.wait_nodes_up()

    def hard_reset_test_base(self, add_node_fun):
        for i, node in enumerate(self.cluster.connected_nodes):
            other_node = self.cluster.connected_nodes[i ^ 1]
            ns_pid = other_node.get_ns_server_pid()
            otp_other_node = other_node.otp_node()

            try:
                # Forcefully pause the other node's ns_server to be able to
                # exercise unsafe failover (without chronicle quorum)
                os.kill(ns_pid, signal.SIGSTOP)

                # node must take over as orchestrator before attempting to
                # failover other_node (which drops off after SIGSTOP).
                self.cluster.wait_for_orchestrator(node)

                # Hard failover will fail. Poll to wait until the leader lease
                # is acquired by the possibly new orchestrator.
                testlib.poll_for_condition(
                    lambda: self.cluster.failover_node(
                        victim_node = other_node,
                        graceful=False,
                        victim_otp_node=otp_other_node,
                        expected_code=None).text == "Cannot safely perform a "
                    "failover at the moment",
                    sleep_time=0.5,
                    timeout=60,
                    retry_on_assert=True)

                # Force unsafe failover now that node is orchestrator and lease
                # has been acquired.
                self.cluster.failover_node(victim_node=other_node,
                                           graceful=False, allow_unsafe=True,
                                           verbose=True,
                                           victim_otp_node=otp_other_node)
            finally:
                # Resume other_node's ns_server process.
                os.kill(ns_pid, signal.SIGCONT)

            # The node that initiated the failover shouldn't see the other node
            assert_cluster_size(node, 1)

            # The node that went through unsafe failover doesn't receive any
            # notification of its ejection and sees both nodes in the cluster
            assert_cluster_size(other_node, 2)

            # Node addition will fail because unsafe failed over node thinks
            # its part of a cluster
            add_node_fun(other_node, services=HardResetTests.services_to_run,
                         expected_code=400)

            # Node was initialized
            testlib.get_succ(other_node, '/pools/default')

            # Hard reset unsafe failed over node
            testlib.post_succ(other_node, '/controller/hardResetNode')

            # The node that was hard reset may be in the process of leaving the
            # cluster and the web server may have yet to restart. Wait until
            # restart is complete.
            wait_hard_reset_node_up(other_node)

            # Node is now uninitialized
            testlib.get_fail(other_node, '/pools/default', expected_code=404)

            # Node addition/joining cluster will succeed now that the node has
            # been reset
            add_node_fun(other_node, services=HardResetTests.services_to_run)

            self.cluster.rebalance(wait=True, verbose=True)

            # All nodes should see each other now
            assert_cluster_size(node, 2)
            assert_cluster_size(other_node, 2)

    def hard_reset_timeout_before_failover_testbase(self, add_node_fun):
        node = self.cluster.connected_nodes[0]
        other_node = self.cluster.connected_nodes[1]

        testlib.diag_eval(other_node, "ns_config:set({node, node(), "
                          "{timeout, {ns_cluster, hard_reset}}}, 10)")
        r = testlib.post_fail(other_node, '/controller/hardResetNode',
                              expected_code=500)
        assert r.text == 'Request timed out\n'

        # The node that was hard reset may be in the process of leaving the
        # cluster and the web server may have yet to restart. Wait until restart
        # is complete.
        wait_hard_reset_node_up(other_node)

        testlib.get_fail(other_node, '/pools/default', expected_code=404)

        assert_cluster_size(node, 2)

        # The node cannot reach other_node (which was hard reset prior to an
        # unsafe failover). The only fix is to unsafe failover the hard reset
        # node and add it back to the cluster.

        # node must take over as orchestrator before attempting to failover
        # other_node (which stops responding after its hard reset).
        self.cluster.wait_for_orchestrator(node)
        otp_other_node = other_node.otp_node()

        # Force unsafe failover now that node is orchestrator. Occasionally,
        # chronicles times out in electing a leader, retry until there is one.
        testlib.poll_for_condition(
            lambda: self.cluster.failover_node(
                victim_node = other_node,
                graceful=False,
                allow_unsafe=True,
                verbose=True,
                victim_otp_node=otp_other_node).status_code == 200,
            sleep_time=0.5,
            timeout=60,
            retry_on_assert=True)

        # Add node back to original cluster
        add_node_fun(other_node, services=HardResetTests.services_to_run)

        self.cluster.rebalance(wait=True, verbose=True)

        # All nodes should see each other now
        assert_cluster_size(node, 2)
        assert_cluster_size(other_node, 2)

    def hard_reset_add_node_test(self):
        self.hard_reset_test_base(self.cluster.add_node)

    def hard_reset_join_cluster_test(self):
        self.hard_reset_test_base(self.cluster.do_join_cluster)

    def hard_reset_timeout_before_failover_add_node_test(self):
        self.hard_reset_timeout_before_failover_testbase(self.cluster.add_node)

    def hard_reset_timeout_before_failover_join_cluster_test(self):
        self.hard_reset_timeout_before_failover_testbase(
            self.cluster.do_join_cluster)

# Assert that the cluster has the expected size
def assert_cluster_size(cluster, expected_size):
    resp = testlib.get_succ(cluster, "/pools/default")
    nodes = [node["hostname"] for node in resp.json()["nodes"]]
    assert len(nodes) == expected_size, \
        f"Wrong number of nodes in cluster. Expected {expected_size} " \
        f"nodes, found the following set of nodes: {nodes}"

def wait_hard_reset_node_up(node):
    # The hard reset node may still be starting web server after leaving. We
    # should wait for such a node to leave the cluster and start web server.
    def node_is_up(node):
        try:
            resp = testlib.get(node, '/pools/default')
            return 404 == resp.status_code and '"unknown pool"' == resp.text
        except Exception as e:
            print(f'got exception: {e}')
            return False

    testlib.poll_for_condition(
        lambda: node_is_up(node), sleep_time=1, timeout=60,
        msg=f'wait for hard reset node {node} to be up')
