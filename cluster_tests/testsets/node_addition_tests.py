# @author Couchbase <info@couchbase.com>
# @copyright 2023 Couchbase, Inc.
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
from subprocess import Popen
from testlib.util import Service
import testlib
import os
from testlib import ClusterRequirements
from testsets.sample_buckets import SampleBucketTasksBase
from testsets.cert_load_tests import read_cert_file, generate_node_certs, \
     load_ca, load_node_cert, load_client_cert, generate_internal_client_cert, \
     to_pkcs8

class NodeAdditionCommunityTests(testlib.BaseTestSet, SampleBucketTasksBase):

    def __init__(self, cluster):
        super().__init__(cluster)
        SampleBucketTasksBase.__init__(self)

    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Community",
                                    memsize=256,
                                    min_num_nodes=3, num_connected=1,
                                    encryption=False, afamily='ipv4',
                                    exact_services=[Service.KV])]

    def setup(self):
        self.sample_bucket = "beer-sample"
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        self.cluster.delete_bucket(self.sample_bucket)
        if len(self.cluster.connected_nodes) > 1:
            self.cluster.rebalance(self.cluster.connected_nodes[1:], wait=True,
                                verbose=True)
            assert_cluster_size(self.cluster, 1)
        self.cluster.wait_nodes_up()

    def basic_node_addition_with_rebalance_test(self):
        self.load_and_assert_sample_bucket(self.cluster, self.sample_bucket)
        nodes_to_add = self.cluster.disconnected_nodes()
        assert len(nodes_to_add) >= 2, "Expected at least 2 nodes to add"

        # Add nodes using both addition methods
        self.cluster.add_node(nodes_to_add[0])
        self.cluster.do_join_cluster(nodes_to_add[1])
        self.cluster.rebalance(wait=True)
        assert_cluster_size(self.cluster, 3)

        # Remove nodes from the cluster
        self.cluster.rebalance(self.cluster.connected_nodes[1:], wait=True,
                               verbose=True)
        assert_cluster_size(self.cluster, 1)
        self.cluster.wait_nodes_up()

        # Add nodes to the cluster again, primarily to test bucket cleanup
        # procedures (bucket data that is left after first node removal)
        self.cluster.add_node(nodes_to_add[0])
        self.cluster.do_join_cluster(nodes_to_add[1])
        self.cluster.rebalance(wait=True)
        assert_cluster_size(self.cluster, 3)


class NodeAdditionTests(testlib.BaseTestSet):

    def __init__(self, cluster):
        super().__init__(cluster)
        self.req_num_nodes = None

    def setup(self):
        self.req_num_nodes = len(self.cluster._nodes)

    def teardown(self):
        pass

    def test_teardown(self):
        print("Removing all but the first node")
        # Rebalance the cluster and remove all but one node
        self.cluster.rebalance(self.cluster.connected_nodes[1:], wait=True,
                               verbose=True)

        # Verify that the cluster has the correct number of nodes.
        # Note that if this fails then we will cause future testsets to fail.
        assert_cluster_size(self.cluster, 1)
        self.cluster.wait_nodes_up()

    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise",
                                    min_num_nodes=3, num_connected=1,
                                    afamily="ipv4",
                                    exact_services=[Service.KV]),
                ClusterRequirements(edition="Enterprise",
                                    min_num_nodes=3, num_connected=1,
                                    afamily="ipv6",
                                    exact_services=[Service.KV])]

    def n2n_test_base(self, method, enable: bool):
        """
        A base structure of testing node to node encryption with adding nodes.
        :param method: which REST request to add nodes:
                       cluster.add_node/cluster.do_join_cluster
        :param enable: whether node-to-node encryption is enabled or disabled
        """
        # Change the node-to-node settings.
        self.cluster.toggle_n2n_encryption(enable=enable)

        # Test a node addition method
        for node in self.cluster.disconnected_nodes():
            method(node)
        self.cluster.rebalance(wait=True)

        # Verify that the cluster has the correct number of nodes
        assert_cluster_size(self.cluster, self.req_num_nodes)

        # Verify all nodes, including the new node, have the correct n2n setting
        assert_n2n(self.cluster, enable)

    def n2n_off_addnode_test(self):
        self.n2n_test_base(self.cluster.add_node, False)

    def n2n_on_addnode_test(self):
        self.n2n_test_base(self.cluster.add_node, True)

    def n2n_off_joinself_test(self):
        self.n2n_test_base(self.cluster.do_join_cluster, False)

    def n2n_on_joinself_test(self):
        self.n2n_test_base(self.cluster.do_join_cluster, True)


class NodeAdditionWithCertsBase:
    def __init__(self, int_cert_in_chain):
        self.int_cert_in_chain = int_cert_in_chain

    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise",
                                    min_num_nodes=2, num_connected=1,
                                    encryption=False, afamily='ipv4',
                                    exact_services=[Service.KV])]

    def setup(self):
        self.cluster_ca = read_cert_file('test_CA.pem')
        self.new_node_root_ca = read_cert_file('test_CA2.pem')
        self.cluster_ca_key = read_cert_file('test_CA.pkey')
        self.new_node_ca_key = read_cert_file('test_CA2.pkey')
        self.new_node_int = read_cert_file('test_CA2_int.pem')
        self.new_node_int_key = read_cert_file('test_CA2_int.pkey')
        afamily = self.cluster_node().afamily()

        self.cluster_node_passphrase = testlib.random_str(8)
        self.cluster_node_cert, self.cluster_node_key = \
            generate_node_certs(self.cluster_node().addr(afamily=afamily),
                                self.cluster_ca, self.cluster_ca_key)
        self.cluster_node_key = to_pkcs8(self.cluster_node_key,
                                         self.cluster_node_passphrase)

        self.new_node_passphrase = testlib.random_str(8)
        self.new_node_cert_from_int, self.new_node_key_from_int = \
            generate_node_certs(self.new_node().addr(afamily=afamily),
                                self.new_node_int, self.new_node_int_key)

        if self.int_cert_in_chain:
            self.new_node_chain = (f"{self.new_node_cert_from_int}\n"
                                   f"{self.new_node_int}")
        else:
            self.new_node_chain = self.new_node_cert_from_int

        self.cluster_client_passphrase = testlib.random_str(8)
        self.cluster_client_cert, self.cluster_client_key = \
            generate_internal_client_cert(self.cluster_ca,
                                          self.cluster_ca_key,
                                          'test_client_name1')
        self.cluster_client_key = to_pkcs8(self.cluster_client_key,
                                           self.cluster_client_passphrase)

        self.new_node_client_passphrase = testlib.random_str(8)
        (self.new_node_client_cert_from_int,
         self.new_node_client_key_from_int) = \
            generate_internal_client_cert(self.new_node_int,
                                          self.new_node_int_key,
                                          'test_client_name3')

        if self.int_cert_in_chain:
            self.new_node_ca = self.new_node_root_ca
        else:
            self.new_node_ca = self.new_node_root_ca + self.new_node_int

    def teardown(self):
        pass

    def test_teardown(self):
        self.cluster.rebalance(self.cluster.connected_nodes[1:], wait=True,
                               verbose=True)
        assert_cluster_size(self.cluster, 1)
        self.cluster.wait_nodes_up()
        for n in self.cluster._nodes:
            testlib.post_succ(n, '/controller/regenerateCertificate',
                              params={'forceResetCACertificate': 'false',
                                      'dropUploadedCertificates': 'true'})
            CAs = testlib.get_succ(n, '/pools/default/trustedCAs').json()
            for ca in CAs:
                testlib.delete(n, f'/pools/default/trustedCAs/{ca["id"]}')

            testlib.toggle_client_cert_auth(n, enabled=False)

    # Node addition is initiated by the-cluster-node:

    # Both nodes use custom certs:

    def add_trusted_node_to_trusted_cluster_test(self):
        self.provision_cluster_node()
        self.provision_new_node()
        load_ca(self.cluster_node(), self.new_node_ca)
        load_ca(self.new_node(), self.cluster_ca)
        self.cluster.add_node(self.new_node()).json()

    def add_trusted_node_to_untrusted_cluster_test(self):
        self.provision_cluster_node()
        self.provision_new_node()
        load_ca(self.cluster_node(), self.new_node_ca)
        if encryption_enabled(self.cluster_node()):
            # Addition fails because the-new-node fails to verify otp
            # connectivity (which uses TLS in this case) to the-cluster-node
            r = self.cluster.add_node(self.new_node(),
                                      expected_code=400)
            self.assert_new_node_unknown_ca_error(r.json())
        else:
            self.cluster.add_node(self.new_node())

    def add_untrusted_node_to_trusted_cluster_test(self):
        self.provision_cluster_node()
        self.provision_new_node()
        load_ca(self.new_node(), self.cluster_ca)
        r = self.cluster.add_node(self.new_node(), expected_code=400).json()
        self.assert_cluster_unknown_ca_error(r)

    def add_untrusted_node_to_untrusted_cluster_test(self):
        self.provision_cluster_node()
        self.provision_new_node()
        r = self.cluster.add_node(self.new_node(), expected_code=400).json()
        self.assert_cluster_unknown_ca_error(r)

    # Cluster node uses ootb certs, new node uses custom certs:

    def add_trusted_node_to_trusted_ootb_cluster_test(self):
        load_ca(self.new_node(), self.cluster_ootb_ca())
        load_ca(self.cluster_node(), self.new_node_ca)
        self.provision_new_node()
        self.cluster.add_node(self.new_node())

    def add_trusted_node_to_untrusted_ootb_cluster_test(self):
        load_ca(self.cluster_node(), self.new_node_ca)
        self.provision_new_node()
        if encryption_enabled(self.cluster_node()):
            # Addition fails because the-new-node fails to verify otp
            # connectivity (which uses TLS in this case) to the-cluster-node
            r = self.cluster.add_node(self.new_node(),
                                      expected_code=400)
            self.assert_new_node_unknown_ca_error(r.json())
        else:
            self.cluster.add_node(self.new_node())

    def add_untrusted_node_to_trusted_ootb_cluster_test(self):
        load_ca(self.new_node(), self.cluster_ootb_ca())
        self.provision_new_node()
        # Despite the fact that the-cluster-node is ootb, the new-node will
        # validate its certificates when doing engageCluster
        r = self.cluster.add_node(self.new_node(), expected_code=400).json()
        self.assert_new_node_untrusted_error(r)

    def add_untrusted_node_to_untrusted_ootb_cluster_test(self):
        self.provision_new_node()
        r = self.cluster.add_node(self.new_node(), expected_code=400).json()
        if encryption_enabled(self.cluster_node()):
            # When encryption is on, the-new-node fails to verify otp
            # connectivity in engageCluster
            self.assert_new_node_unknown_ca_error(r)
        else:
            # Despite the fact that the-cluster-node is ootb, the new-node will
            # validate its certificates when doing engageCluster
            self.assert_new_node_untrusted_error(r)

    # The node addition will fail as the node being added has disabled
    # client cert authentication
    def add_trusted_node_to_trusted_cluster_client_cert_disabled_test_notyet(self):
        self.provision_cluster_node()
        self.provision_new_node()
        load_ca(self.cluster_node(), self.new_node_ca)
        load_ca(self.new_node(), self.cluster_ca)
        # Initialize the node being added to the cluster so that it has
        # a user/password and we can toggle client cert required. This
        # node/cluster gets over-written when it is added to the provisioned
        # cluster node.
        data_path = self.new_node().data_path()
        data = {"hostname": self.new_node().host,
                "services": "kv",
                "username": "Administrator",
                "password": "asdasd",
                "port": "SAME"}
        testlib.post_succ(self.new_node(), f"/clusterInit", data=data).json()

        testlib.toggle_client_cert_auth(self.new_node(),
                                        enabled=False, mandatory=False)
        r = self.cluster.add_node(self.new_node(), use_client_cert_auth=True,
                                  expected_code=400).json()
        self.assert_node_must_use_client_cert(r)

        # Run the same test except specify an invalid username/password
        r = self.cluster.add_node(self.new_node(), use_client_cert_auth=False,
                                  auth=("BadUser", "password"),
                                  expected_code=400).json()
        self.assert_cannot_use_invalid_username_password(r)

        # Change the new node so it uses mandatory client certs
        testlib.toggle_client_cert_auth(self.new_node(),
                                        enabled=True, mandatory=True)
        # Try using an invalid username/password
        r = self.cluster.add_node(self.new_node(), use_client_cert_auth=False,
                                  auth=("BadUser", "password"),
                                  expected_code=400).json()
        self.assert_cluster_requires_per_node_cert(r)

        # Need to clean up the node that was /clusterInit'd so subsequent
        # tests don't get residual errors.
        testlib.post_succ(self.new_node(), '/controller/hardResetNode')


    # Cluster node uses custom certs, new node uses ootb certs:

    def add_trusted_ootb_node_to_trusted_cluster_test(self):
        [ca_id] = load_ca(self.cluster_node(),
                          self.new_node_ootb_ca())
        load_ca(self.new_node(), self.cluster_ca)
        self.provision_cluster_node()
        self.cluster.add_node(self.new_node())
        # We can remove the ca that we added before, because the-new-node
        # should already use new certs now (new certs are regenerated during
        # node addition in this case)
        testlib.delete(self.cluster, f'/pools/default/trustedCAs/{ca_id}')

    def add_trusted_ootb_node_to_untrusted_cluster_test(self):
        [ca_id] = load_ca(self.cluster_node(),
                          self.new_node_ootb_ca())
        self.provision_cluster_node()
        if encryption_enabled(self.cluster_node()):
            # Addition fails because the-new-node fails to verify otp
            # connectivity (which uses TLS in this case) to the-cluster-node
            # Note that n2n encryption always verifies server's name even when
            # node uses ootb certificates
            r = self.cluster.add_node(self.new_node(),
                                      expected_code=400)
            self.assert_new_node_unknown_ca_error(r.json())
        else:
            self.cluster.add_node(self.new_node())
            # We can remove the ca that we added before, because the-new-node
            # should already use new certs now (new certs are regenerated
            # during node addition in this case)
            testlib.delete(self.cluster, f'/pools/default/trustedCAs/{ca_id}')

    def add_untrusted_ootb_node_to_trusted_cluster_test(self):
        load_ca(self.new_node(), self.cluster_ca)
        self.provision_cluster_node()
        r = self.cluster.add_node(self.new_node(), expected_code=400).json()
        self.assert_cluster_unknown_ca_error(r)

    def add_untrusted_ootb_node_to_untrusted_cluster_test(self):
        self.provision_cluster_node()
        r = self.cluster.add_node(self.new_node(), expected_code=400).json()
        self.assert_cluster_unknown_ca_error(r)

    # Node addition is iniated by the-new-node (node joins the cluster):

    # Both nodes use custom certs:

    def trusted_node_joins_trusted_cluster_test(self):
        load_ca(self.cluster_node(), self.new_node_ca)
        load_ca(self.new_node(), self.cluster_ca)
        self.provision_cluster_node()
        self.provision_new_node()
        self.cluster.do_join_cluster(self.new_node())

    def untrusted_node_joins_trusted_cluster_test(self):
        load_ca(self.new_node(), self.cluster_ca)
        self.provision_cluster_node()
        self.provision_new_node()
        r = self.cluster.do_join_cluster(self.new_node(),
                                         expected_code=400).json()
        self.assert_cluster_unknown_ca_error(r)

    def trusted_node_joins_untrusted_cluster_test(self):
        load_ca(self.cluster_node(), self.new_node_ca)
        self.provision_cluster_node()
        self.provision_new_node()
        r = self.cluster.do_join_cluster(self.new_node(),
                                         expected_code=400).json()
        self.assert_new_node_unknown_ca_error(r)

    def untrusted_node_joins_untrusted_cluster_test(self):
        self.provision_cluster_node()
        self.provision_new_node()
        r = self.cluster.do_join_cluster(self.new_node(),
                                         expected_code=400).json()
        self.assert_new_node_unknown_ca_error(r)

    # Cluster node uses custom certs, new node uses ootb certs:

    def trusted_ootb_node_joins_trusted_cluster_test(self):
        [ca_id] = load_ca(self.cluster_node(),
                          self.new_node_ootb_ca())
        load_ca(self.new_node(), self.cluster_ca)
        self.provision_cluster_node()
        self.cluster.do_join_cluster(self.new_node())
        testlib.delete(self.cluster, f'/pools/default/trustedCAs/{ca_id}')

    def untrusted_ootb_node_joins_trusted_cluster_test(self):
        load_ca(self.new_node(), self.cluster_ca)
        self.provision_cluster_node()
        r = self.cluster.do_join_cluster(self.new_node(),
                                         expected_code=400).json()
        self.assert_cluster_unknown_ca_error(r)

    def trusted_ootb_node_joins_untrusted_cluster_test(self):
        [ca_id] = load_ca(self.cluster_node(),
                          self.new_node_ootb_ca())
        self.provision_cluster_node()
        if encryption_enabled(self.cluster_node()):
            # Addition fails because the-new-node fails to verify otp
            # connectivity (which uses TLS in this case) to the-cluster-node
            # Note that n2n encryption always verifies server's name even when
            # node uses ootb certificates
            r = self.cluster.do_join_cluster(self.new_node(),
                                             expected_code=400)
            self.assert_new_node_unknown_ca_error(r.json())
        else:
            # Join works in this case because ootb node is not validating
            # the-cluster-node's name when sending the join request over https
            self.cluster.do_join_cluster(self.new_node())
            testlib.delete(self.cluster, f'/pools/default/trustedCAs/{ca_id}')

    def untrusted_ootb_node_joins_untrusted_cluster_test(self):
        self.provision_cluster_node()
        r = self.cluster.do_join_cluster(self.new_node(),
                                         expected_code=400).json()
        # The error happens on cluster side because the-new-node is ootb and
        # not validating cluster-node's certs
        self.assert_cluster_unknown_ca_error(r)

    # Cluster node uses ootb certs, new node uses custom certs:

    def trusted_node_joins_trusted_ootb_cluster_test(self):
        load_ca(self.new_node(), self.cluster_ootb_ca())
        load_ca(self.cluster_node(), self.new_node_ca)
        self.provision_new_node()
        self.cluster.do_join_cluster(self.new_node())

    def untrusted_node_joins_trusted_ootb_cluster_test(self):
        load_ca(self.new_node(), self.cluster_ootb_ca())
        self.provision_new_node()
        r = self.cluster.do_join_cluster(self.new_node(),
                                         expected_code=400).json()
        # Despite the fact that the-cluster-node is ootb, the new-node will
        # validate its certificates when doing engageCluster
        self.assert_new_node_untrusted_error(r)

    def trusted_node_joins_untrusted_ootb_cluster_test(self):
        load_ca(self.cluster_node(), self.new_node_ca)
        self.provision_new_node()
        r = self.cluster.do_join_cluster(self.new_node(),
                                         expected_code=400).json()
        self.assert_new_node_unknown_ca_error(r)

    def untrusted_node_joins_untrusted_ootb_cluster_test(self):
        self.provision_new_node()
        r = self.cluster.do_join_cluster(self.new_node(),
                                         expected_code=400).json()
        self.assert_new_node_unknown_ca_error(r)

    # Node addition when client certificate authentication is used:

    # New node and cluster both have client cert auth set to mandatory.
    # Also both use custom client certificates.
    # Note: using join for succ case also covers the add_node scenario
    def client_cert_auth_everywhere_test(self):
        self.provision_cluster_node(should_load_client_cert=True)
        self.provision_new_node(should_load_client_cert=True)
        load_ca(self.cluster_node(), self.new_node_ca)
        load_ca(self.new_node(), self.cluster_ca)
        testlib.toggle_client_cert_auth(self.cluster_node(),
                                        enabled=True, mandatory=True)
        testlib.toggle_client_cert_auth(self.new_node(),
                                        enabled=True, mandatory=True)
        self.cluster.do_join_cluster(self.new_node(),
                                     use_client_cert_auth=True)

    # Cluster has client cert auth set to mandatory. Cluster node has custom
    # client certs uploaded.
    # The-node-to-be-added doesn't allow client cert auth, but has custom
    # certs uploaded (so it can get authenticated at the cluster node).
    # Also both use custom client certificates.
    # Note: using join for succ case also covers the add_node scenario
    def client_cert_auth_on_cluster_only_test(self):
        self.provision_cluster_node(should_load_client_cert=True)
        self.provision_new_node(should_load_client_cert=True)
        load_ca(self.cluster_node(), self.new_node_ca)
        load_ca(self.new_node(), self.cluster_ca)
        testlib.toggle_client_cert_auth(self.cluster_node(),
                                        enabled=True, mandatory=True)
        self.cluster.do_join_cluster(self.new_node(),
                                     use_client_cert_auth=True)

    # Cluster has client cert auth set to mandatory, but doesn't have client
    # cert uploaded (uses ootb client certs).
    # The-node-to-be-added doesn't allow client cert auth, and doesn't have
    # client certs uploaded (uses ootb client certs).
    def client_cert_auth_ootb_client_certs_on_cluster_via_add_test(self):
        self.provision_cluster_node(should_load_client_cert=False)
        self.provision_new_node(should_load_client_cert=False)
        load_ca(self.cluster_node(), self.new_node_ca)
        load_ca(self.new_node(), self.cluster_ca)
        testlib.toggle_client_cert_auth(self.cluster_node(),
                                        enabled=True, mandatory=True)
        self.cluster.add_node(self.new_node())

    # Cluster has client cert auth set to mandatory, but doesn't have client
    # cert uploaded (uses ootb client certs).
    # The-node-to-be-added doesn't allow client cert auth, and doesn't have
    # client certs uploaded (uses ootb client certs).
    # Since this node uses "join", the cluster node has to trust new-node's
    # ootb CA. Otherwise new-node won't be able to authenticat at
    # the cluster node
    def client_cert_auth_ootb_client_certs_on_cluster_via_join_test(self):
        [ca_id] = load_ca(self.cluster_node(),
                          self.new_node_ootb_ca())
        self.provision_cluster_node(should_load_client_cert=False)
        self.provision_new_node(should_load_client_cert=False)
        load_ca(self.cluster_node(), self.new_node_ca)
        load_ca(self.new_node(), self.cluster_ca)

        testlib.toggle_client_cert_auth(self.cluster_node(),
                                        enabled=True, mandatory=True)
        self.cluster.do_join_cluster(self.new_node(),
                                     use_client_cert_auth=True)
        testlib.delete(self.cluster, f'/pools/default/trustedCAs/{ca_id}')

    # Cluster has client cert auth set to disabled. The node-to-be-added
    # specifies clientCertAuth=true when joining the cluster. Ensure the
    # appropriate error is returned. Also ensure an appropriate error if
    # clientCertAuth=false but an invalid username/password is specified.
    def client_cert_auth_ootb_client_certs_disabled_on_cluster_join_test(self):
        self.provision_cluster_node(should_load_client_cert=False)
        self.provision_new_node(should_load_client_cert=False)
        testlib.toggle_client_cert_auth(self.cluster_node(),
                                        enabled=False, mandatory=False)
        load_ca(self.new_node(), self.cluster_ca)

        r = self.cluster.do_join_cluster(self.new_node(),
                                         use_client_cert_auth=True,
                                         expected_code=400).json()
        self.assert_cannot_use_per_node_client_cert(r)

        # Now attempt using a bad username/password
        r = self.cluster.do_join_cluster(self.new_node(),
                                         use_client_cert_auth=False,
                                         auth=("baduser", "password"),
                                         expected_code=400).json()
        self.assert_cannot_use_invalid_username_password(r)

        # XXX: disabled until it can be triaged
        # Change the cluster node so it uses mandatory client certs
        #testlib.toggle_client_cert_auth(self.cluster_node(),
        #                                enabled=True, mandatory=True)
        # Try using a bad username/password
        #r = self.cluster.do_join_cluster(self.new_node(),
        #                                 use_client_cert_auth=False,
        #                                 auth=("baduser", "password"),
        #                                 expected_code=400).json()
        #self.assert_added_node_must_use_client_cert(r)

    def provision_cluster_node(self, should_load_client_cert=False):
        cluster_node = self.cluster_node()
        load_ca(cluster_node, self.cluster_ca)
        load_node_cert(cluster_node, self.cluster_node_cert,
                       self.cluster_node_key,
                       passphrase=self.cluster_node_passphrase)
        if should_load_client_cert:
            load_client_cert(cluster_node, self.cluster_client_cert,
                             self.cluster_client_key,
                             passphrase=self.cluster_client_passphrase)

    def provision_new_node(self, should_load_client_cert=False):
        new_node = self.new_node()
        load_ca(new_node, self.new_node_ca)
        load_node_cert(new_node, self.new_node_chain,
                       self.new_node_key_from_int,
                       passphrase=self.new_node_passphrase)
        if should_load_client_cert:

            if self.int_cert_in_chain:
                new_node_client_cert = (f"{self.new_node_client_cert_from_int}"
                                        f"\n{self.new_node_int}")
            else:
                new_node_client_cert = self.new_node_client_cert_from_int
            new_node_client_key = self.new_node_client_key_from_int

            load_client_cert(new_node, new_node_client_cert,
                             new_node_client_key,
                             passphrase=self.new_node_client_passphrase)

    def cluster_node(self):
        return self.cluster.connected_nodes[0]

    def new_node(self):
        return self.cluster.spare_node()

    def cluster_ootb_ca(self):
        return self.get_generated_CA(self.cluster_node())

    def new_node_ootb_ca(self):
        return self.get_generated_CA(self.new_node())

    def get_generated_CA(self, node):
        CAs = testlib.get_succ(node, '/pools/default/trustedCAs').json()
        # This function assumes that there are no CA's uploaded or generated
        # because that's the only case when we need it for these ^^ tests
        assert len(CAs) == 1, 'unexpected number of CA certificates'
        assert CAs[0]['type'] == 'generated', \
               'unexpected type of CA certificate'
        return CAs[0]['pem']


    def assert_cluster_unknown_ca_error(self, response):
        assert_msg_in_error('Unknown CA', response[0])
        assert_msg_in_error('Please review the CAs available on the cluster',
                            response[0])
        assert_failed_to_connect_error(self.new_node(), response[0])


    def assert_new_node_unknown_ca_error(self, response):
        assert_msg_in_error('Unknown CA', response[0])
        assert_msg_in_error('Please review the CAs available on the new node',
                            response[0])
        assert_failed_to_connect_error(self.cluster_node(), response[0])

    def assert_new_node_untrusted_error(self, response):
        assert_msg_in_error('Error validating new node\'s node certificate '
                            'chain. CA certificate for this chain is not found '
                            'in the list of trusted CA\'s', response[0])

    def assert_cannot_use_per_node_client_cert(self, response):
        assert_msg_in_error('Invalid credentials. Ensure client certificate '
                            'authentication is enabled for the cluster.',
                            response[0])


    def assert_cannot_use_invalid_username_password(self, response):
        assert_msg_in_error('Invalid credentials. Verify username and '
                            'password.', response[0])


    def assert_node_must_use_client_cert(self, response):
        assert_msg_in_error('Invalid credentials. Ensure client certificate '
                            'authentication is enabled on the node being '
                            'added.', response[0])


    def assert_cluster_requires_per_node_cert(self, response):
        assert_msg_in_error('Cluster node requires per-node client '
                            'certificate when client certificate '
                            'authentication is set to mandatory.',
                            response[0])

    def assert_added_node_must_use_client_cert(self, response):
        assert_msg_in_error('Node being added requires per-node client '
                            'certificate when client certificate '
                            'authentication is set to mandatory.',
                            response[0])


class NodeAdditionWithCertsTests(NodeAdditionWithCertsBase,
                                 testlib.BaseTestSet):

    def __init__(self, cluster):
        NodeAdditionWithCertsBase.__init__(self, True)
        testlib.BaseTestSet.__init__(self, cluster)


class NodeAdditionWithIntCertsNotInChainTests(NodeAdditionWithCertsBase,
                                              testlib.BaseTestSet):
    def __init__(self, cluster):
        NodeAdditionWithCertsBase.__init__(self, False)
        testlib.BaseTestSet.__init__(self, cluster)

    def add_int_untrusted_node_to_trusted_cluster_test(self):
        self.provision_cluster_node()
        self.provision_new_node()
        load_ca(self.cluster_node(), self.new_node_root_ca)
        load_ca(self.new_node(), self.cluster_ca)

        r = self.cluster.add_node(self.new_node(),
                                  expected_code=400).json()
        self.assert_new_node_untrusted_error(r)


class NodeAdditionWithCertsN2NIPv6Tests(testlib.BaseTestSet):
    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise",
                                    min_num_nodes=2, num_connected=1,
                                    encryption=True, afamily='ipv6',
                                    exact_services=[Service.KV])]

    def __init__(self, cluster):
        super().__init__(cluster)
        self.wrapped = NodeAdditionWithIntCertsNotInChainTests(cluster)

    def setup(self):
        self.wrapped.setup()

    def teardown(self):
        self.wrapped.teardown()

    def test_teardown(self):
        self.wrapped.test_teardown()

    def add_trusted_node_to_trusted_cluster_test(self):
        self.wrapped.add_trusted_node_to_trusted_cluster_test()

    def add_trusted_node_to_untrusted_cluster_test(self):
        self.wrapped.add_trusted_node_to_untrusted_cluster_test()

    def add_trusted_node_to_untrusted_ootb_cluster_test(self):
        self.wrapped.add_trusted_node_to_untrusted_ootb_cluster_test()

    def add_untrusted_node_to_untrusted_ootb_cluster_test(self):
        self.wrapped.add_untrusted_node_to_untrusted_ootb_cluster_test()

    def add_trusted_ootb_node_to_untrusted_cluster_test(self):
        self.wrapped.add_trusted_ootb_node_to_untrusted_cluster_test()

    def trusted_ootb_node_joins_untrusted_cluster_test(self):
        self.wrapped.trusted_ootb_node_joins_untrusted_cluster_test()

    def add_int_untrusted_node_to_trusted_cluster_test(self):
        self.wrapped.add_int_untrusted_node_to_trusted_cluster_test()


def assert_msg_in_error(msg, error):
    assert msg in error, f'missing "{msg}" in error: "{error}"'


def assert_failed_to_connect_error(remote_node, error):
    host = testlib.maybe_add_brackets(remote_node.host)
    msg1 = 'Failed to establish TLS connection to ' \
           f'{host}:{remote_node.tls_service_port()}'
    msg2 = 'Failed to establish TLS connection to ' \
           f'{host}:{remote_node.otp_port(encryption=True)}'
    assert (msg1 in error) or (msg2 in error), \
           f'missing "{msg1}" or "{msg2}" in "{error}"'

# Assert that all nodes have node-to-node encryption enabled/disabled
def assert_n2n(cluster, expected_value):
    r = testlib.get_succ(cluster, "/pools/default")
    bad_nodes = [node['hostname'] for node in r.json()['nodes']
                 if node['nodeEncryption'] != expected_value]
    assert len(bad_nodes) == 0, \
        f"Expected nodeEncryption to be {expected_value} for all nodes, " \
        f"but got {not expected_value} for the following nodes: {bad_nodes}"


# Assert that the cluster has the expected size
def assert_cluster_size(cluster, expected_size):
    resp = testlib.get_succ(cluster, "/pools/default")
    nodes = [node["hostname"] for node in resp.json()["nodes"]]
    assert len(nodes) == expected_size, \
        f"Wrong number of nodes in cluster. Expected {expected_size} " \
        f"nodes, found the following set of nodes: {nodes}"


def encryption_enabled(node):
    return testlib.get_succ(node, '/nodes/self').json()['nodeEncryption']


# Note that this function changes n2n encryption for one node only
# If that node is part of a cluster it might break node connectivity inside
# of that cluster. Use it for standalone nodes only. For clusters use
# cluster.toggle_n2n_encryption()
def toggle_node_n2n(node, enable=True):
    # Create an external listener
    testlib.post_succ(node, "/node/controller/enableExternalListener",
                      data={"nodeEncryption": "on"
                            if enable else "off"})

    # Change the node-to-node encryption settings
    testlib.post_succ(node, "/node/controller/setupNetConfig",
                      data={"nodeEncryption": "on"
                            if enable else "off"})
    # Disable any unused listeners
    testlib.post_succ(node,
                      "/node/controller/disableUnusedExternalListeners")


BUCKET_NAME = "dummy-bucket"

class NodeRemovalStreamingChunked(testlib.BaseTestSet):

    def setup(self):
        pass

    def teardown(self):
        self.cluster.add_node(self.cluster.disconnected_nodes()[0],
                              do_rebalance=True)

    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise",
                                    min_num_nodes=2, num_connected=2,
                                    include_services=[Service.KV, Service.CBAS],
                                    balanced=True,
                                    num_vbuckets=16,
                                    buckets=[{"name": BUCKET_NAME,
                                              "storageBackend": "couchstore",
                                              "ramQuota": 256}])]

    def basic_node_removal_test(self):
        # listen to the poolStreaming endpoint node A
        port = self.cluster.connected_nodes[0].port
        _, password = self.cluster.connected_nodes[0].auth
        cwd = os.getcwd()

        # Use a bunch of different relative paths to make it easier to run
        # cluster_tests without forcing you to launch it from the same
        # directory as they do in CI.
        path = {"PATH": f"{cwd}/../../../install/bin/priv:" \
                f"{cwd}/../../install/bin/priv:" \
                f"{cwd}/../install/bin/priv:{cwd}/install/bin/priv/"}
        proc = Popen(["chunked_reader", f"{port}", password], env=path)

        # remove node A from the cluster
        self.cluster.failover_node(self.cluster.connected_nodes[0],
                                   graceful=True)
        self.cluster.eject_node(self.cluster.connected_nodes[0],
                                self.cluster.connected_nodes[1])

        # Verify that chunked stream is closed correctly using
        # chunked_reader.go binary.
        return_code = proc.wait()
        assert return_code == 0
