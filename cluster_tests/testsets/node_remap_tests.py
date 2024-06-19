# @author Couchbase <info@couchbase.com>
# @copyright 2024 Couchbase, Inc.
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
import os.path

import cluster_run_lib

import atexit
import testlib
import testlib.requirements
from testsets.sample_buckets import SampleBucketTasksBase

import run
import shutil
import subprocess
import time
import sys
from pathlib import Path

scripts_dir = os.path.join(run.scriptdir, "..", "scripts/")
print(scripts_dir)
sys.path.append(scripts_dir)

import node_remap

REMAP_MULTIPLE = 2

class NodeRemapTest(testlib.BaseTestSet, SampleBucketTasksBase):

    def __init__(self, cluster):
        super().__init__(cluster)
        SampleBucketTasksBase.__init__(self)
        self.req_num_nodes = None

    @staticmethod
    def requirements():
        return [
                # Test with multiple nodes, we should be able to establish
                # connectivity. Currently testing with all services. We could
                # drop that to just KV if we see other services cause issues in
                # our tests.
                testlib.ClusterRequirements(edition="Enterprise",
                                            num_nodes=2,
                                            services=list(testlib.Service),
                                            memsize=1024),
                # Test with a single node. The hostname mapping is slightly
                # different.
                testlib.ClusterRequirements(edition="Enterprise",
                                            num_nodes=1,
                                            services=[testlib.Service.KV],
                                            memsize=1024)]

    def setup(self):
        self.load_and_assert_sample_bucket(self.cluster, "travel-sample")
        # The test remaps snapshots, make sure we have one
        testlib.post_succ(self.cluster, "/diag/eval",
                          data="{ok, _} = chronicle:force_snapshot()")

        # Note that this bucket likely won't have a snapshot. We want to be able
        # to recover from chronicle logs alone, so we'll try to test that here
        # by not forcing a snapshot
        self.cluster.create_bucket({'name': 'default',
                                    'storageBackend': 'couchstore',
                                    'ramQuotaMB': '100'}, sync=True)

        for node in self.cluster._nodes:
            node.set_alternate_address(node.host)


    def test_teardown(self):
        pass

    def teardown(self):
        # Clean up the cloned stuff
        self.cluster.delete_bucket("travel-sample")
        self.cluster.delete_bucket("default")

    def remap_cluster(self, old_cluster):
        old_start_index = old_cluster.first_node_index
        new_start_index = old_start_index * REMAP_MULTIPLE

        cluster_path = (run.scriptdir /
                        Path(f'test_cluster_data-{old_cluster.index}'))

        # We must tell all nodes about all nodes being remapped, because all
        # nodes eventually contain all config. Build up that map (or set of
        # args) before we run the script.
        remap_args = []
        hostname = '127.0.0.1'

        if len(old_cluster._nodes) == 1:
            hostname = 'cb.local'

        for i in range(len(old_cluster._nodes)):
            old_node_index = old_start_index + i
            new_node_index = new_start_index + i
            remap_args += [[f'n_{old_node_index}@{hostname}',
                            f'n_{new_node_index}@{hostname}']]

        for i in range(len(old_cluster._nodes)):
            old_node_index = old_start_index + i
            new_node_index = new_start_index + i

            # Copy files, cluster_run always passes node names when starting up
            # the cluster based on the start index. As such, we make things
            # easier for ourselves if we run the remapped cluster from the
            # remapped index. To accomplish this we copy all files there and
            # let the remap script output to that directory
            shutil.copyfile(cluster_path/'couch'/f'n_{old_node_index}_conf.ini',
                            cluster_path/'couch'/f'n_{new_node_index}_conf.ini')

            shutil.copytree(cluster_path/'data'/f'n_{old_node_index}',
                            cluster_path/'data'/f'n_{new_node_index}')

            shutil.copytree(cluster_path/'logs'/f'n_{old_node_index}',
                            cluster_path/'logs'/f'n_{new_node_index}')

            # And now we remap the config in the new (remapped) node directory
            node_remap.run_config_remap_via_escript_wrapper(
                root_dir=run.rootdir,
                initargs=[f'{cluster_path}/data/n_{new_node_index}/initargs'],
                output_path=f'{cluster_path}/data/n_{new_node_index}',
                remap=remap_args,
                capture_output=testlib.config['intercept_output']
            )

    def start_and_test_remapped_cluster(self, old_cluster, old_uuid,
                                        old_cookie):
        # When we remap the cluster we do not remap the ports on which each node
        # runs. For on-prem deployments this is fine, but it makes it trickier
        # to work with a cluster_run. Rather than attempt to remap ports, we are
        # going to tell cluster_run to offset the ports to the original ones.
        # This will allow cluster_run and the rest of cluster_tests to connect
        # to the remapped nodes without issue. We can do this by setting
        # cluster_run_lib.base_api_port. It's a little hacky perhaps, but it
        # saves us from passing around an option everywhere that would only be
        # used by the script remapping. We'll reset this later in a finally
        # statement to make sure that the next cluster connects to the correct
        # ports.
        cluster_run_lib.base_api_port -= old_cluster.first_node_index

        new_first_node_index = old_cluster.first_node_index * REMAP_MULTIPLE
        try:
            # Turn on new rempped cluster
            print(f"Starting remapped cluster at node index "
                  f"{new_first_node_index}")
            c = old_cluster.requirements.create_cluster(
                    old_cluster.auth,
                    old_cluster.index,
                    run.tmp_cluster_dir,
                    old_cluster.first_node_index * REMAP_MULTIPLE,
                    connect=False)

            # Bucket should come back
            c.wait_for_bucket("travel-sample")

            # And all nodes should be healthy too
            c.wait_for_nodes_to_be_healthy()

            new_uuid = c.get_cluster_uuid()
            assert old_uuid != new_uuid

            new_cookie = c.get_cluster_uuid
            assert old_cookie != new_cookie

            # MB-62201: alternate addresses should be removed
            for node in c._nodes:
                current_alt_address = node.get_alternate_addresses()
                assert None == current_alt_address

            # Sanity check and shut down the remapped cluster
            c.smog_check()
        finally:
            print(f"Shutting down remapped cluster at node index "
                  f"{new_first_node_index}")
            c.teardown()
            print(f"Shut down remapped cluster at node index "
                  f"{new_first_node_index}")

            cluster_run_lib.base_api_port += old_cluster.first_node_index

    def basic_remap_test(self):
        # Grab some old values from the cluster to compare before against the
        # remapped cluster
        old_uuid = self.cluster.get_cluster_uuid()
        old_cookie = self.cluster.get_cookie()

        # We can't tear down the older cluster in the setup because it wants to
        # log via diag/eval to the cluster...
        print(f"Shutting down original cluster at node index "
              f"{self.cluster.first_node_index}")

        self.cluster.teardown()

        print(f"Shut down original cluster at node index "
              f"{self.cluster.first_node_index}")
        try:
            self.remap_cluster(self.cluster)

            self.start_and_test_remapped_cluster(self.cluster, old_uuid,
                                                 old_cookie)
        finally:
            print(f"Starting original cluster at node index "
                  f"{self.cluster.first_node_index}")
            self.cluster = self.cluster.requirements.create_cluster(
                self.cluster.auth, self.cluster.index,
                run.tmp_cluster_dir, self.cluster.first_node_index, False)
