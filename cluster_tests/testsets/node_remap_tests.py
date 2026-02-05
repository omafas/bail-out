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
from testlib.test_tag_decorator import tag, Tag
from testsets.sample_buckets import SampleBucketTasksBase

import run
import shutil
import subprocess
import time
import sys
from pathlib import Path
from copy import deepcopy

sys.path.append(testlib.get_scripts_dir())

import node_remap

REMAP_OFFSET = 10
CLUSTER_INDEX_OFFSET = 10000

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
                # our tests. We make assumptions about paths, we cannot use a
                # provided cluster.
                testlib.ClusterRequirements(edition="Enterprise",
                                            num_nodes=2,
                                            include_services=
                                            list(testlib.Service),
                                            min_memsize=1024,
                                            balanced=True,
                                            buckets=[],
                                            test_generated_cluster=True,
                                            num_vbuckets=16),
                # Test with a single node. The hostname mapping is slightly
                # different. We make assumptions about paths, we cannot use a
                # provided cluster.
                testlib.ClusterRequirements(edition="Enterprise",
                                            num_nodes=1,
                                            include_services=
                                            [testlib.Service.KV],
                                            min_memsize=1024,
                                            buckets=[],
                                            test_generated_cluster=True,
                                            num_vbuckets=16)]

    def setup(self):
        self.load_and_assert_sample_bucket(self.cluster, "travel-sample")
        # The script remaps chronicle snapshots and chronicle logs. Make sure
        # that we have a snapshot containing the travel-sample info, we will
        # later assert that it comes up, to test that the snapshot remapping
        # works.
        testlib.post_succ(self.cluster, "/diag/eval",
                          data="{ok, _} = chronicle:force_snapshot()")

        # Note that this bucket likely won't exist in a chronicle snapshot. We
        # want to be able to recover from chronicle logs alone, so we'll try to
        # test that here by not forcing a snapshot
        self.cluster.create_bucket({'name': 'default',
                                    'storageBackend': 'couchstore',
                                    'ramQuotaMB': '100'}, sync=True)

        for node in self.cluster._nodes:
            node.set_alternate_address(node.host)

        testlib.post_succ(self.cluster, '/settings/autoFailover', data={
            "enabled": "true",
            "timeout": 120})


    def test_teardown(self):
        pass

    def teardown(self):
        pass

        # The remap script should turn off AFO, turn it back on
        testlib.post_succ(self.cluster, '/settings/autoFailover', data={
            "enabled": "true",
            "timeout": 120})

    def remap_cluster(self, old_cluster):
        old_start_index = old_cluster.first_node_index
        new_start_index = old_start_index + REMAP_OFFSET

        old_cluster_path = Path(old_cluster.get_cluster_path())
        new_cluster_path = Path(
            old_cluster.get_cluster_path(index_offset=CLUSTER_INDEX_OFFSET))

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

        # Remove directory if it exists, so we are not affected by any
        # left-over files from previous runs
        shutil.rmtree(new_cluster_path, ignore_errors=True)
        os.makedirs(new_cluster_path/'couch')
        os.makedirs(new_cluster_path/'data')
        os.makedirs(new_cluster_path/'logs')

        for i in range(len(old_cluster._nodes)):
            old_node_index = old_start_index + i
            new_node_index = new_start_index + i

            # Copy files, cluster_run always passes node names when starting up
            # the cluster based on the start index. As such, we make things
            # easier for ourselves if we run the remapped cluster from the
            # remapped index. To accomplish this we copy all files there and
            # let the remap script output to that directory

            shutil.copyfile(
                old_cluster_path/'couch'/f'n_{old_node_index}_conf.ini',
                new_cluster_path/'couch'/f'n_{new_node_index}_conf.ini'
            )

            shutil.copytree(old_cluster_path/'data'/f'n_{old_node_index}',
                            new_cluster_path/'data'/f'n_{new_node_index}')

            shutil.copytree(old_cluster_path/'logs'/f'n_{old_node_index}',
                            new_cluster_path/'logs'/f'n_{new_node_index}')

            initargs_path = (
                new_cluster_path/'data'/f'n_{new_node_index}'/'initargs'
            )

            assert os.path.isfile(initargs_path)

            # And now we remap the config in the new (remapped) node directory
            node_remap.run_config_remap_via_escript_wrapper(
                root_dir=testlib.get_install_dir(),
                initargs=[initargs_path],
                output_path=new_cluster_path/'data'/f'n_{new_node_index}',
                remap=remap_args,
                capture_output=testlib.config['intercept_output']
            )

    def check_nodefile(self, old_cluster):
        # nodefile is a cluster_run only file containing the node name. Check
        # that it was remapped properly. We can't check the ip files because
        # they will all list localhost/cb.local.
        old_start_index = old_cluster.first_node_index
        new_start_index = old_start_index + REMAP_OFFSET

        new_cluster_path = Path(
            old_cluster.get_cluster_path(CLUSTER_INDEX_OFFSET))

        hostname = '127.0.0.1'
        if len(old_cluster._nodes) == 1:
            hostname = 'cb.local'

        for i in range(len(old_cluster._nodes)):
            old_node_index = old_start_index + i
            new_node_index = new_start_index + i
            output_path=f'{new_cluster_path}/data/n_{new_node_index}'

            nodefile_path = f'{output_path}/nodefile'
            nodefile = open(nodefile_path, 'r')
            contents = nodefile.read().splitlines()[0]
            assert contents == f'n_{new_node_index}@{hostname}'

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
        cluster_run_lib.base_api_port -= REMAP_OFFSET

        new_first_node_index = old_cluster.first_node_index + REMAP_OFFSET
        c = None
        try:
            # Turn on new rempped cluster
            print(f"Starting remapped cluster at node index "
                  f"{new_first_node_index}")

            # Remove the buckets requirement, so that the bucket isn't deleted
            new_requirements = deepcopy(old_cluster.requirements)
            new_requirements.requirements["buckets"] = None
            c = new_requirements.create_cluster(
                    old_cluster.auth,
                    # Don't clash with self.cluster
                    old_cluster.index + CLUSTER_INDEX_OFFSET,
                    run.tmp_cluster_dir,
                    new_first_node_index,
                    connect=False)

            # Bucket should come back
            c.wait_for_bucket("travel-sample")

            # And all nodes should be healthy too
            c.wait_for_nodes_to_be_healthy()

            new_uuid = c.get_cluster_uuid()
            assert old_uuid != new_uuid

            new_cookie = c.get_cookie()
            assert old_cookie != new_cookie

            # MB-62201: alternate addresses should be removed
            for node in c._nodes:
                current_alt_address = node.get_alternate_addresses()
                assert None == current_alt_address

            for node in c._nodes:
                afo_settings = testlib.get_succ(node,
                                                '/settings/autoFailover').json()
                assert not afo_settings["enabled"]

            # Sanity check and shut down the remapped cluster
            c.smog_check()
        finally:
            if c is not None:
                print(f"Shutting down remapped cluster at node index "
                      f"{new_first_node_index}")
                c.destroy()
                print(f"Shut down remapped cluster at node index "
                      f"{new_first_node_index}")

            cluster_run_lib.base_api_port += REMAP_OFFSET

    @tag(Tag.LowUrgency)
    def basic_remap_test(self):
        # Grab some old values from the cluster to compare before against the
        # remapped cluster
        old_uuid = self.cluster.get_cluster_uuid()
        old_cookie = self.cluster.get_cookie()

        # We can't tear down the older cluster in the setup because it wants to
        # log via diag/eval to the cluster...
        print(f"Shutting down original cluster at node index "
              f"{self.cluster.first_node_index}")

        self.cluster.stop_all_nodes()

        print(f"Shut down original cluster at node index "
              f"{self.cluster.first_node_index}")
        try:
            self.remap_cluster(self.cluster)

            self.check_nodefile(self.cluster)
            self.start_and_test_remapped_cluster(self.cluster, old_uuid,
                                                 old_cookie)
        finally:
            print(f"Starting original cluster at node index "
                  f"{self.cluster.first_node_index}")
            self.cluster.restart_all_nodes()

