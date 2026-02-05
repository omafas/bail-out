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

import testlib

import run
import shutil
import sys

from pathlib import Path

sys.path.append(testlib.get_scripts_dir())

from testlib.test_tag_decorator import tag, Tag

import node_remap

class ConfigRemapTest(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return [
            # We make assumptions about paths, we cannot use a provided cluster.
            testlib.ClusterRequirements(edition="Enterprise",
                                        num_nodes=1,
                                        test_generated_cluster=True)]

    def setup(self):
        testlib.post_succ(self.cluster, '/settings/autoFailover',
                          data={"enabled": "true", "timeout": 120})

        # Fill up a few chronicle logs to ensure that we are rewriting
        # correctly. There exists an issue where, if we attempt to overwrite the
        # file that we are reading, we will corrupt the file. We aren't doing
        # that here because it doesn't work, but this will make this test fail
        # in a standalone way if that changes (rather than relying on previous
        # test having run on this cluster).
        testlib.post_succ(self.cluster, '/diag/eval',
            data='F = fun(X) ->'
                              'chronicle_kv:set(kv, foo, X)'
                     'end,'
                 'lists:foreach(F, lists:seq(1,300))')

    def teardown(self):
        # The remap script should turn off AFO, turn it back on
        testlib.post_succ(self.cluster, '/settings/autoFailover',
                          data={"enabled": "true", "timeout": 120})

    def run_config_remap(self, old_cluster, extra_args):
        old_start_index = old_cluster.first_node_index

        cluster_path = Path(old_cluster.get_cluster_path())

        for i in range(len(old_cluster._nodes)):
            old_node_index = old_start_index + i


            node_remap.run_config_remap(
                root_dir=testlib.get_install_dir(),
                initargs=[f'{cluster_path}/data/n_{old_node_index}/initargs'],
                output_path=f'{cluster_path}/data/tmp',
                capture_output=testlib.config['intercept_output'],
                extra_args=extra_args
            )

            shutil.copytree(cluster_path/'data'/f'tmp',
                            cluster_path/'data'/f'n_{old_node_index}',
                            dirs_exist_ok=True)

    @tag(Tag.LowUrgency)
    def without_remap_test(self):
        testlib.post_succ(
            self.cluster, '/settings/fusion',
            json={'logStoreURI': 'local://001', 'enableSyncThresholdMB': 1024})

        otp_nodes = str(list(testlib.get_otp_nodes(self.cluster).values()))
        testlib.diag_eval(self.cluster, f"chronicle_compat:push({otp_nodes}).")

        # We can't tear down the older cluster in the setup because it wants to
        # log via diag/eval to the cluster...
        print(f"Shutting down original cluster at node index "
              f"{self.cluster.first_node_index}")

        self.cluster.stop_all_nodes()

        print(f"Shut down original cluster at node index "
              f"{self.cluster.first_node_index}")

        extra_args = ['--disable-auto-failover',
                      '--rewrite-key-value', 'log_store_uri', '"local://002"',
                      '--rewrite-key-value', 'enable_sync_threshold_mb', '1000']

        self.run_config_remap(self.cluster, extra_args)

        print(f"Starting original cluster at node index "
              f"{self.cluster.first_node_index}")

        self.cluster.restart_all_nodes()

        for node in self.cluster._nodes:
            afo_settings = testlib.get_succ(node,
                                            '/settings/autoFailover').json()
            assert not afo_settings["enabled"]
            fusion_settings = testlib.get_succ(node, '/settings/fusion').json()
            assert fusion_settings['logStoreURI'] == 'local://002'
            assert fusion_settings['enableSyncThresholdMB'] == 1000
