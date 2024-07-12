# @author Couchbase <info@couchbase.com>
# @copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import os
import shutil
import zipfile
import re
from pathlib import Path
import testlib
from testlib.util import Service
from testlib import ClusterRequirements

from subprocess import Popen

MAX_TIMEOUT = 60 * 5 # 5 minute timeout
ANALYTICS_DEBUG = "analytics_debug.log"

class CbcollectTest(testlib.BaseTestSet):
    def setup(self):
        os.mkdir("temp-zip", mode=0o777)

    def teardown(self):
        shutil.rmtree("./temp-zip", ignore_errors=True)

    def test_teardown(self):
        pass

    @staticmethod
    def requirements():
        return [ClusterRequirements(edition="Enterprise",
                                    num_nodes=3, num_connected=3,
                                    services=[Service.KV])]

    def get_path(self, cwd):
        return f"{cwd}/../../install/lib/python/interp/bin"

    def get_pythonpath(self, cwd):
        return f"{self.get_code_dir(cwd)}:{cwd}/../pylib"

    def get_code_dir(self, cwd):
        return f"{cwd}/../../install/lib/python"

    def get_cbcollect_env(self, cwd):
        return {"PATH": self.get_path(cwd),
                "PYTHONPATH": self.get_pythonpath(cwd)}

    def start_cbcollect(self, cwd, init, base):
        return Popen(["python3", "-s",
                      f"{self.get_code_dir(cwd)}/cbcollect_info",
                      "--initargs", init,
                      f"temp-zip/test-cbcollect-{base}.zip",
                      "--log-redaction-level", "partial"],
                     env=self.get_cbcollect_env(cwd))

    def redacted_analytics_test(self):
        procs = []
        redacted_zips = []
        cwd = os.getcwd()
        regex = re.compile("<ud>(?![a-f0-9]{40})</ud>")

        # copy over the analytics_debug.log
        for logs_dir in \
                Path(f'test_cluster_data-{self.cluster.index}/logs/').glob("n*"):
            base = os.path.basename(logs_dir)
            shutil.copyfile(f"./resources/fixtures/{ANALYTICS_DEBUG}",
                            Path(f"{logs_dir}/{ANALYTICS_DEBUG}"))
            redacted_zips.append(f"temp-zip/test-cbcollect-{base}-redacted.zip")

        # start cbcollect processes
        for zips_dir in \
                Path(f'test_cluster_data-{self.cluster.index}/data/').glob("n*"):
            procs.append(
                self.start_cbcollect(cwd, Path(f"{cwd}/{zips_dir}/initargs"),
                                     os.path.basename(zips_dir)))

        # wait for collections to end + get result
        for proc in procs:
            assert proc.wait(MAX_TIMEOUT) == 0

        # unzip the zip files and check that there are no <ud>...</ud> that
        # aren't correctly filled..
        unzipped_dirs = {}
        for zipp in redacted_zips:
            with zipfile.ZipFile(zipp, mode="r") as zObj:
                zObj.extractall(path="temp-zip/.")

            for g in Path('.').glob("temp-zip/*/"):
                unzipped_dirs[g] = True

        for zip_dir in unzipped_dirs.keys():
            os.chdir(f"{zip_dir}")
            with open(f"ns_server.{ANALYTICS_DEBUG}") as analytics_logfile:
                for line in analytics_logfile.readlines():
                    assert regex.search(line) is None
            os.chdir("../..")
