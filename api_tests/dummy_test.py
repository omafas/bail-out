# @author Couchbase <info@couchbase.com>
# @copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib

class DummyTestSet(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes=1)

    def setup(self, cluster):
        pass

    def teardown(self, cluster):
        pass

    def dummy1_test(self, cluster):
        pass

    def dummy2_test(self, cluster):
        pass
