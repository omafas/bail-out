# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
from testlib.requirements import Service


class IntCredsRotationTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(min_num_nodes=2,
                                           include_services=[Service.KV,
                                                             Service.QUERY])

    def setup(self):
        d = 'ns_config:read_key_fast(int_creds_protection_sleep, undefined).'
        r = testlib.post_succ(self.cluster, '/diag/eval', data=d)
        self.orig_protection_sleep = r.text
        d = 'ns_config:set(int_creds_protection_sleep, 5000).'
        testlib.post_succ(self.cluster, '/diag/eval', data=d)


    def teardown(self):
        if self.orig_protection_sleep == 'undefined':
            d = 'ns_config:delete(int_creds_protection_sleep).'
        else:
            d = f'ns_config:set(int_creds_protection_sleep, ' \
                               '{self.orig_protection_sleep}).'
        testlib.post_succ(self.cluster, '/diag/eval', data=d)


    def on_demand_rotation_test(self):
        old_pass = get_pass(self.cluster.connected_nodes[0])
        testlib.get_succ(self.cluster.connected_nodes[0], '/pools/default',
                         auth=('@', old_pass))
        testlib.post_succ(self.cluster.connected_nodes[0],
                          '/node/controller/rotateInternalCredentials')
        # Old passoword is not working anymore
        testlib.get_fail(self.cluster.connected_nodes[0], '/pools/default', 401,
                         auth=('@', old_pass))
        # ... while new password works
        new_pass = get_pass(self.cluster.connected_nodes[0])
        testlib.get_succ(self.cluster.connected_nodes[0], '/pools/default',
                         auth=('@', new_pass))


    def periodic_rotation_test(self):
        interval_s = 5
        r = testlib.get_succ(self.cluster, '/settings/security')
        curr_interval = r.json()['intCredsRotationInterval']
        try:
            testlib.post_succ(self.cluster,
                              '/settings/security/intCredsRotationInterval',
                              data=str(interval_s * 1000))

            print(f"rotation interval is set to {interval_s}")
            # run on demand rotation in order to make sure this node has
            # finished all previous rotations; this is basically a "sync" call
            testlib.post_succ(self.cluster.connected_nodes[0],
                              '/node/controller/rotateInternalCredentials')
            print(f"on demand rotation finished")

            old_pass = get_pass(self.cluster.connected_nodes[0])

            def pass_has_changed():
                new_pass = get_pass(self.cluster.connected_nodes[0])
                return new_pass != old_pass

            testlib.poll_for_condition(pass_has_changed, sleep_time=1,
                                       timeout=60, verbose=True)
        finally:
            testlib.post_succ(self.cluster,
                              '/settings/security/intCredsRotationInterval',
                              data=str(curr_interval))



def get_pass(node):
    r = testlib.post_succ(node, '/diag/eval',
                          data='ns_config_auth:get_password(special).')
    return r.text.strip('"')
