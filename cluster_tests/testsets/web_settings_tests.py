# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
import json


class WebSettingsTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise")

    def setup(self):
        pass

    def teardown(self):
        self.delete_security_settings('/settings/security')
        self.delete_security_settings('/settings/security/clusterManager')
        pass

    def basic_security_settings_api_test(self):
        path = '/settings/security'
        self.set_and_verify_security_settings_all_at_once(path)
        self.set_and_verify_security_settings_one_by_one(path)
        self.delete_security_settings(path)
        r = testlib.get_succ(self.cluster, path).json()
        testlib.assert_eq(True, r['honorCipherOrder'])
        testlib.assert_eq('tlsv1.2', r['tlsMinVersion'])
        testlib.assert_eq([], r['cipherSuites'])

    # set and delete all at once
    def basic_security_settings_api_cluster_manager_1_test(self):
        path = '/settings/security/clusterManager'
        self.set_and_verify_security_settings_all_at_once(path)
        testlib.delete_succ(self.cluster, path)
        r = testlib.get_succ(self.cluster, path).json()
        testlib.assert_not_in('honorCipherOrder', r)
        testlib.assert_not_in('tlsMinVersion', r)
        testlib.assert_not_in('cipherSuites', r)

    # set and delete one by one
    def basic_security_settings_api_cluster_manager_2_test(self):
        path = '/settings/security/clusterManager'
        self.set_and_verify_security_settings_one_by_one(path)
        self.delete_security_settings(path)
        r = testlib.get_succ(self.cluster, path).json()
        testlib.assert_not_in('honorCipherOrder', r)
        testlib.assert_not_in('tlsMinVersion', r)
        testlib.assert_not_in('cipherSuites', r)

    def set_and_verify_security_settings_all_at_once(
            self, base_endpoint,
            honor_order = False,
            min_vsn = 'tlsv1.3',
            ciphers = ["TLS_AES_128_GCM_SHA256",
                       "TLS_ECDHE_ECDSA_WITH_AES_128_CCM"]):
        testlib.post_succ(self.cluster, base_endpoint,
                          data = {'cipherSuites': json.dumps(ciphers),
                                  'honorCipherOrder': 'true' if honor_order
                                                       else 'false',
                                  'tlsMinVersion': min_vsn})
        self.verify_security_settings(base_endpoint, honor_order, min_vsn,
                                      ciphers)

    def set_and_verify_security_settings_one_by_one(
            self, base_endpoint,
            honor_order = False,
            min_vsn = 'tlsv1.3',
            ciphers = ["TLS_AES_128_GCM_SHA256", \
                       "TLS_ECDHE_ECDSA_WITH_AES_128_CCM"]):
        testlib.post_succ(self.cluster, f'{base_endpoint}/cipherSuites',
                          data = json.dumps(ciphers))
        testlib.post_succ(self.cluster, f'{base_endpoint}/honorCipherOrder',
                          data = 'true' if honor_order else 'false')
        testlib.post_succ(self.cluster, f'{base_endpoint}/tlsMinVersion',
                          data = min_vsn)
        self.verify_security_settings(base_endpoint, honor_order, min_vsn,
                                      ciphers)

    def verify_security_settings(self, base_endpoint, honor_order, min_vsn,
                                 ciphers):
        r = testlib.get_succ(self.cluster, base_endpoint).json()
        testlib.assert_eq(honor_order, r['honorCipherOrder'])
        testlib.assert_eq(min_vsn, r['tlsMinVersion'])
        testlib.assert_eq(ciphers, r['cipherSuites'])
        r = testlib.get_succ(self.cluster,
                             f'{base_endpoint}/honorCipherOrder').json()
        testlib.assert_eq(honor_order, r)
        r = testlib.get_succ(self.cluster,
                             f'{base_endpoint}/tlsMinVersion').json()
        testlib.assert_eq(min_vsn, r)
        r = testlib.get_succ(self.cluster,
                             f'{base_endpoint}/cipherSuites').json()
        testlib.assert_eq(ciphers, r)

    def delete_security_settings(self, base_endpoint):
        testlib.delete_succ(self.cluster, f'{base_endpoint}/tlsMinVersion')
        testlib.delete_succ(self.cluster, f'{base_endpoint}/honorCipherOrder')
        testlib.delete_succ(self.cluster, f'{base_endpoint}/cipherSuites')
