# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
from datetime import datetime
import os
import re
from cryptography import x509


class AlertTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    def setup(self):
        testlib.diag_eval(self.cluster, 'menelaus_web_alerts_srv:reset().')
        limits = testlib.get_succ(self.cluster, '/settings/alerts/limits')\
            .json()
        self.prev_cert_expiration = limits['certExpirationDays']

        # Set alert check interval to 1s
        testlib.diag_eval(self.cluster,
                          'ns_config:set({timeout,{menelaus_web_alerts_srv,'
                          'sample_rate}}, 1000)')
        testlib.diag_eval(self.cluster,
                          'menelaus_web_alerts_srv ! check_alerts')

    def teardown(self):
        testlib.diag_eval(self.cluster, 'menelaus_web_alerts_srv:reset().')
        testlib.post_succ(self.cluster, '/settings/alerts/limits',
                          data={'certExpirationDays':
                                str(self.prev_cert_expiration)})

        # Set alert check interval back to default 60s
        testlib.diag_eval(self.cluster,
                          'ns_config:delete({timeout,{menelaus_web_alerts_srv,'
                          'sample_rate}})')

    def cert_about_to_expire_alert_test(self):
        node_data_dir = self.cluster.connected_nodes[0].data_path()
        certs_dir = os.path.join(node_data_dir, 'config', 'certs')
        cert_path = os.path.join(certs_dir, 'chain.pem')
        client_cert_path = os.path.join(certs_dir, 'client_chain.pem')
        expiration1 = get_expiration_for_cert(cert_path)
        expiration2 = get_expiration_for_cert(client_cert_path)
        max_expiration = max(expiration1, expiration2)

        testlib.post_succ(self.cluster, '/settings/alerts/limits',
                          data={'certExpirationDays': str(max_expiration)})

        def check_alert():
            r = testlib.get_succ(self.cluster, '/pools/default').json()
            alerts = r['alerts']
            if len(alerts) < 2:
                print(f"Alert check failed, expected >= 2 alerts, got {alerts}")
                return False

            regex = r'^Server certificate for node .+ will expire at .+$'

            def is_expected(x): return re.match(regex, x['msg']) is not None
            has_node_alert = any(map(is_expected, alerts))

            if not has_node_alert:
                print(f"Alert check failed, expected {regex}, got {alerts}")
                return False

            regex = r'^Client certificate on node .+ will expire at .+$'

            def is_expected(x): return re.match(regex, x['msg']) is not None
            has_client_alert = any(map(is_expected, alerts))

            if not has_client_alert:
                print(f"Alert check failed, expected {regex}, got {alerts}")
                return False

            return True

        testlib.poll_for_condition(check_alert, sleep_time=1, timeout=120,
                                   verbose=True,
                                   msg='wait for cert expiration alert')


def get_expiration_for_cert(cert_path):
    print(f'Extracting expiration for {cert_path}')
    with open(cert_path, 'rb') as f:
        pem = f.read()
    cert = x509.load_pem_x509_certificate(pem)
    expire_datetime = cert.not_valid_after
    print(f'expire_datetime: {expire_datetime}')
    now_datetime = datetime.utcnow()
    print(f'now: {now_datetime}')
    assert expire_datetime > now_datetime
    will_expire_in = (expire_datetime - now_datetime).days + 1
    print(f'cert will expire in {will_expire_in} days')
    return will_expire_in
