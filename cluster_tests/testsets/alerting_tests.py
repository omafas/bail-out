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
import random
import time
from cryptography import x509
import urllib.parse
from testsets.native_encryption_tests import create_secret, \
                                             delete_secret, \
                                             aws_test_secret, \
                                             write_good_aws_creds_file, \
                                             write_bad_aws_creds_file, \
                                             set_min_timer_interval

import sys
sys.path.append(testlib.get_pylib_dir())
import cluster_run_lib

alert_check_interval_s = 1 # seconds


class AlertTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes=2, num_connected=1,
                                           afamily="ipv4")

    def setup(self):
        testlib.diag_eval(self.cluster, "menelaus_web_alerts_srv:reset().")

        # Set alert check interval to 1s
        testlib.diag_eval(self.cluster,
                          "ns_config:set({timeout,{menelaus_web_alerts_srv,"
                          f"sample_rate}}}}, {alert_check_interval_s * 1000})")
        testlib.diag_eval(self.cluster,
                          "menelaus_web_alerts_srv ! check_alerts")

    def teardown(self):
        # Set alert check interval back to default 60s
        testlib.diag_eval(self.cluster,
                          "ns_config:delete({timeout,{menelaus_web_alerts_srv,"
                          "sample_rate}})")

    def test_teardown(self):
        testlib.diag_eval(self.cluster, "menelaus_web_alerts_srv:reset().")

    def cert_about_to_expire_alert_test(self):
        limits = testlib.get_succ(self.cluster, "/settings/alerts/limits")\
            .json()
        prev_cert_expiration = limits["certExpirationDays"]
        try:
            node_data_dir = self.cluster.connected_nodes[0].data_path()
            certs_dir = os.path.join(node_data_dir, "config", "certs")
            cert_path = os.path.join(certs_dir, "chain.pem")
            client_cert_path = os.path.join(certs_dir, "client_chain.pem")
            expiration1 = get_expiration_for_cert(cert_path)
            expiration2 = get_expiration_for_cert(client_cert_path)
            max_expiration = max(expiration1, expiration2)

            testlib.post_succ(self.cluster, "/settings/alerts/limits",
                            data={"certExpirationDays": str(max_expiration)})

            alert_regexps = [
                r"^Server certificate for node .+ will expire at .+$",
                r"^Client certificate on node .+ will expire at .+$"
            ]
            testlib.poll_for_condition(
                lambda: assert_alerts(self.cluster, alert_regexps),
                sleep_time=1, timeout=120, verbose=True, retry_on_assert=True,
                msg="wait for cert expiration alert")
        finally:
            testlib.post_succ(self.cluster, "/settings/alerts/limits",
                              data={"certExpirationDays":
                                    str(prev_cert_expiration)})

    def to_node(self):
        return self.cluster._nodes[1]

    def from_node(self):
        return self.cluster._nodes[0]

    def xdcr_replications_deleted_alert_test(self):
        def check_xdcr_up():
            r = testlib.get(self.cluster, "/pools/default/replications")
            return r.status_code == 200

        # If we do not wait, we will get an `econnrefused` error from goxdcr
        testlib.poll_for_condition(check_xdcr_up, sleep_time=1, timeout=30)
        resp = testlib.get_succ(self.cluster,
                                "/pools/default/replications").json()
        assert len(resp) == 0
        protocol = "ipv4"
        hostname = self.to_node().host
        testlib.post_succ(self.to_node(), "/nodeInit",
                          data={"afamily": protocol,
                                "hostname": hostname})
        data = {"afamily": "ipv4",
                "nodeEncryption": "off",
                "memoryQuota": 256,
                "port": "SAME",
                "username": "Administrator",
                "password": "asdasd",
                "services": "kv"}
        testlib.post_succ(self.to_node(), "/clusterInit", data=data)

        def check_node_ready():
            r = testlib.get(self.to_node(), "/pools/default")
            return r.status_code == 200

        testlib.poll_for_condition(check_node_ready, sleep_time=5, timeout=300,
                                   msg="wait for to_node to be ready")

        data = {"name": "A",
                "ramQuotaMB": 128}
        testlib.post_succ(self.from_node(), "/pools/default/buckets",
                          expected_code=202, data=data)
        testlib.post_succ(self.to_node(), "/pools/default/buckets",
                          expected_code=202, data=data)

        cluster_run_lib.wait_for_rebalance(
            f"http://{self.from_node().host}:{self.from_node().port}")
        cluster_run_lib.wait_for_rebalance(
            f"http://{self.to_node().host}:{self.to_node().port}")

        testlib.post_succ(self.cluster, "/pools/default/remoteClusters",
                          data={"name": "n_1",
                                "hostname":
                                f"{self.to_node().host}:{self.to_node().port}",
                                "port": self.to_node().port,
                                "username": "Administrator",
                                "password": "asdasd"}).json()

        def check_remote_cluster():
            diag_expr = "chronicle_kv:get(kv, {bucket, \"A\", last_balanced_vbmap})."
            r = testlib.diag_eval(self.to_node(), diag_expr)
            return r.text.startswith("{ok,")

        testlib.poll_for_condition(check_remote_cluster, sleep_time=1,
                                   timeout=120)
        testlib.post_succ(self.cluster, "/controller/createReplication",
                          data={"fromBucket": "A",
                                "replicationType": "continuous",
                                "toBucket": "A",
                                "toCluster": "n_1"}).json()

        def check_replication_created():
            r = testlib.get_succ(self.cluster,
                                 "/pools/default/replications").json()
            return len(r) >= 1

        testlib.poll_for_condition(check_replication_created, sleep_time=1,
                                   timeout=120)

        # Need to ensure that we've recorded the creation of the replication.
        def check_replication_recorded():
            statname = "xdcr_number_of_replications_total"
            response = testlib.get_succ(
                self.from_node(),
                f"/_prometheus/api/v1/query?query={statname}").json()
            value = response["data"]["result"]
            if len(value) == 0:
                return False
            repl_count = int(float(value[0]["value"][1]))
            return repl_count >= 1

        testlib.poll_for_condition(check_replication_recorded, sleep_time=1,
                                   timeout=60)
        alerts_json = testlib.get_succ(self.cluster, "/pools/default").json()
        for alert in alerts_json["alerts"]:
            assert "XDCR replication deleted on node" not in alert["msg"]

        replications = testlib.get_succ(self.cluster,
                                        "/pools/default/replications").json()
        for repl in replications:
            repl_id: str = repl["id"]
            repl_id = urllib.parse.quote(repl_id, safe="")
            resp = testlib.delete_succ(
                self.cluster,
                f"/controller/cancelXDCR/{repl_id}").json()

        def check_alert():
            r = testlib.get_succ(self.cluster, "/pools/default").json()
            for alert in r["alerts"]:
                if "XDCR replication deleted for target cluster" in alert["msg"]:
                    return True
            return False

        testlib.poll_for_condition(check_alert, sleep_time=1, timeout=60,
                                   msg="waiting for XDCR replication deleted alert")
        testlib.post_succ(self.to_node(), "/controller/hardResetNode", data={})

        def check_node_reset():
            r = testlib.get(self.to_node(), "/pools/default")
            return r.status_code != 200

        testlib.poll_for_condition(check_node_reset, sleep_time=5,
                                   timeout=300)

    def prometheus_metrics_alerts_test(self):
        eval_string = """lists:map(
  fun(T) -> menelaus_web_alerts_srv:local_alert({T, node()}, <<"test">>) end,
  menelaus_alert:alert_keys_all())."""

        r = testlib.diag_eval(self.cluster, eval_string)
        assert r.status_code == 200

        def check_alert_metric_recorded():
            statname = "cm_alerts_triggered_total"
            response = testlib.get_succ(
                self.from_node(),
                f"/_prometheus/api/v1/query?query={statname}").json()
            value = response["data"]["result"]
            if len(value) == 0:
                return False

            for metric in value:
                alert_value = metric["value"]
                if int(float(alert_value[1])) < 1:
                    return False
            return True

        testlib.poll_for_condition(check_alert_metric_recorded, sleep_time=2,
                                   timeout=120)

    def encr_at_rest_key_test_failed_alert_test(self):
        Key = '/settings/security/encryptionKeysTestIntervalSeconds'
        OrigInterval = testlib.get_succ(self.cluster, Key).json()
        aws_secret_id = None
        try:
            set_min_timer_interval(self.cluster, 1)
            testlib.post_succ(self.cluster, Key, data='1')
            bad_creds_node = random.choice(self.cluster.connected_nodes)
            creds_file = write_good_aws_creds_file(bad_creds_node)
            aws_secret = aws_test_secret(name='AWS Key', good_arn=True,
                                         creds_file=creds_file)
            aws_secret_id = create_secret(bad_creds_node, aws_secret)
            bad_node_hostname = bad_creds_node.addr()
            KeyAlertRegex = \
              r'^Encryption-at-Rest key validation event at .+: FAILED ' \
              fr'on node "{bad_node_hostname}" for key "AWS Key"\. ' \
              r'Error details: "encryption failed: test encryption error"\.$'

            # Wait for alerts to be checked, and make sure no alert is generated
            time.sleep(alert_check_interval_s + 1)
            assert_no_alerts(self.cluster, [KeyAlertRegex])

            # Write bad credentials and wait for alert to be generated
            write_bad_aws_creds_file(bad_creds_node)
            testlib.poll_for_condition(
                lambda: assert_alerts(self.cluster, [KeyAlertRegex]),
                sleep_time=1, timeout=60, verbose=True, retry_on_assert=True,
                msg='wait for key validation alert')

            # Delete the secret and wait for alert to disappear
            delete_secret(bad_creds_node, aws_secret_id)
            aws_secret_id = None
            testlib.poll_for_condition(
                lambda: assert_no_alerts(self.cluster, [KeyAlertRegex]),
                sleep_time=1, timeout=60, verbose=True, retry_on_assert=True,
                msg='wait for key validation alert')

        finally:
            # Restore original interval
            testlib.post_succ(self.cluster, Key, data=str(OrigInterval))
            set_min_timer_interval(self.cluster, None)
            if aws_secret_id is not None:
                delete_secret(bad_creds_node, aws_secret_id)


def get_expiration_for_cert(cert_path):
    print(f"Extracting expiration for {cert_path}")
    with open(cert_path, "rb") as f:
        pem = f.read()
    cert = x509.load_pem_x509_certificate(pem)
    expire_datetime = cert.not_valid_after
    print(f"expire_datetime: {expire_datetime}")
    now_datetime = datetime.utcnow()
    print(f"now: {now_datetime}")
    assert expire_datetime > now_datetime
    will_expire_in = (expire_datetime - now_datetime).days + 1
    print(f"cert will expire in {will_expire_in} days")
    return will_expire_in


def assert_alerts(cluster, expected_alerts_regexps):
    r = testlib.get_succ(cluster, "/pools/default").json()
    alerts = r["alerts"]
    print(f"alerts: {alerts}")
    expected_alerts_count = len(expected_alerts_regexps)
    assert len(alerts) >= expected_alerts_count, \
           f"Alert check failed, expected {expected_alerts_count} " \
           f"alerts, got {alerts}"

    for expected_regex in expected_alerts_regexps:
        present = lambda x: re.match(expected_regex, x["msg"]) is not None
        has_alert = any(map(present, alerts))
        assert has_alert, \
               f"Alert check failed, expected {expected_regex}, got {alerts}"
    return True


def assert_no_alerts(cluster, unwanted_alerts_regexps):
    r = testlib.get_succ(cluster, '/pools/default').json()
    alerts = r['alerts']
    print(f'alerts: {alerts}')
    for unwanted_regex in unwanted_alerts_regexps:
        present = lambda x: re.match(unwanted_regex, x['msg']) is not None
        has_alert = any(map(present, alerts))
        assert not has_alert, \
               f"Unwanted alert found, unwanted {unwanted_regex}, got {alerts}"
    return True
