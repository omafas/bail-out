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

from testlib.mock_smtp_server import start_mock_smtp_server
from testlib.test_tag_decorator import tag, Tag

import sys
sys.path.append(testlib.get_pylib_dir())
import cluster_run_lib

alert_check_interval_s = 1 # seconds


class AlertTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(
            num_nodes=2,
            num_connected=1,
            afamily="ipv4",
            buckets=[{"name": "A",
                      "ramQuota": 128}])

    def setup(self):
        testlib.diag_eval(self.cluster, "menelaus_web_alerts_srv:reset().")

        # Set alert check interval to 1s
        testlib.diag_eval(self.cluster,
                          "ns_config:set({timeout,{menelaus_web_alerts_srv,"
                          f"sample_rate}}}}, {alert_check_interval_s * 1000})")
        testlib.diag_eval(self.cluster,
                          "menelaus_web_alerts_srv ! check_alerts")

        # Disable certificate verification for self-signed certs when TLS
        # is enabled
        testlib.diag_eval(
            self.cluster,
            "ns_config:set({ns_mail, disable_verify_peer}, true).")

        # Set up mock SMTP server for email verification
        self.mock_smtp_server = self.setup_mock_email_server(
            smtp_host='127.0.0.1',
            smtp_port=None,  # auto-assign port
            sender='alerts_test@example.com',
            recipients='admin@example.com',
            use_tls=True,
            enable_alerts=None  # preserve existing/default alerts
        )

    def teardown(self):
        # Stop mock SMTP server and restore email configuration
        self.teardown_mock_email_server()

        # Set alert check interval back to default 60s
        testlib.diag_eval(self.cluster,
                          "ns_config:delete({timeout,{menelaus_web_alerts_srv,"
                          "sample_rate}})")

    def test_teardown(self):
        # Clear captured emails after each test to ensure clean state for next
        # test
        if hasattr(self, 'mock_smtp_server') and self.mock_smtp_server:
            try:
                self.mock_smtp_server.clear_emails()
            except Exception:
                pass

        replications = testlib.get_succ(self.from_node(),
                                        "/pools/default/replications").json()
        for repl in replications:
            repl_id: str = repl["id"]
            repl_id = urllib.parse.quote(repl_id, safe="")
            try:
                testlib.delete_succ(
                    self.from_node(),
                    f"/controller/cancelXDCR/{repl_id}")
            except Exception as e:
                print(f"Failed to delete replication {repl_id}: {e}")

        remotes = testlib.get(self.from_node(),
                              "/pools/default/remoteClusters").json()
        for r in remotes:
            if r["name"] == "n_1":
                # DELETE /pools/default/remoteClusters/n_1 returns 400
                # when remote doesn't exist, so can't use ensure_deleted here
                testlib.delete(self.from_node(),
                               "/pools/default/remoteClusters/n_1"),

        # Reset to_node cluster if it was initialized
        r = testlib.get(self.to_node(), "/pools/default")
        if r.status_code == 200:
            testlib.post_succ(self.to_node(), "/controller/hardResetNode",
                              data={})
            testlib.wait_for_ejected_node(self.to_node())

        testlib.diag_eval(self.cluster, "menelaus_web_alerts_srv:reset().")

    def setup_mock_email_server(self, smtp_host='127.0.0.1', smtp_port=None,
                                sender='test_sender@example.com',
                                recipients='test_recipient@example.com',
                                enable_alerts=None, use_tls=False):
        """
        Set up a mock SMTP server and configure the cluster to use it for
        email alerts.

        Args:
            smtp_host: Host for the mock SMTP server (default: 127.0.0.1)
            smtp_port: Port for the mock SMTP server (None for auto-assign)
            sender: Email sender address (default: test_sender@example.com)
            recipients: Comma-separated list of email recipients
            enable_alerts: List of alert types to enable for email (None to
                           preserve existing)
            use_tls: Whether to use TLS for SMTP (STARTTLS)

        Returns:
            SMTPServerRunner instance
        """
        # Create log file in cluster directory
        self.smtp_log_path = os.path.join(self.cluster.get_cluster_path(),
                                          'logs', 'mock_smtp.log')
        print(f"SMTP server log will be written to: {self.smtp_log_path}")

        # Start mock SMTP server
        self.mock_smtp_server = start_mock_smtp_server(
                                  host=smtp_host,
                                  port=smtp_port or 0,
                                  use_tls=use_tls,
                                  require_starttls=use_tls,
                                  log_file_path=self.smtp_log_path)
        actual_port = self.mock_smtp_server.port

        # Configure cluster email settings
        self.configure_email_alerts(
            enabled=True,
            sender=sender,
            recipients=recipients,
            smtp_host=smtp_host,
            smtp_port=actual_port,
            smtp_encrypt=use_tls,
            enable_alerts=enable_alerts
        )

        return self.mock_smtp_server

    def teardown_mock_email_server(self):
        """Stop the mock SMTP server and restore original email
        configuration."""
        if hasattr(self, 'mock_smtp_server') and self.mock_smtp_server:
            try:
                self.mock_smtp_server.clear_emails()
            except:
                pass
            try:
                self.mock_smtp_server.stop_server()
            except:
                pass
            self.mock_smtp_server = None

        # Restore certificate verification setting
        testlib.diag_eval(
            self.cluster,
            "ns_config:delete({ns_mail, disable_verify_peer}).")

        # Disable email alerts to restore default state
        self.configure_email_alerts(enabled=False)

    def configure_email_alerts(self, enabled=False,
                               sender='couchbase@localhost',
                               recipients='root@localhost',
                               smtp_host='localhost',
                               smtp_port=25,
                               smtp_user='',
                               smtp_pass='',
                               smtp_encrypt=False,
                               enable_alerts=None):
        """
        Configure email alert settings on the cluster.

        Args:
            enabled: Whether email alerts are enabled
            sender: Email sender address
            recipients: Comma-separated list of email recipients
            smtp_host: SMTP server hostname
            smtp_port: SMTP server port
            smtp_user: SMTP username
            smtp_pass: SMTP password
            smtp_encrypt: Whether to use TLS/SSL for SMTP
            enable_alerts: List of alert types to enable (None to preserve
                           existing)
        """
        # Get current alert configuration to preserve existing alerts when
        # not specified
        if enable_alerts is None:
            r = testlib.get_succ(self.cluster, '/settings/alerts')
            current_settings = r.json()
            enable_alerts = current_settings.get('alerts', [])
            print(f"Preserving {len(enable_alerts)} existing alert types")

        email_data = {
            'enabled': 'true' if enabled else 'false',
            'sender': sender,
            'recipients': recipients,
            'emailHost': smtp_host,
            'emailPort': str(smtp_port),
            'emailUser': smtp_user,
            'emailPass': smtp_pass,
            'emailEncrypt': 'true' if smtp_encrypt else 'false'
        }

        # Add alerts parameter if we have alerts to enable
        if enable_alerts is not None and enable_alerts != []:
            email_data['alerts'] = ','.join(alert for alert in enable_alerts)

        print(f"Configuring email alerts: {email_data}")
        testlib.post_succ(self.cluster, '/settings/alerts', data=email_data)

        # Verify the settings were applied
        response = testlib.get_succ(self.cluster, '/settings/alerts').json()
        assert response['enabled'] == enabled, \
            "Email alerts not configured correctly"
        print(f"Email alerts configured: enabled={enabled}, "
              f"alerts={len(response.get('alerts', []))}, "
              f"sender={response['sender']}, "
              f"recipients={response['recipients']}")

    def send_test_email_test(self):
        """Test sending a test email via /settings/alerts/testEmail."""
        # Clear any previously captured emails
        self.mock_smtp_server.clear_emails()

        # Get current email settings to get host/port
        settings = testlib.get_succ(self.cluster, '/settings/alerts').json()

        # Use new sender and multiple recipients
        test_sender = f'{testlib.random_str(10)}@example.com'
        test_recipients = ['recipient1@example.com',
                           'recipient2@example.com',
                           'recipient3@example.com']

        # Send test email - use real host/port but new sender/recipients
        test_data = {
            'subject': 'Test Email Subject',
            'body': 'This is a test email body',
            'enabled': 'true',
            'sender': test_sender,
            'recipients': ','.join(test_recipients),
            'emailHost': settings['emailServer']['host'],
            'emailPort': str(settings['emailServer']['port']),
            'emailUser': settings['emailServer']['user'],
            'emailPass': '',
            'emailEncrypt': 'true' if settings['emailServer']['encrypt']
                            else 'false'
        }

        testlib.post_succ(self.cluster, '/settings/alerts/testEmail',
                          data=test_data)

        # Wait for the email to be captured by the mock SMTP server
        def check_test_email_received():
            emails = self.mock_smtp_server.captured_emails
            for email in emails:
                if 'Test Email Subject' in email.subject:
                    print(f"Test email received: {email}")
                    # Verify sender
                    assert email.sender == test_sender, \
                        f"Sender mismatch: expected {test_sender}, " \
                        f"got {email.sender}"
                    # Verify body
                    assert 'This is a test email body' in email.body, \
                        f"Email body mismatch: {email.body}"
                    for recipient in test_recipients:
                        assert recipient in email.recipients, \
                            f"Recipient {recipient} did not receive the " \
                            f"email. Actual recipients: {test_email.recipients}"
                        print(f"  - {recipient}: OK")
                    return True
            return False

        testlib.poll_for_condition(check_test_email_received, sleep_time=1,
                                   timeout=30,
                                   msg='wait for test email to be received')

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
                lambda: assert_alerts(self.cluster, alert_regexps,
                                      verify_email=True,
                                      mock_smtp_server=self.mock_smtp_server),
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

    @tag(Tag.LowUrgency)
    def xdcr_replications_deleted_alert_test(self):
        def check_xdcr_up():
            r = testlib.get(self.cluster, "/pools/default/replications")
            return r.status_code == 200

        # If we do not wait, we will get an `econnrefused` error from goxdcr
        testlib.poll_for_condition(check_xdcr_up, sleep_time=1, timeout=30)
        resp = testlib.get_succ(self.cluster,
                                "/pools/default/replications").json()
        assert len(resp) == 0
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

        # Create bucket on to_node
        data = {"name": "A", "ramQuotaMB": 128}
        testlib.post_succ(self.to_node(), "/pools/default/buckets",
                          expected_code=202, data=data)

        cluster_run_lib.wait_for_rebalance(self.from_node().url)
        cluster_run_lib.wait_for_rebalance(self.to_node().url)

        testlib.post_succ(self.from_node(), "/pools/default/remoteClusters",
                          data={"name": "n_1",
                                "hostname":
                                f"{self.to_node().host}:{self.to_node().port}",
                                "username": "Administrator",
                                "password": "asdasd"}).json()

        def check_remote_vbmap():
            r = testlib.get_succ(self.to_node(), "/pools/default/buckets/A")
            info = r.json()
            vbServerMap = info["vBucketServerMap"]
            print(f"Checking the remote bucket vbmap: {vbServerMap}")
            return len(vbServerMap["vBucketMap"]) > 0

        def get_replications_num():
            statname = "xdcr_number_of_replications_total"
            response = testlib.get_succ(
                self.from_node(),
                f"/_prometheus/api/v1/query?query={statname}").json()
            value = response["data"]["result"]
            if len(value) == 0:
                print("xdcr_number_of_replications_total doesn't exist")
                return 0

            count = int(float(value[0]["value"][1]))
            print(f"xdcr_number_of_replications_total={count}")
            return count

        original_number_of_replications = get_replications_num()

        testlib.poll_for_condition(check_remote_vbmap, sleep_time=1,
                                   timeout=120)
        testlib.post_succ(self.from_node(), "/controller/createReplication",
                          data={"fromBucket": "A",
                                "replicationType": "continuous",
                                "toBucket": "A",
                                "toCluster": "n_1"}).json()

        def check_replication_created():
            r = testlib.get_succ(self.from_node(),
                                 "/pools/default/replications").json()
            return len(r) >= 1

        testlib.poll_for_condition(check_replication_created, sleep_time=1,
                                   timeout=120)

        # Need to ensure that we've recorded the creation of the replication.

        def check_replication_recorded():
            return get_replications_num() > original_number_of_replications

        testlib.poll_for_condition(check_replication_recorded, sleep_time=1,
                                   timeout=60)

        r = testlib.get_succ(self.to_node(),
                             '/pools/default/terseClusterInfo')
        ToClusterUUID = r.json()['clusterUUID']
        alert_re = (
            ".+XDCR replication deleted for target cluster UUID: "
            f"{ToClusterUUID}.*")

        alerts_json = testlib.get_succ(self.from_node(),
                                       "/pools/default").json()
        print(f"Pre-existing alerts: {alerts_json['alerts']}")
        for alert in alerts_json["alerts"]:
            assert re.match(alert_re, alert["msg"]) is None

        replications = testlib.get_succ(self.from_node(),
                                        "/pools/default/replications").json()
        for repl in replications:
            repl_id: str = repl["id"]
            repl_id = urllib.parse.quote(repl_id, safe="")
            resp = testlib.delete_succ(
                self.from_node(),
                f"/controller/cancelXDCR/{repl_id}").json()

        testlib.poll_for_condition(
            lambda: assert_alerts(self.from_node(), [alert_re],
                                  verify_email=True,
                                  mock_smtp_server=self.mock_smtp_server),
            sleep_time=1, timeout=60, verbose=True, retry_on_assert=True,
            msg='wait for key validation alert')

    @tag(Tag.LowUrgency)
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
              r'Error details: "encryption failed: test encryption error"\.'

            # Wait for alerts to be checked, and make sure no alert is generated
            time.sleep(alert_check_interval_s + 1)
            assert_no_alerts(self.cluster, [KeyAlertRegex])

            # Write bad credentials and wait for alert to be generated
            write_bad_aws_creds_file(bad_creds_node)
            testlib.poll_for_condition(
                lambda: assert_alerts(self.cluster, [KeyAlertRegex],
                                      verify_email=True,
                                      mock_smtp_server=self.mock_smtp_server),
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


def assert_alerts(cluster, expected_alerts_regexps, verify_email=False,
                  mock_smtp_server=None):
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

    # Verify email alerts were sent if requested
    if verify_email:
        if mock_smtp_server is None:
            raise ValueError("verify_email=True requires mock_smtp_server "
                             "parameter")

        captured_emails = mock_smtp_server.captured_emails
        print(f"captured {len(captured_emails)} emails: ")
        for m in captured_emails:
            print(f"From: {m.sender}\n"
                  f"To: {m.recipients}\n"
                  f"Subject: {m.subject}\n"
                  f"Body: {m.body}")

        # Fail if fewer emails were captured than expected alert patterns
        assert len(captured_emails) >= expected_alerts_count, \
               f"Expected {expected_alerts_count} emails, got " \
               f"{len(captured_emails)}"

        # Check that emails were sent for each expected alert pattern
        for expected_regex in expected_alerts_regexps:
            found_email = False
            for email in captured_emails:
                # Check if alert message appears in either subject or body
                if re.search(expected_regex, email.subject) or \
                   re.search(expected_regex, email.body):
                    found_email = True
                    print(f"Found email matching '{expected_regex}': "
                          f"{email.subject}")
                    break

            assert found_email, \
                   f"Expected email for pattern: {expected_regex}"

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
