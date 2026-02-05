import testlib
import subprocess
import json
from testsets.secret_management_tests import change_password

class CLIIntegrationTests(testlib.BaseTestSet):
    def __init__(self, cluster, set_master_password=False):
        super().__init__(cluster)
        self.master_password = None
        self.set_master_password = set_master_password

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition='Enterprise')

    def node(self):
        return self.cluster.connected_nodes[0]

    def setup(self):
        if self.set_master_password:
            self.master_password = change_password(self.node())

    def teardown(self):
        if self.set_master_password:
            change_password(self.node(), password='')

    def reset_password_test(self):
        admin, cur_pass = self.node().auth
        try:
            new_pass = testlib.random_str(8)
            reset_password(self.node(), new_pass,
                           master_password=self.master_password)
            # Old password should not work
            testlib.get_fail(self.node(), '/nodes/self', expected_code=401)
            # New password should work
            testlib.get_succ(self.node(), '/nodes/self', auth=(admin, new_pass))
        finally:
            # Reset password back
            reset_password(self.node(), cur_pass,
                           master_password=self.master_password)

        testlib.get_succ(self.node(), '/nodes/self')

    def reset_ciphers_test(self):
        ciphers = ["TLS_AES_256_GCM_SHA384",
                   "TLS_CHACHA20_POLY1305_SHA256",
                   "TLS_AES_128_GCM_SHA256",
                   "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
                   "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                   "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                   "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                   "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
                   "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
                   "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
                   "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384"]
        try:
            set_ciphers(self.node(), ciphers)
            server_ciphers = get_ciphers(self.node())
            assert len(server_ciphers) > 0, f'Ciphers not set: {server_ciphers}'
        finally:
            reset_ciphers(self.node(), master_password=self.master_password)

        server_ciphers = get_ciphers(self.node())
        assert len(server_ciphers) == 0, f'Ciphers not reset: {server_ciphers}'

    def lock_unlock_admin_test(self):
        try:
            admin_manage(self.node(), '--lock',
                         master_password=self.master_password)
            testlib.get_fail(self.node(), '/nodes/self', expected_code=401)
        finally:
            admin_manage(self.node(), '--unlock',
                         master_password=self.master_password)

        testlib.get_succ(self.node(), '/nodes/self')

    def remsh_test(self):
        res = cli_with_cluster_url(self.node(), 'server-eshell',
                                   '--eval', 'node().')
        # Very rarely the output can contain '{removed_failing_handler,default}'
        # for unknown reason.
        # This seems to be a bug but it is harmless and hard to reproduce.
        # For this reason, using assert_in() instead of assert_eq() here.
        testlib.assert_in(self.node().otp_node(), res)


class CLIIntegrationTestsWithMasterPass(CLIIntegrationTests):
    def __init__(self, cluster):
        super().__init__(cluster, set_master_password=True)

def admin_manage(node, actioni, master_password=None):
    cli_with_port_and_path(node, 'admin-manage', actioni,
                           master_password=master_password)


def reset_password(node, new_password, master_password=None):
    cli_with_port_and_path(node, 'reset-admin-password',
                           '--new-password', new_password,
                           master_password=master_password)


def reset_ciphers(node, master_password=None):
    cli_with_port_and_path(node, 'reset-cipher-suites', '--force',
                           master_password=master_password)


def set_ciphers(node, ciphers):
    setting_security(node, '--set', '--cipher-suites', ','.join(ciphers))


def get_ciphers(node):
    resp = json.loads(setting_security(node, '--get'))
    return resp['cipherSuites']


def setting_security(node, *args):
    return cli_with_cluster_url(node, 'setting-security', *args)


def cli_with_port_and_path(node, cmd, *other_args, master_password=None):
    cli_args = [cmd, '-P', str(node.port), '--config-path', node.data_path()]
    if master_password is not None:
        cli_args.extend(['--master-password', master_password])

    return cli(*cli_args, *other_args)


def cli_with_cluster_url(node, cmd, *args):
    (admin, password) = node.auth
    return cli(cmd, '-c', node.service_url(None), '-u', admin, '-p', password,
               *args)


def cli(cmd, *args):
    cli_path = testlib.get_utility_path('couchbase-cli')
    all_args = [cli_path, cmd] + list(args)
    cmd = ' '.join(all_args)
    print(f'Running: {cmd}')
    r = subprocess.run(all_args, capture_output=True)
    assert r.returncode == 0, f'Command failed\nCmd: {cmd}\n' \
                              f'stdout: {r.stdout}\nstderr: {r.stderr}'
    return str(r.stdout, 'utf-8')
