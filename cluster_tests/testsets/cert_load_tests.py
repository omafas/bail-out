# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
import os
import ipaddress
import subprocess
import sys
import time

scriptdir = sys.path[0]
certs_path = os.path.join(scriptdir, 'resources', 'test_certs')
generate_cert_path = os.path.join(scriptdir, '..', 'deps', 'gocode', 'bin',
                                  'generate_cert')
openssl_path = os.path.join(scriptdir, '..', '..', 'install', 'bin', 'openssl')


class CertLoadTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition='Enterprise')

    def setup(self):
        ca_path = os.path.join(certs_path, 'test_CA.pem')
        with open(ca_path, 'r') as f:
            self.ca_pem = f.read()
        ca_key = os.path.join(certs_path, 'test_CA.pkey')
        with open(ca_key, 'r') as f:
            self.ca_key = f.read()

        self.node_addr = self.cluster.connected_nodes[0].addr()
        self.loaded_CA_ids = load_ca(self.cluster.connected_nodes[0],
                                     self.ca_pem)

    def teardown(self):
        testlib.post_succ(self.cluster, '/controller/regenerateCertificate',
                          params={'forceResetCACertificate': 'false',
                                  'dropUploadedCertificates': 'true'})
        for ca_id in self.loaded_CA_ids:
            testlib.delete_succ(self.cluster,
                                f'/pools/default/trustedCAs/{ca_id}',
                                expected_code=204)

    def rsa_private_key_pkcs1_test(self):
        self.generate_and_load_cert('rsa')

    def rsa_private_key_pkcs8_test(self):
        self.generate_and_load_cert('rsa', pkcs8=True)

    def rsa_private_key_pkcs8_encrypted_test(self):
        self.generate_and_load_cert('rsa', pkcs8=True,
                                         passphrase=testlib.random_str(8))

    def ec_private_key_test(self):
        self.generate_and_load_cert('ec')

    def ec_private_key_pkcs8_test(self):
        self.generate_and_load_cert('ec', pkcs8=True)

    def ec_private_key_pkcs8_encrypted_test(self):
        self.generate_and_load_cert('ec', pkcs8=True,
                                         passphrase=testlib.random_str(8))

    def client_cert_with_rsa_key_test(self):
        self.generate_and_load_cert('rsa', is_client=True)

    def client_cert_with_ec_key_test(self):
        self.generate_and_load_cert('ec', is_client=True)

    def client_cert_with_rsa_pkcs8_key_test(self):
        self.generate_and_load_cert('rsa', is_client=True, pkcs8=True)

    def client_cert_with_ec_pkcs8_key_test(self):
        self.generate_and_load_cert('ec', is_client=True, pkcs8=True)

    def client_cert_with_rsa_pkcs8_encrypted_key_test(self):
        self.generate_and_load_cert('rsa', is_client=True, pkcs8=True,
                                    passphrase=testlib.random_str(8))

    def client_cert_with_ec_pkcs8_encrypted_key_test(self):
        self.generate_and_load_cert('ec', is_client=True, pkcs8=True,
                                    passphrase=testlib.random_str(8))

    def client_pkcs12_with_rsa_key_test(self):
        self.generate_and_load_pkcs12_cert('rsa', is_client=True)

    def client_pkcs12_with_ec_key_test(self):
        self.generate_and_load_pkcs12_cert('ec', is_client=True)

    def client_pkcs12_with_encrypted_rsa_key_test(self):
        self.generate_and_load_pkcs12_cert('rsa', is_client=True,
                                           passphrase=testlib.random_str(8))

    def client_pkcs12_with_encrypted_ec_key_test(self):
        self.generate_and_load_pkcs12_cert('ec', is_client=True,
                                           passphrase=testlib.random_str(8))

    def generate_and_load_cert(self, key_type, is_client=False,
                               pkcs8=False, passphrase=None):
        if is_client:
            cert, key = generate_internal_client_cert(self.ca_pem, self.ca_key,
                                                      'test_name')
        else:
            cert, key = generate_node_certs(self.node_addr,
                                            self.ca_pem, self.ca_key,
                                            key_type=key_type)
        if pkcs8 == False:
            assert passphrase is None, \
                   'encryption is supported only for pkcs8 keys'
        if pkcs8:
            key = to_pkcs8(key, passphrase)
        load_cert(self.cluster.connected_nodes[0], cert, key, passphrase,
                  is_client=is_client)

    def pkcs12_rsa_key_test(self):
        self.generate_and_load_pkcs12_cert('rsa')

    def pkcs12_ec_key_test(self):
        self.generate_and_load_pkcs12_cert('ec')

    def pkcs12_encrypted_rsa_key_test(self):
        self.generate_and_load_pkcs12_cert('rsa',
                                           passphrase=testlib.random_str(8))

    def pkcs12_encrypted_ec_key_test(self):
        self.generate_and_load_pkcs12_cert('ec',
                                           passphrase=testlib.random_str(8))

    ## Reload node certificates is disruptive to the system. So if the cert
    ## is the same as what is in use we skip the reloading.
    def short_circuit_reloading_node_cert_test(self):

        def load_cert_return_timestamp(force=False):
            load_cert(self.cluster.connected_nodes[0], cert, key,
                      passphrase=None, is_client=False, force_reload=force)
            r = testlib.get_succ(self.cluster.connected_nodes[0],
                                 f'/pools/default/certificate/'
                                 f'node/{self.cluster.connected_nodes[0]}')
            r = r.json()
            return r['loadTimestamp']

        cert, key = generate_node_certs(self.node_addr,
                                        self.ca_pem, self.ca_key)

        ts1 = load_cert_return_timestamp(force=False)
        time.sleep(2)
        ts2 = load_cert_return_timestamp(force=False)
        ## Because the certs haven't changed the returned timestamp will
        ## not have changed.
        assert (ts1 == ts2)
        ts3 = load_cert_return_timestamp(force=True)
        ## The reload was forced so the returned timestamp will be different.
        assert (ts1 != ts3)

    def generate_and_load_pkcs12_cert(self, key_type, passphrase=None,
                                      is_client=False):
        if is_client:
            cert, key = generate_internal_client_cert(self.ca_pem, self.ca_key,
                                                      'test_name')
        else:
            cert, key = generate_node_certs(self.node_addr,
                                            self.ca_pem, self.ca_key,
                                            key_type=key_type)

        node_data_path = self.cluster.connected_nodes[0].data_path()
        inbox_dir = os.path.join(node_data_path, 'inbox')
        os.makedirs(inbox_dir, exist_ok=True)
        filename = 'couchbase_client.p12' if is_client else 'couchbase.p12'
        pkcs12_path = os.path.join(inbox_dir, filename)
        try:
            write_pkcs12(cert, key, pkcs12_path,
                         passphrase=passphrase)
            data = {'privateKeyPassphrase': {'type': 'plain',
                                             'password': passphrase}} \
                   if passphrase is not None else None
            endpoint = 'reloadClientCertificate' if is_client \
                       else 'reloadCertificate'
            testlib.post_succ(self.cluster, f'/node/controller/{endpoint}',
                              json=data)
        finally:
            if os.path.exists(pkcs12_path):
                os.remove(pkcs12_path)


def load_node_cert(node, cert, key, passphrase=None):
    load_cert(node, cert, key, passphrase, is_client=False)


def load_client_cert(node, cert, key, passphrase=None):
    load_cert(node, cert, key, passphrase, is_client=True)


def load_cert(node, cert, key, passphrase, is_client, force_reload=False):
    inbox_dir = os.path.join(node.data_path(), 'inbox')
    chain_file_name = 'client_chain.pem' if is_client else 'chain.pem'
    chain_path = os.path.join(inbox_dir, chain_file_name)
    pkey_file_name = 'client_pkey.key' if is_client else 'pkey.key'
    pkey_path = os.path.join(inbox_dir, pkey_file_name)
    os.makedirs(inbox_dir, exist_ok=True)
    try:
        with open(chain_path, 'w') as f:
            f.write(cert)
        with open(pkey_path, 'w') as f:
            f.write(key)
        endpoint = 'reloadClientCertificate' if is_client \
                   else 'reloadCertificate'
        data = None
        if passphrase is not None:
            data = {'privateKeyPassphrase': {'type': 'plain',
                                             'password': passphrase}}
        if force_reload:
            if data is None:
                data = {'forceReload': True}
            else:
                data['forceReload'] = True
        testlib.post_succ(node, f'/node/controller/{endpoint}', json=data)
    finally:
        if os.path.exists(chain_path):
            os.remove(chain_path)
        if os.path.exists(pkey_path):
            os.remove(pkey_path)


def load_ca(node, CA):
    ca_dir = os.path.join(node.data_path(), 'inbox', 'CA')
    ca_path = os.path.join(ca_dir, 'ca.pem')
    os.makedirs(ca_dir, exist_ok=True)
    try:
        with open(ca_path, 'w') as f:
            f.write(CA)

        r = testlib.post_succ(node, '/node/controller/loadTrustedCAs')
        r = r.json()
        # Returning a list mostly to handle the case when that certificate
        # is already loaded. In this case we don't need to remove it in
        # teardown()
        return [c['id'] for c in r]
    finally:
        if os.path.exists(ca_path):
            os.remove(ca_path)


def run_generate_cert(args, env):
    r = subprocess.run([generate_cert_path] + args,
                       capture_output=True, env=env)
    assert r.returncode == 0, f'generate_cert returned {r.returncode}'

    separator = '-----END CERTIFICATE-----'
    tokens = r.stdout.decode().split(separator)
    assert len(tokens) == 2, f'unexpected return of generate_cert: {r.output}'
    cert = tokens[0] + separator
    key = tokens[1]
    print(f'Generated cert: {cert}\nGenerated key: {key}')
    return (cert, key)


def generate_internal_client_cert(CA, CAKey, name_in_cert):
    return generate_client_cert(CA, CAKey,
                                email=f'{name_in_cert}@internal.couchbase.com')


def generate_client_cert(CA, CAKey, cn="TEST CLIENT CERT",
                         email='test_client@example.com'):
    args = ['--generate-leaf', '--common-name', cn,
            '--san-emails', email, '--client']

    return run_generate_cert(args, {'CACERT': CA, 'CAPKEY': CAKey})


def generate_node_certs(node_addr, CA, CAKey, key_type='rsa'):
    try:
        ipaddress.ip_address(node_addr)
        is_raw = True
    except ValueError:
        is_raw = False

    args = ['--generate-leaf', '--common-name', 'TEST Server Node',
            '--san-ip-addrs' if is_raw else '--san-dns-names', node_addr,
            '--pkey-type', key_type]

    return run_generate_cert(args, {'CACERT': CA, 'CAPKEY': CAKey})


def to_pkcs8(key, passphrase):
    args = ['pkcs8', '-topk8']
    encr_args = ['-v2', 'aes256', '-passout', f'pass:{passphrase}'] \
                if passphrase is not None else ['-nocrypt']
    r = subprocess.run([openssl_path] + args + encr_args, capture_output=True,
                       input=key.encode("utf-8"))
    assert r.returncode == 0, f'openssl pkcs8 returned {r.returncode}\n' \
                              f'stdout: {r.stdout.decode()}\n' \
                              f'stderr: {r.stderr.decode()}'
    return r.stdout.decode()


def write_pkcs12(cert, key, out_file, passphrase=None):
    s = testlib.random_str(8)
    in_key_path = os.path.join(os.path.dirname(out_file), f"temp_key_{s}.pem")
    in_crt_path = os.path.join(os.path.dirname(out_file), f"temp_cert_{s}.pem")
    try:
        with open(in_key_path, 'w') as f:
            f.write(key)
        with open(in_crt_path, 'w') as f:
            f.write(cert)

        args = ['pkcs12', '-export', '-out', out_file, '-inkey', in_key_path,
                '-in', in_crt_path]

        encr_args = ['-aes128', '-passout', f'pass:{passphrase}'] \
                    if passphrase is not None else \
                    ['-keypbe',  'NONE', '-certpbe', 'NONE', '-nomaciter',
                     '-passout', 'pass:']

        r = subprocess.run([openssl_path] + args + encr_args,
                           capture_output=True)
        assert r.returncode == 0, f'openssl pkcs12 returned {r.returncode}\n' \
                                  f'stdout: {r.stdout.decode()}\n' \
                                  f'stderr: {r.stderr.decode()}'
    finally:
        if os.path.exists(in_crt_path):
            os.remove(in_crt_path)
        if os.path.exists(in_key_path):
            os.remove(in_key_path)


def read_cert_file(filename):
    with open(os.path.join(certs_path, filename), 'r') as f:
        pem = f.read()
    return pem
