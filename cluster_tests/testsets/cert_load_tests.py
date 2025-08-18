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
        return testlib.ClusterRequirements(edition='Enterprise',
                                           min_num_nodes=2,
                                           encryption=True)

    def setup(self):
        # Need to remove extras,
        regenerate_certs_and_remove_unused_generated_cas(self.cluster)

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

    def generate_and_load_cert(self, key_type, node=None, is_client=False,
                               pkcs8=False, passphrase=None):
        if node is None:
            node = self.cluster.connected_nodes[0]
        node_addr = node.addr()
        if is_client:
            cert, key = generate_internal_client_cert(self.ca_pem, self.ca_key,
                                                      'test_name')
        else:
            cert, key = generate_node_certs(node_addr,
                                            self.ca_pem, self.ca_key,
                                            key_type=key_type)
        if pkcs8 == False:
            assert passphrase is None, \
                   'encryption is supported only for pkcs8 keys'
        if pkcs8:
            key = to_pkcs8(key, passphrase)
        return load_cert(node, cert, key, passphrase,
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

    def regen_certs_test(self):
        node_certs_before = self.load_custom_certs()

        original_trusted_cas = get_trusted_CAs(self.cluster)
        original_trusted_ca_pems = [ca.get('pem')
                                    for ca in original_trusted_cas]
        try:
            new_ca_pem = regenerate_certs(self.cluster,
                                          force_reset_ca=False,
                                          drop_uploaded_certs=False)
            assert new_ca_pem in original_trusted_ca_pems, \
                "CA regenerated when force_reset=false "
            # CA should not be regenerated, and should not become trusted
            assert_trusted_CAs_unchanged(self.cluster, original_trusted_cas)
            assert_no_certs_changed(self.cluster, node_certs_before, None)
        finally:
            regenerate_certs_and_remove_unused_generated_cas(self.cluster,
                                                             node_certs_before)

    def regen_and_drop_certs_test(self):
        node_certs_before = self.load_custom_certs()

        original_trusted_cas = get_trusted_CAs(self.cluster)
        original_trusted_ca_pems = [ca.get('pem')
                                    for ca in original_trusted_cas]
        try:
            new_ca_pem = regenerate_certs(self.cluster,
                                          force_reset_ca=False,
                                          drop_uploaded_certs=True)
            # Since we're dropping the uploaded certs, but not resetting the ca,
            # the new_ca_pem won't actually be used by the new node/client certs
            wait_all_nodes_updated(self.cluster, node_certs_before, None)
            assert new_ca_pem in original_trusted_ca_pems, \
                "CA regenerated when force_reset=false "
            # CA should not be regenerated
            assert_trusted_CAs_unchanged(self.cluster, original_trusted_cas)
        finally:
            regenerate_certs_and_remove_unused_generated_cas(self.cluster,
                                                             node_certs_before)

    def regen_force_reset_certs_test(self):
        node_certs_before = self.load_custom_certs()

        original_trusted_cas = get_trusted_CAs(self.cluster)
        original_trusted_ca_pems = [ca.get('pem')
                                    for ca in original_trusted_cas]
        try:
            new_ca_pem = regenerate_certs(self.cluster,
                                          force_reset_ca=True,
                                          drop_uploaded_certs=False)
            assert_no_certs_changed(self.cluster, node_certs_before, new_ca_pem)
            # CA should be regenerated, and newly trusted
            assert new_ca_pem not in original_trusted_ca_pems, \
                "CA not regenerated when force_reset=true"
            assert_CA_newly_trusted(self.cluster, original_trusted_cas,
                                    new_ca_pem)
        finally:
            regenerate_certs_and_remove_unused_generated_cas(self.cluster,
                                                             node_certs_before)

    def regen_force_reset_and_drop_certs_test(self):
        node_certs_before = self.load_custom_certs()

        original_trusted_cas = get_trusted_CAs(self.cluster)
        original_trusted_ca_pems = [ca.get('pem')
                                    for ca in original_trusted_cas]
        try:
            new_ca_pem = regenerate_certs(self.cluster,
                                          force_reset_ca=True,
                                          drop_uploaded_certs=True)
            wait_all_nodes_updated(self.cluster, node_certs_before, new_ca_pem)

            # CA should be regenerated, and newly trusted
            assert new_ca_pem not in original_trusted_ca_pems, \
                "CA not regenerated when force_reset=true"
            assert_CA_newly_trusted(self.cluster, original_trusted_cas,
                                    new_ca_pem)
        finally:
            regenerate_certs_and_remove_unused_generated_cas(self.cluster,
                                                             node_certs_before)

    def regen_certs_with_untrusted_ca_test(self):
        node_certs_before = self.load_custom_certs()

        original_trusted_cas = get_trusted_CAs(self.cluster)
        original_trusted_ca_pems = [ca.get('pem')
                                    for ca in original_trusted_cas]
        original_trusted_generated_cas = [ca for ca in original_trusted_cas
                                          if ca.get('type') == 'generated']
        original_trusted_generated_ca_ids = [
            ca.get('id') for ca in original_trusted_generated_cas]
        try:
            # Remove the generated ca, to test re-trusting
            [ootb_ca] = original_trusted_generated_ca_ids
            testlib.delete_succ(self.cluster,
                                f'/pools/default/trustedCAs/{ootb_ca}',
                                expected_code=204)
            # Get updated ca lists, for comparison
            trusted_cas_before = get_trusted_CAs(self.cluster)
            trusted_generated_ca_pems_before = \
                [ca.get('pem')
                 for ca in trusted_cas_before
                 if ca.get('type') == 'generated']

            new_ca_pem = regenerate_certs(self.cluster,
                                          force_reset_ca=False,
                                          drop_uploaded_certs=False)
            assert_no_certs_changed(self.cluster, node_certs_before, None)
            # CA should not be regenerated, and shouldn't become trusted (as the
            # certs weren't force-dropped
            assert new_ca_pem not in trusted_generated_ca_pems_before, \
                "CA wasn't re-trusted, instead an already trusted pem was used"
            assert new_ca_pem in original_trusted_ca_pems, \
                "CA regenerated when force_reset=false"
            assert_trusted_CAs_unchanged(self.cluster, trusted_cas_before)
        finally:
            regenerate_certs_and_remove_unused_generated_cas(self.cluster,
                                                             node_certs_before)

    def regen_and_drop_certs_with_untrusted_ca_test(self):
        node_certs_before = self.load_custom_certs()

        original_trusted_cas = get_trusted_CAs(self.cluster)
        original_trusted_ca_pems = [ca.get('pem')
                                    for ca in original_trusted_cas]
        original_trusted_generated_cas = [ca for ca in original_trusted_cas
                                          if ca.get('type') == 'generated']
        original_trusted_generated_ca_ids = [
            ca.get('id') for ca in original_trusted_generated_cas]

        try:
            # Remove the generated ca, to test re-trusting
            ootb_ca = original_trusted_generated_ca_ids[0]
            testlib.delete_succ(self.cluster,
                                f'/pools/default/trustedCAs/{ootb_ca}',
                                expected_code=204)

            # Get updated ca lists, for comparison
            trusted_cas_before = get_trusted_CAs(self.cluster)
            trusted_generated_ca_pems_before = \
                [ca.get('pem')
                 for ca in trusted_cas_before
                 if ca.get('type') == 'generated']

            new_ca_pem = regenerate_certs(self.cluster,
                                          force_reset_ca=False,
                                          drop_uploaded_certs=True)
            wait_all_nodes_updated(self.cluster, node_certs_before, new_ca_pem)

            assert new_ca_pem not in trusted_generated_ca_pems_before, \
                "CA not regenerated when untrusted"
            assert new_ca_pem in original_trusted_ca_pems, \
                "CA regenerated when force_reset=false "
            # CA should not be regenerated, but should become trusted again.
            # Explicitly check 'pem' instead of 'id', since the id could change
            # when re-trusting the ca.
            assert_trusted_CAs_unchanged(self.cluster, original_trusted_cas,
                                         'pem')
            assert_CA_newly_trusted(self.cluster, trusted_cas_before,
                                    new_ca_pem)
        finally:
            regenerate_certs_and_remove_unused_generated_cas(self.cluster,
                                                             node_certs_before)

    def regen_force_reset_certs_with_untrusted_ca_test(self):
        node_certs_before = self.load_custom_certs()

        original_trusted_cas = get_trusted_CAs(self.cluster)
        original_trusted_ca_pems = [ca.get('pem')
                                    for ca in original_trusted_cas]
        original_trusted_generated_cas = [ca for ca in original_trusted_cas
                                          if ca.get('type') == 'generated']
        original_trusted_generated_ca_ids = [
            ca.get('id') for ca in original_trusted_generated_cas]

        try:
            # Remove the generated ca, to test re-trusting
            ootb_ca = original_trusted_generated_ca_ids[0]
            testlib.delete_succ(self.cluster,
                                f'/pools/default/trustedCAs/{ootb_ca}',
                                expected_code=204)

            # Get updated ca lists, for comparison
            trusted_cas_before = get_trusted_CAs(self.cluster)
            trusted_generated_ca_pems_before = \
                [ca.get('pem')
                 for ca in trusted_cas_before
                 if ca.get('type') == 'generated']

            new_ca_pem = regenerate_certs(self.cluster,
                                          force_reset_ca=True,
                                          drop_uploaded_certs=False)
            assert_no_certs_changed(self.cluster, node_certs_before, new_ca_pem)

            # CA should be regenerated, and newly trusted
            assert new_ca_pem not in trusted_generated_ca_pems_before, \
                "CA not regenerated when force_reset=true"
            assert new_ca_pem not in original_trusted_ca_pems, \
                "CA re-trusted and not regenerated when force_reset=true"
            assert_CA_newly_trusted(self.cluster, original_trusted_cas,
                                    new_ca_pem)
            assert_CA_newly_trusted(self.cluster, trusted_cas_before,
                                    new_ca_pem)
        finally:
            regenerate_certs_and_remove_unused_generated_cas(self.cluster,
                                                             node_certs_before)

    def regen_force_reset_certs_and_drop_with_untrusted_ca_test(self):
        node_certs_before = self.load_custom_certs()

        original_trusted_cas = get_trusted_CAs(self.cluster)
        original_trusted_ca_pems = [ca.get('pem')
                                    for ca in original_trusted_cas]
        original_trusted_generated_cas = [ca for ca in original_trusted_cas
                                          if ca.get('type') == 'generated']
        original_trusted_generated_ca_ids = [
            ca.get('id') for ca in original_trusted_generated_cas]

        try:
            # Remove the last generated ca
            ootb_ca = original_trusted_generated_ca_ids[0]
            testlib.delete_succ(self.cluster,
                                f'/pools/default/trustedCAs/{ootb_ca}',
                                expected_code=204)

            trusted_cas_before = get_trusted_CAs(self.cluster)

            trusted_generated_ca_pems_before = [ca.get('pem')
                                      for ca in trusted_cas_before
                                      if ca.get('type') == 'generated']

            new_ca_pem = regenerate_certs(self.cluster,
                                          force_reset_ca=True,
                                          drop_uploaded_certs=True)
            wait_all_nodes_updated(self.cluster, node_certs_before, new_ca_pem)
            # CA should be regenerated, and newly trusted
            assert new_ca_pem not in trusted_generated_ca_pems_before, \
                "CA not regenerated when force_reset=true"
            assert new_ca_pem not in original_trusted_ca_pems, \
                "CA re-trusted and not regenerated when force_reset=true"
            assert_CA_newly_trusted(self.cluster, original_trusted_cas,
                                    new_ca_pem)
            assert_CA_newly_trusted(self.cluster, trusted_cas_before,
                                    new_ca_pem)
        finally:
            regenerate_certs_and_remove_unused_generated_cas(self.cluster,
                                                             node_certs_before)

    def load_custom_certs(self):
        node_certs = []
        for node in self.cluster.connected_nodes:
            node_cert = self.generate_and_load_cert('rsa', node=node,
                                                    is_client=False)
            client_cert = self.generate_and_load_cert('rsa', node=node,
                                                      is_client=True)
            node_certs.append((node, node_cert, client_cert))
        return node_certs


def wait_all_nodes_updated(cluster, node_certs_before, new_ca_pem):
    def all_nodes_updated():
        # Only check the node certs if we know the new ca
        # TODO: do this properly
        if new_ca_pem is not None and len(node_certs_before) > 0:
            trusted_cas = get_trusted_CAs(cluster)
            [new_ca_details] = [ca for ca in trusted_cas
                                if ca['pem'] == new_ca_pem]
            node_certs = new_ca_details['nodes']
            assert len(node_certs) == len(node_certs_before), \
                f"Not all node certs using new ca ({node_certs})"
            client_cert_nodes = new_ca_details['client_cert_nodes']
            assert len(client_cert_nodes) == len(node_certs_before), \
                f"Not all client certs using new ca ({client_cert_nodes})"
        # Confirm that node certs are regenerated
        for (node, node_cert, client_cert) in node_certs_before:
            assert_cert_regenerated(node, node_cert)
            assert_cert_regenerated(node, client_cert, is_client=True)
    testlib.poll_for_condition(all_nodes_updated, 1, timeout=60,
                               retry_on_assert=True)


def assert_no_certs_changed(cluster, node_certs_before, new_ca_pem):
    if new_ca_pem is not None:
        trusted_cas = get_trusted_CAs(cluster)
        [new_ca_details] = [ca for ca in trusted_cas
                            if ca['pem'] == new_ca_pem]
        node_certs = new_ca_details['nodes']
        assert len(node_certs) == 0, \
            f"Node certs unexpectedly using the new ca ({node_certs})"
        client_cert_nodes = new_ca_details['client_cert_nodes']
        assert len(client_cert_nodes) == 0, \
            f"Client certs unexpectedly using the new ca ({client_cert_nodes})"
    # Confirm that node certs are unchanged
    for (node, node_cert, client_cert) in node_certs_before:
        assert_cert_unchanged(node, node_cert)
        assert_cert_unchanged(node, client_cert, is_client=True)


# Regenerate certs and tidy up trusted certs by removing unused ones
def regenerate_certs_and_remove_unused_generated_cas(cluster,
                                                     node_certs_before=None):
    if node_certs_before is None:
        node_certs_before = get_node_certs(cluster)
    new_ca_pem = regenerate_certs(cluster)
    wait_all_nodes_updated(cluster, node_certs_before, new_ca_pem)
    for trusted_ca in get_trusted_CAs(cluster):
        if (trusted_ca['type'] == 'generated' and
                trusted_ca['pem'] != new_ca_pem):
            ca_id = trusted_ca['id']
            testlib.delete_succ(cluster,
                                f'/pools/default/trustedCAs/{ca_id}',
                                expected_code=204)


def assert_CA_newly_trusted(cluster, trusted_cas_before, expected_new_pem):
    trusted_cas_after = get_trusted_CAs(cluster)
    ca_ids_before = [ca['id'] for ca in trusted_cas_before]
    new_cas = [ca for ca in trusted_cas_after if ca['id'] not in ca_ids_before]
    assert len(new_cas) == 1, f'expected 1 new CA, got {len(new_cas)}'
    new_ca_props = new_cas[0]
    assert new_ca_props['pem'] == expected_new_pem, \
           'unexpected pem in new CA'


def assert_trusted_CAs_unchanged(cluster, trusted_cas_before, identifier='id'):
    trusted_cas_after = get_trusted_CAs(cluster)
    ids_before = [ca[identifier] for ca in trusted_cas_before]
    ids_after = [ca[identifier] for ca in trusted_cas_after]
    assert ids_before == ids_after, 'CAs changed'


def assert_cert_regenerated(node, prev_cert_before, is_client=False):
    node_cert_after = get_node_cert(node, is_client=is_client)
    assert node_cert_after['type'] == 'generated', \
           'node_cert_after type != generated'
    assert node_cert_after['pem'] != prev_cert_before['pem'], \
           'node_cert_after pem == node_cert_before pem'


def assert_cert_unchanged(node, prev_cert_before, is_client=False):
    node_cert_after = get_node_cert(node, is_client=is_client)
    assert node_cert_after['type'] != 'generated', \
           'node_cert_after type == generated'
    assert node_cert_after['pem'] == prev_cert_before['pem'], \
           'node_cert_after pem != node_cert_before pem'


def regenerate_certs(cluster, force_reset_ca=True, drop_uploaded_certs=True):
    params = {'forceResetCACertificate': 'true' if force_reset_ca else 'false',
              'dropUploadedCertificates': 'true' if drop_uploaded_certs
                                                 else 'false'}
    r = testlib.post_succ(cluster, '/controller/regenerateCertificate',
                          params=params)
    return r.text


def get_trusted_CAs(cluster):
    r = testlib.get_succ(cluster, '/pools/default/trustedCAs')
    return r.json()


def get_node_certs(cluster):
    node_certs = []
    for node in cluster.connected_nodes:
        node_cert = get_node_cert(node)
        client_cert = get_node_cert(node, is_client=True)
        node_certs.append((node, node_cert, client_cert))
    return node_certs


def get_node_cert(node, is_client=False):
    if is_client:
        r = testlib.get_succ(node, '/pools/default/certificates/client')
    else:
        r = testlib.get_succ(node, '/pools/default/certificates')
    hostname = node.hostname()
    res = r.json()
    for cert_props in res:
        if cert_props['node'] == hostname:
            return cert_props
    raise Exception(f'No node cert found for {hostname}: {res}')


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
        r = get_node_cert(node, is_client=is_client)
        assert r['type'] == 'uploaded', f'cert type {r} != uploaded'
        return r
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
