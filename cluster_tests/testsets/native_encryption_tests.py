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
import re
import random
import os
import sys
import subprocess
import base64
from pathlib import Path
from datetime import datetime, timedelta, timezone
import dateutil

from testlib.test_tag_decorator import tag, Tag
from testsets.secret_management_tests import change_password, post_es_config
from testlib.requirements import Service
import time
import textwrap
import uuid
from testsets.sample_buckets import SampleBucketTasksBase
from testsets.users_tests import put_user
import shutil
from contextlib import contextmanager
from contextlib import suppress
from multiprocessing import Process
from kmip.pie.client import ProxyKmipClient, enums
from kmip.services.server import KmipServer

encrypted_file_magic = b'\x00Couchbase Encrypted\x00'
min_timer_interval = 1 # seconds
dek_info_update_interval = 3 # seconds
min_dek_gc_interval = 2 # seconds
dek_counters_renew_interval = 1 # seconds
key_test_interval = 1 # seconds
default_max_deks = 50
default_dek_lifetime = 60*60*24*365
min_dek_lifetime = 60*60*24*30
default_dek_rotation = 60*60*24*30
min_dek_rotation = 60*60*24*7
dek_lifetime_margin = 5*60
kmip_port = 5696

class NativeEncryptionTests(testlib.BaseTestSet, SampleBucketTasksBase):

    def __init__(self, cluster):
        super().__init__(cluster)
        SampleBucketTasksBase.__init__(self)

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes = 3,
                                           edition='Enterprise',
                                           include_services={'n0': [Service.KV],
                                                     'n1': [Service.QUERY],
                                                     'n2': [Service.KV]},
                                           buckets=[],
                                           balanced=True,
                                           num_vbuckets=16)

    def setup(self):
        # Wait for orchestrator to move to non kv node.
        # Here we assume that there is only one non kv node:
        set_min_timer_interval(self.cluster, min_timer_interval)
        set_dek_info_update_interval(self.cluster, dek_info_update_interval)
        set_min_dek_gc_interval(self.cluster, min_dek_gc_interval)
        set_dek_counters_renew_interval(self.cluster,
                                        dek_counters_renew_interval)
        testlib.post_succ(
            self.cluster, '/settings/security',
            data={'encryptionKeysTestIntervalSeconds': key_test_interval})
        non_kv_node = None
        for n in self.cluster.connected_nodes:
            if Service.KV not in n.get_services():
                assert non_kv_node is None
                non_kv_node = n
        assert non_kv_node is not None
        self.cluster.wait_for_orchestrator(non_kv_node)

        # Bypass some stricter config validation restrictions to allow better
        # flexibility for testing
        set_ns_config_value(self.cluster, 'test_bypass_encr_cfg_restrictions',
                            'true')

        # since master password is per-node, we should use the same node for
        # all HTTP requests in all tests that use node secret management (SM)
        self.sm_node = random.choice(self.cluster.connected_nodes)
        self.bucket_name = testlib.random_str(8)
        self.bucket_name2 = testlib.random_str(8)
        set_cfg_encryption(self.cluster, 'nodeSecretManager', -1)
        self.sample_bucket = "beer-sample"
        # Creating a few keys whose role is to just exist while other tests
        # are running. It increases code coverage.
        id1 = create_secret(self.random_node(), aws_test_secret())
        id2 = create_secret(self.random_node(),
                            cb_managed_secret(encrypt_with='encryptionKey',
                                              encrypt_secret_id=id1))
        self.pre_created_ids = [id2, id1] # so we can remove them later
        # Memorize all existing ids so we don't remove them in test_teardown
        self.pre_existing_ids = [s['id'] for s in get_secrets(self.cluster)]

    def teardown(self):
        new_existing_ids = [s['id'] for s in get_secrets(self.cluster)]
        for s_id in self.pre_existing_ids:
            assert s_id in new_existing_ids, \
                   f'Secret {s_id} disappeared during tests'
        for s_id in self.pre_created_ids:
            delete_secret(self.cluster, s_id)
        testlib.delete(self.cluster,
                       f'/settings/security/encryptionKeysTestIntervalSeconds')
        set_dek_counters_renew_interval(self.cluster, None)
        set_min_dek_gc_interval(self.cluster, None)
        set_dek_info_update_interval(self.cluster, None)
        set_min_timer_interval(self.cluster, None)
        # In teardown we disable encryption for logs, here we drop deks
        # to ensure that absolutely all deks are unencrypted
        drop_deks(self.cluster, 'log')
        testlib.poll_for_condition(
            lambda: assert_logs_unencrypted(self.cluster, ["debug.log"],
                                            check_suffixes=['*']),
            sleep_time=1, attempts=50, retry_on_assert=True, verbose=True)
        set_ns_config_value(self.cluster, 'test_bypass_encr_cfg_restrictions',
                            None)

    def test_teardown(self):
        for n in self.cluster.connected_nodes:
            delete_aws_fake_creds_file(n)
        set_cfg_encryption(self.cluster, 'nodeSecretManager', -1,
                           dek_lifetime=default_dek_lifetime,
                           dek_rotation=default_dek_rotation)
        set_audit_encryption(self.cluster, 'disabled', -1,
                             dek_lifetime=default_dek_lifetime,
                             dek_rotation=default_dek_rotation)
        r = testlib.get_succ(self.cluster,
                             '/settings/security/encryptionAtRest/log')
        r = r.json()
        if r['encryptionMethod'] == 'encryptionKey':
            # Setting it to nodeSecretManager first, so all log deks get
            # reencrypted with the master password (to unlock the secrets
            # removal below)
            set_log_encryption(self.cluster, 'nodeSecretManager', -1,
                               dek_lifetime=default_dek_lifetime,
                               dek_rotation=default_dek_rotation)
        set_log_encryption(self.cluster, 'disabled', -1,
                           dek_lifetime=default_dek_lifetime,
                           dek_rotation=default_dek_rotation)
        set_bucket_dek_limit(self.cluster, self.bucket_name, None)
        self.cluster.delete_bucket(self.bucket_name)
        self.cluster.delete_bucket(self.bucket_name2)
        self.cluster.delete_bucket(self.sample_bucket)
        for s in get_secrets(self.cluster):
            if s['id'] not in self.pre_existing_ids:
                delete_secret(self.cluster, s['id'])
        post_es_config(self.sm_node, {'keyStorageType': 'file',
                                      'keyEncrypted': 'true',
                                      'passwordSource': 'env'})
        change_password(self.sm_node, password='')
        set_cfg_dek_limit(self.cluster, None)
        set_log_dek_limit(self.cluster, None)
        set_all_bucket_dek_limit(self.cluster, None)
        set_remove_hist_keys_interval(self.cluster, None)

    def random_node(self):
        return random.choice(self.cluster.connected_nodes)


    def basic_create_update_delete_test(self):
        data = testlib.random_str(8)
        start_len = len(get_secrets(self.random_node()))

        # Create new secret
        secret_json = cb_managed_secret(name='Test Secret 1')
        secret_id = create_secret(self.random_node(), secret_json)
        assert_secret_used_by(self.random_node(), secret_id, []) # not used yet

        # Encrypt data using that secret
        encrypted = encrypt_with_key(self.random_node(),
                                     get_kek_id(self.random_node(), secret_id),
                                     data)

        # Checking secrets GET
        secret = get_secret(self.random_node(), secret_id)
        secret_list = get_secrets(self.random_node())
        assert len(secret_list) == start_len + 1, \
               f'unexpected length of secrets ({secret_list})'
        assert secret['name'] == 'Test Secret 1', 'unexpected name'
        assert secret['type'] == 'cb-server-managed-aes-key-256', \
               'unexpected type'
        verify_kek_files(self.cluster, secret)

        # Trying to update the secret that doesn't exist
        update_secret(self.random_node(), secret_id+1, secret_json,
                      expected_code=404)

        # Trying to change type which is not allowed
        secret_json['type'] = 'awskms-symmetric-key'
        errors = update_secret(self.random_node(), secret_id, secret_json,
                               expected_code=400)
        assert errors['type'] == 'The field can\'t be changed', \
               f'unexpected error: {errors}'

        # Changing secret's name
        secret_json['name'] = 'Test Secret 2'
        secret_json['type'] = 'cb-server-managed-aes-key-256'
        update_secret(self.random_node(), secret_id, secret_json)
        secret = get_secret(self.random_node(), secret_id)
        assert secret['name'] == 'Test Secret 2', 'unexpected name'
        verify_kek_files(self.cluster, secret)
        check_decrypt(self.random_node(),
                      get_kek_id(self.random_node(), secret_id),
                      encrypted, data)

        # Deleting secret
        delete_secret(self.random_node(), secret_id)
        verify_kek_files(self.cluster, secret, verify_missing=True)

    @tag(Tag.LowUrgency)
    def bucket_with_encryption_test(self):
        secret1_json = cb_managed_secret(name='Test Secret 1')
        secret2_json = cb_managed_secret(name='Test Secret 2')

        secret1_id = create_secret(self.random_node(), secret1_json)
        assert_secret_used_by(self.random_node(), secret1_id, []) # not used yet

        # Can't create because the secret does not exist
        bucket_props = {'name': self.bucket_name,
                        'ramQuota': 100,
                        'encryptionAtRestKeyId': secret1_id + 1}
        resp = self.cluster.create_bucket(bucket_props, expected_code=400)
        errors = resp.json()
        e = errors['errors']['encryptionAtRestKeyId']
        assert e == 'Encryption key does not exist', \
               f'unexpected error: {errors}'

        bucket_props['encryptionAtRestKeyId'] = secret1_id
        self.cluster.create_bucket(bucket_props, sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        assert_secret_used_by(self.random_node(), secret1_id,
                              [used_by_bucket(self.bucket_name)])

        kek1_id = get_kek_id(self.random_node(), secret1_id)
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=kek1_id)

        poll_verify_cluster_bucket_dek_info(self.cluster, self.bucket_name,
                                            data_statuses=['encrypted'],
                                            dek_number=1)

        # Can't delete because it is in use
        delete_secret(self.random_node(), secret1_id, expected_code=400)

        # Can't modify because the secret doesn't exist
        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestKeyId': secret1_id + 1},
                                   expected_code=400)

        secret2_id = create_secret(self.random_node(), secret2_json)
        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestKeyId': secret2_id})
        poll_assert_secret_used_by(self.random_node(), secret1_id, [])
        assert_secret_used_by(self.random_node(), secret2_id,
                              [used_by_bucket(self.bucket_name)])

        kek2_id = get_kek_id(self.random_node(), secret2_id)
        # update is asynchronous, so we can't assume the dek gets reencrypted
        # immediately
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=kek2_id)

        # Now it can be deleted
        delete_secret(self.random_node(), secret1_id)
        self.cluster.delete_bucket(self.bucket_name)
        poll_assert_secret_used_by(self.random_node(), secret2_id, [])
        delete_secret(self.random_node(), secret2_id)

    def bucket_without_encryption_test(self):
        secret1_json = cb_managed_secret(name='Test Secret 1')
        secret1_id = create_secret(self.random_node(), secret1_json)
        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': -1},
                                   sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=0)
        # Can't modify because the secret doesn't exist
        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestKeyId': secret1_id + 1},
                                   expected_code=400)
        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestKeyId': secret1_id})
        kek1_id = get_kek_id(self.random_node(), secret1_id)
        # update is asynchronous, so we can't assume the dek gets reencrypted
        # immediately
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=kek1_id)
        poll_verify_cluster_bucket_dek_info(self.cluster, self.bucket_name,
                                            data_statuses=['encrypted',
                                                           'partiallyEncrypted'],
                                            dek_number=1)
        # Can't delete because it is in use
        delete_secret(self.random_node(), secret1_id, expected_code=400)
        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestKeyId': -1})

    def delete_historical_keys_test(self):
        secret_json = cb_managed_secret(name='Test Secret 1',
                                        usage=['bucket-encryption',
                                               'config-encryption',
                                               'log-encryption'])
        secret_id = create_secret(self.random_node(), secret_json)

        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': secret_id})

        set_cfg_encryption(self.cluster, 'encryptionKey', secret_id)
        set_log_encryption(self.cluster, 'encryptionKey', secret_id)
        assert_secret_used_by(self.random_node(), secret_id,
                              [used_by_log(), used_by_cfg(),
                               used_by_bucket(self.bucket_name)])

        hist_keys = get_historical_keys(self.cluster, secret_id)
        assert len(hist_keys) == 1
        hist_key_id0 = hist_keys[0]['id']

        errors = delete_historical_key(self.random_node(), secret_id,
                                       hist_key_id0, expected_code=400)
        assert errors['_'] == 'Operation is unsafe. Can\'t be removed ' \
                              'because this key still encrypts some data in ' \
                              'configuration, logs, ' \
                              f'bucket "{self.bucket_name}"', \
               f'unexpected error: {errors}'

        rotate_secret(self.random_node(), secret_id)

        hist_keys = get_historical_keys(self.random_node(), secret_id)
        assert len(hist_keys) == 2
        assert hist_keys[1]['id'] == hist_key_id0
        hist_key_id1 = hist_keys[0]['id']

        # It doesn't have to become removable immediately, it may take some
        # time to reencrypt DEKs
        testlib.poll_for_condition(
            lambda: delete_historical_key(self.random_node(), secret_id,
                                          hist_key_id0),
            sleep_time=1, attempts=50, retry_on_assert=True, verbose=True)
        delete_historical_key(self.random_node(), secret_id, hist_key_id0,
                              expected_code=404)

        hist_keys = get_historical_keys(self.random_node(), secret_id)
        assert len(hist_keys) == 1
        assert hist_keys[0]['id'] == hist_key_id1

        self.cluster.delete_bucket(self.bucket_name)
        set_cfg_encryption(self.random_node(), 'nodeSecretManager', -1)
        set_log_encryption(self.random_node(), 'nodeSecretManager', -1)

        errors = delete_historical_key(self.random_node(), secret_id,
                                       hist_key_id1, expected_code=400)
        assert errors['_'] == 'Operation is unsafe. Can\'t delete active key', \
               f'unexpected error: {errors}'


    def secret_not_allowed_to_encrypt_bucket_test(self):
        # Creating buckets so we can use 'bucket-encryption-<bucket_name>'
        # in usage field of the secret
        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100})
        self.cluster.create_bucket({'name': self.bucket_name2, 'ramQuota': 100})

        secret1_json = cb_managed_secret(
                         usage=[f'bucket-encryption-{self.bucket_name}',
                                f'bucket-encryption-{self.bucket_name2}'])
        secret1_id = create_secret(self.random_node(), secret1_json)

        self.cluster.delete_bucket(self.bucket_name)

        # Re-creating bucket again and try using the secret, it should fail
        # because the was supposed to encrypt previous incarnation of the bucket
        # (bucket's uuid has changed)
        bucket_props = {'name': self.bucket_name,
                        'ramQuota': 100,
                        'encryptionAtRestKeyId': secret1_id}

        # Trying to use a secret that is not allowed to encrypt this bucket
        resp = self.cluster.create_bucket(bucket_props, expected_code=400)
        errors = resp.json()
        e = errors['errors']['encryptionAtRestKeyId']
        assert e == 'Encryption key can\'t encrypt this bucket', \
               f'unexpected error: {errors}'

        secret1_json['usage']= ['bucket-encryption',
                                f'bucket-encryption-{self.bucket_name2}']
        update_secret(self.random_node(), secret1_id, secret1_json)

        # Now the secret should work fine for encryption
        self.cluster.create_bucket(bucket_props)

        secret2_json = cb_managed_secret(
                         usage=[f'bucket-encryption-{self.bucket_name2}'])
        secret2_id = create_secret(self.random_node(), secret2_json)

        # Trying to change encryption secret to the one that can't encrypt
        # this bucket
        bucket_props['encryptionAtRestKeyId'] = secret2_id
        resp = self.cluster.update_bucket(bucket_props, expected_code=400)
        errors = resp.json()
        e = errors['errors']['encryptionAtRestKeyId']
        assert e == 'Encryption key can\'t encrypt this bucket', \
               f'unexpected error: {errors}'

        # Trying to forbid using this secret for our bucket encryption
        secret1_json['usage'] = [f'bucket-encryption-{self.bucket_name2}']
        errors = update_secret(self.random_node(), secret1_id, secret1_json,
                               expected_code=400)
        assert errors['_'] == 'Can\'t modify usage as this key is in use', \
               f'unexpected error: {errors}'

        # Trying again, but now we add permission to encrypt this bucket
        secret1_json['usage'].append(f'bucket-encryption-{self.bucket_name}')
        update_secret(self.random_node(), secret1_id, secret1_json)


    def change_SM_password_test(self):
        data = testlib.random_str(8)
        secret_id = create_secret(self.sm_node, cb_managed_secret())
        encrypted = encrypt_with_key(self.sm_node,
                                     get_kek_id(self.sm_node, secret_id),
                                     data)
        change_password(self.sm_node)
        check_decrypt(self.sm_node,
                      get_kek_id(self.sm_node, secret_id), encrypted, data)

    def SM_data_key_rotation_test(self):
        data = testlib.random_str(8)
        secret_id = create_secret(self.sm_node, cb_managed_secret())
        kek_id = get_kek_id(self.sm_node, secret_id)
        encrypted = encrypt_with_key(self.sm_node, kek_id, data)
        testlib.post_succ(self.sm_node, '/node/controller/rotateDataKey')
        check_decrypt(self.sm_node, kek_id, encrypted, data)
        # Run another rotation in order to make sure that the backup key is
        # removed and try decryption again
        testlib.poll_for_condition(
            lambda: testlib.post_succ(self.sm_node,
                                      '/node/controller/rotateDataKey'),
            sleep_time=0.3, attempts=50, retry_on_assert=True, verbose=True)
        check_decrypt(self.sm_node, kek_id, encrypted, data)

        # Testing that config deks are also reencrypted
        # Cache reset should fail if it can't read keys from disk
        testlib.diag_eval(
          self.sm_node,
          '{ok, changed} = cb_crypto:reset_dek_cache(configDek).')

    def import_ear_dek_negative_test(self):
        kv_node = kv_nodes(self.cluster)[0]
        # Invalid type returns 400
        r = testlib.post_fail(
            kv_node, '/_cbauth/importEaRDEK', 400,
            json={'type': 'invalid', 'dekPaths': ['/some/path']})
        assert r.json()['errors']['type'] == 'invalid type: "invalid"', \
               f'unexpected error: {r.json()}'
        # Missing dekPaths returns 400
        r = testlib.post_fail(
            kv_node, '/_cbauth/importEaRDEK', 400,
            json={'type': 'bucket'})
        assert r.json()['errors']['dekPaths'] == 'The value must be supplied', \
               f'unexpected error: {r.json()}'
        # Empty dekPaths returns 400 (must contain at least one element)
        r = testlib.post_fail(
            kv_node, '/_cbauth/importEaRDEK', 400,
            json={'type': 'bucket', 'dekPaths': []})
        assert r.json()['errors']['dekPaths'] == \
               'Must contain at least one element', \
               f'unexpected error: {r.json()}'
        # Valid body but nonexistent file returns 400 (global error in "_")
        # Need a real bucket UUID, otherwise the server returns 500
        self.cluster.create_bucket(
            {'name': self.bucket_name, 'ramQuota': 100}, sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        r = testlib.post_fail(
            kv_node, '/_cbauth/importEaRDEK', 400,
            json={'type': 'bucket', 'bucketUUID': bucket_uuid,
                  'dekPaths': ['/nonexistent/dek.key.test-id']})
        msg = r.json()['errors']['_']
        assert 'Failed to read DEK file' in msg, \
               f'unexpected error: {r.json()}'
        assert 'file /nonexistent/dek.key.test-id does not exist' in msg, \
               f'unexpected error: {r.json()}'
        # Import on non-KV node returns 400 (bucket not found on that node)
        non_kv_node = next(filter(lambda n: Service.KV not in n.get_services(),
                                  self.cluster.connected_nodes))
        r = testlib.post_fail(
            non_kv_node, '/_cbauth/importEaRDEK', 400,
            json={'type': 'bucket', 'bucketUUID': bucket_uuid,
                  'dekPaths': ['/nonexistent/dek.key.test-id']})
        expected = 'Key type does not exist on this node'
        assert r.json()['errors']['_'] == expected, (
            f'unexpected error: {r.json()}')
        self.cluster.delete_bucket(self.bucket_name)

    def import_dek_for_buckets_test(self):
        secret_id = create_secret(self.random_node(), cb_managed_secret())
        self.cluster.create_bucket(
            {'name': self.bucket_name, 'ramQuota': 100,
             'encryptionAtRestKeyId': secret_id},
            sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        bucket_deks_rel = Path() / 'data' / bucket_uuid / 'deks'
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=lambda n: n >= 1)
        nodes = kv_nodes(self.cluster)
        assert len(nodes) >= 2, 'need at least 2 KV nodes'
        for source, target in [(nodes[0], nodes[1]), (nodes[1], nodes[0])]:
            paths = copy_dek_files_to_node(source, target, bucket_deks_rel)
            assert len(paths) >= 1, f'no DEK files on {source} to import'
            post_import_ear_dek(target, 'bucket', paths,
                                bucket_uuid=bucket_uuid)

    def change_SM_config_test(self):
        data = testlib.random_str(8)
        # Creating a secret that will be encrypted by node SM
        secret1_id = create_secret(self.sm_node, cb_managed_secret())
        encrypted = encrypt_with_key(self.sm_node,
                                     get_kek_id(self.sm_node, secret1_id),
                                     data)

        # Changing configuration of node Secret Manager (it doesn't matter what
        # is changed here, the point is to initiate gosecret config reload)
        post_es_config(self.sm_node, {'keyStorageType': 'file',
                                      'keyEncrypted': 'false'})

        # Check that we can still use already existing secret
        check_decrypt(self.sm_node,
                      get_kek_id(self.sm_node, secret1_id), encrypted, data)

        # Check that we can create new secrets after SM config change
        # and keks are actually saved on disk
        secret2_id = create_secret(self.sm_node, cb_managed_secret())
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret2_id),
                         verify_encryption_kek='encryptionService')

        # Make sure we can create encrypted bucket, and its deks are actually
        # saved on disk
        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': secret2_id},
                                   sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)

        kek_id = get_kek_id(self.random_node(), secret2_id)
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=kek_id)

    def keks_encrypted_by_keks_test(self):
        data = testlib.random_str(8)
        # Creating the following hierarchy:
        #               key1
        #               /  \
        #            key2  key3
        #             |     |
        #            key4  key5
        secret1_id = create_secret(
                       self.random_node(),
                       cb_managed_secret(name='Root (key1)'))
        secret2_id = create_secret(
                       self.random_node(),
                       cb_managed_secret(name='Level 2 (key2)',
                                         encrypt_with='encryptionKey',
                                         encrypt_secret_id=secret1_id))
        secret3_id = create_secret(
                       self.random_node(),
                       cb_managed_secret(name='Level 2 (key3)',
                                         encrypt_with='encryptionKey',
                                         encrypt_secret_id=secret1_id))
        secret4_id = create_secret(
                       self.random_node(),
                       cb_managed_secret(name='Level 3 (key4)',
                                         encrypt_with='encryptionKey',
                                         encrypt_secret_id=secret2_id))
        secret5_id = create_secret(
                       self.random_node(),
                       cb_managed_secret(name='Level 3 (key5)',
                                         encrypt_with='encryptionKey',
                                         encrypt_secret_id=secret3_id))
        assert_secret_used_by(self.random_node(), secret1_id,
                              [used_by_key('Level 2 (key2)'),
                               used_by_key('Level 2 (key3)')])
        assert_secret_used_by(self.random_node(), secret2_id,
                              [used_by_key('Level 3 (key4)')])
        assert_secret_used_by(self.random_node(), secret3_id,
                              [used_by_key('Level 3 (key5)')])
        assert_secret_used_by(self.random_node(), secret4_id, [])
        assert_secret_used_by(self.random_node(), secret5_id, [])

        # Can't create secret because encryption key with such id doesn't exist
        create_secret(self.random_node(),
                      cb_managed_secret(name='key6',
                                        encrypt_with='encryptionKey',
                                        encrypt_secret_id=secret5_id + 1),
                      expected_code=400)

        # Testing that keys on disk are encrypted by correct keks

        # Root:
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret1_id),
                         verify_encryption_kek='encryptionService')
        # Level 2:
        kek1_id = get_kek_id(self.random_node(), secret1_id)
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret2_id),
                         verify_encryption_kek=kek1_id)
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret3_id),
                         verify_encryption_kek=kek1_id)

        # Level 3:
        kek2_id = get_kek_id(self.random_node(), secret2_id)
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret4_id),
                         verify_encryption_kek=kek2_id)

        kek3_id = get_kek_id(self.random_node(), secret3_id)
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret5_id),
                         verify_encryption_kek=kek3_id)

        # Testing that we can use keys from lowest level for encryption and
        # decryption.
        encrypted = encrypt_with_key(self.random_node(),
                                     get_kek_id(self.random_node(), secret4_id),
                                     data)
        check_decrypt(self.random_node(),
                      get_kek_id(self.random_node(), secret4_id),
                      encrypted, data)
        encrypted = encrypt_with_key(self.random_node(),
                                     get_kek_id(self.random_node(), secret5_id),
                                     data)
        check_decrypt(self.random_node(),
                      get_kek_id(self.random_node(), secret5_id),
                      encrypted, data)

    def kek_not_allowed_to_encrypt_kek_test(self):
        good_secret_id = create_secret(
                           self.random_node(),
                           cb_managed_secret(name='Good Secret',
                                             usage=['bucket-encryption',
                                                    'KEK-encryption']))
        bad_secret_id = create_secret(
                          self.random_node(),
                          cb_managed_secret(name='Bad Secret',
                                            usage=['bucket-encryption']))

        secret = cb_managed_secret(name='Lever 2 (key1)',
                                   encrypt_with='encryptionKey',
                                   encrypt_secret_id=bad_secret_id)

        errors = create_secret(self.random_node(), secret, expected_code=400)
        assert errors['_'] == 'Encryption key not allowed', \
               f'unexpected error: {errors}'

        secret['data']['encryptWithKeyId'] = good_secret_id
        secret_id = create_secret(self.random_node(), secret)

        secret['data']['encryptWithKeyId'] = bad_secret_id
        errors = update_secret(self.random_node(), secret_id, secret,
                               expected_code=400)
        assert errors['_'] == 'Encryption key not allowed', \
               f'unexpected error: {errors}'

    def change_encrypt_id_for_kek_test(self):
        secret1_id = create_secret(self.random_node(),
                                   cb_managed_secret(name='Root 1'))
        secret2_id = create_secret(self.random_node(),
                                   cb_managed_secret(name='Root 2'))
        secret3 = cb_managed_secret(name='Lever 2 (key1)',
                                    encrypt_with='encryptionKey',
                                    encrypt_secret_id=secret1_id)
        secret3_id = create_secret(self.random_node(), secret3)

        kek1_id = get_kek_id(self.random_node(), secret1_id)
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret3_id),
                         verify_encryption_kek=kek1_id)

        # Try encrypting secret3 with another secret
        secret3['data']['encryptWithKeyId'] = secret2_id
        update_secret(self.random_node(), secret3_id, secret3)
        kek2_id = get_kek_id(self.random_node(), secret2_id)
        poll_verify_kek_files(self.cluster,
                              get_secret(self.random_node(), secret3_id),
                              verify_encryption_kek=kek2_id)

        # Try encrypt secret3 with node secret manager
        del secret3['data']['encryptWithKeyId']
        secret3['data']['encryptWith'] = 'nodeSecretManager'
        update_secret(self.random_node(), secret3_id, secret3)
        poll_verify_kek_files(self.cluster,
                              get_secret(self.random_node(), secret3_id),
                              verify_encryption_kek='encryptionService')

        # Try encrypting secret3 with secret1 again
        secret3['data']['encryptWithKeyId'] = secret1_id
        secret3['data']['encryptWith'] = 'encryptionKey'
        update_secret(self.random_node(), secret3_id, secret3)
        kek1_id = get_kek_id(self.random_node(), secret1_id)
        poll_verify_kek_files(self.cluster,
                              get_secret(self.random_node(), secret3_id),
                              verify_encryption_kek=kek1_id)

        # Try encrypting secret with itself (must fail)
        secret3['data']['encryptWithKeyId'] = secret3_id
        update_secret(self.random_node(), secret3_id, secret3,
                      expected_code=400)

    def change_secret_usage_test(self):
        secret1 = cb_managed_secret(usage=['KEK-encryption'])
        secret1_id = create_secret(self.random_node(), secret1)

        secret2 = cb_managed_secret(encrypt_with='encryptionKey',
                                    encrypt_secret_id=secret1_id)
        secret2_id = create_secret(self.random_node(), secret2)

        # Can't remove 'KEK-encryption' usage because this secret is
        # currently encrypting another secret
        secret1['usage'] = ['bucket-encryption']
        errors = update_secret(self.random_node(), secret1_id, secret1,
                               expected_code=400)
        assert errors['_'] == 'Can\'t modify usage as this key is in use', \
               f'unexpected error: {errors}'

        # Stop using secret1 for encryption
        del secret2['data']['encryptWithKeyId']
        secret2['data']['encryptWith'] = 'nodeSecretManager'
        update_secret(self.random_node(), secret2_id, secret2)

        # Now this secret doesn't encrypt anything and usage can be changed
        update_secret(self.random_node(), secret1_id, secret1)

    def rotate_kek_that_encrypts_kek_test(self):
        secret1_id = create_secret(
                       self.random_node(),
                       cb_managed_secret(name='Root'))
        secret2_id = create_secret(
                       self.random_node(),
                       cb_managed_secret(name='Lever 2 (key1)',
                                         encrypt_with='encryptionKey',
                                         encrypt_secret_id=secret1_id))
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret1_id),
                         verify_encryption_kek='encryptionService')

        old_kek_id = get_kek_id(self.random_node(), secret1_id)
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret2_id),
                         verify_encryption_kek=old_kek_id)

        rotate_secret(self.random_node(), secret1_id)

        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), secret1_id),
                         verify_encryption_kek='encryptionService',
                         verify_key_count=2)

        # the key should change after rotation
        new_kek_id = get_kek_id(self.random_node(), secret1_id)
        assert new_kek_id != old_kek_id, 'kek id hasn\'t changed'

        # all the keys that are encrypted by the key that has been rotated
        # should be reencrypted
        poll_verify_kek_files(self.cluster,
                              get_secret(self.random_node(), secret2_id),
                              verify_encryption_kek=new_kek_id)

    def rotate_kek_that_encrypts_bucket_dek_test(self):
        secret1_id = create_secret(
                       self.random_node(),
                       cb_managed_secret(name='Root'))

        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': secret1_id},
                                   sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)

        old_kek_id = get_kek_id(self.random_node(), secret1_id)
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=old_kek_id)

        rotate_secret(self.random_node(), secret1_id)

        # the key should change after rotation
        new_kek_id = get_kek_id(self.random_node(), secret1_id)
        assert new_kek_id != old_kek_id, 'kek id hasn\'t changed'

        # all the keys that are encrypted by the key that has been rotated
        # should be reencrypted
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=new_kek_id)


    def auto_rotate_kek_test(self):
        moment = timedelta(seconds=2)
        def now(offset):
            tz = timezone(timedelta(hours=offset))
            return datetime.now(tz=tz).replace(microsecond=0)
        bad_next_rotation = (now(7) - moment).isoformat()
        secret = cb_managed_secret(auto_rotation=True,
                                   next_rotation_time=bad_next_rotation)
        errors = create_secret(self.random_node(), secret, expected_code=400)
        assert errors['data']['nextRotationTime'] == 'must be in the future', \
               f'unexpected error: {errors}'

        rotation_days = 3
        next_rotation = (now(-7) + moment).isoformat()
        secret = cb_managed_secret(auto_rotation=True,
                                   rotation_interval=rotation_days,
                                   next_rotation_time=next_rotation)
        secret_id = create_secret(self.random_node(), secret)

        def rotation_happened(key_num, expected_rotation_time_iso):
            s = get_secret(self.random_node(), secret_id)
            if len(s['data']['keys']) < key_num:
                return False
            assert 'lastRotationTime' in s['data'], f'no lastRotationTime: {s}'
            rotation_time_iso = s['data']['lastRotationTime']
            rotation_time = parse_iso8601(rotation_time_iso)
            expected_time = parse_iso8601(expected_rotation_time_iso)
            assert rotation_time >= expected_time, \
                   f'rotation happened too early'
            # rotation can happen later ns_server does not allow timers
            # to fire more often than once per min_timer_interval
            delta = (rotation_time - expected_time).seconds
            assert delta <= min_timer_interval + 10, \
                   f'rotation happend too late, delta: {delta} seconds'
            next_rotation_time_iso = s['data']['nextRotationTime']
            next_rotation_time = parse_iso8601(next_rotation_time_iso)
            expected_next_rotation_time = expected_time + \
                                          timedelta(days=rotation_days)
            assert next_rotation_time == expected_next_rotation_time, \
                   f'bad next rotation time, got: {next_rotation_time}, ' \
                   f'expected: {expected_next_rotation_time}'
            return True

        testlib.poll_for_condition(
            lambda: rotation_happened(2, next_rotation),
            sleep_time=1, timeout=min_timer_interval + 30)

        # Trying to update secret with new rotation date and check that rotation
        # happens again. Timezone is random. It should not change anything.
        next_rotation = (now(11) + moment).isoformat()
        secret['data']['nextRotationTime'] = next_rotation
        update_secret(self.random_node(), secret_id, secret)

        # Making sure update doesn't overwrite lastRotationTime
        s = get_secret(self.random_node(), secret_id)
        assert 'lastRotationTime' in s['data'], f'no lastRotationTime: {s}'

        testlib.poll_for_condition(
            lambda: rotation_happened(3, next_rotation),
            sleep_time=0.3, timeout=30)

    def cfg_encryption_api_test(self):
        secret = cb_managed_secret(usage=['bucket-encryption'])
        bad_id = create_secret(self.random_node(), secret)
        secret['usage'] = ['bucket-encryption', 'config-encryption']
        secret['name'] = secret['name'] + ' (good)' # has to be unique
        good_id = create_secret(self.random_node(), secret)
        node = self.random_node()
        # There is a secret that is encrypted by master password; if we disable
        # config encryption that secret we be stored unencrypted in chronicle
        set_cfg_encryption(node, 'disabled', -1, expected_code=400)
        set_cfg_encryption(node, 'nodeSecretManager', -1)
        set_cfg_encryption(node, 'encryptionKey', -1, expected_code=400)
        set_cfg_encryption(node, 'encryptionKey', bad_id, expected_code=400)
        set_cfg_encryption(node, 'encryptionKey', good_id)

        assert_secret_used_by(node, good_id, [used_by_cfg()])

        secret['usage'] = ['bucket-encryption']
        errors = update_secret(node, good_id, secret, expected_code=400)
        assert errors['_'] == 'Can\'t modify usage as this key is in use', \
               f'unexpected error: {errors}'

        set_cfg_encryption(node, 'nodeSecretManager', -1)
        poll_assert_secret_used_by(node, good_id, [])

        update_secret(node, good_id, secret)

    def dont_store_secret_in_unencrypted_chronicle_test(self):
        node = self.random_node()
        set_cfg_encryption(node, 'disabled', -1)

        secret = cb_managed_secret()
        # Can't create secret because it will be stored unencrypted in
        # chronicle then
        create_secret(node, secret, expected_code=400)

        # Enabled config encryption and try again. Now creation works:
        set_cfg_encryption(node, 'nodeSecretManager', -1)
        secret_id = create_secret(node, secret)

        # ... and can't disable config encryption anymore:
        set_cfg_encryption(node, 'disabled', -1, expected_code=400)

        # Now try encrypt that secret by another secret. It should become
        # posible to store it in unencrypted chronicle then
        aws_secret = aws_test_secret(name='AWS Key',
                                     usage=['KEK-encryption'])
        aws_secret_id = create_secret(node, aws_secret)
        secret['data']['encryptWith'] = 'encryptionKey'
        secret['data']['encryptWithKeyId'] = aws_secret_id
        update_secret(node, secret_id, secret)

        # Now we can disable config encryption:
        set_cfg_encryption(node, 'disabled', -1)

        # ... but can't update the secret to use "master password" again
        secret['data']['encryptWith'] = 'nodeSecretManager'
        secret['data']['encryptWithKeyId'] = -1
        update_secret(node, secret_id, secret, expected_code=400)

        # Switching secret back to nodeSecretManager so it can be removed
        # in teardown
        set_cfg_encryption(node, 'nodeSecretManager', -1)
        update_secret(node, secret_id, secret)

    def dump_keys_utilities_test(self):
        secret = cb_managed_secret(usage=['bucket-encryption',
                                          'config-encryption'])
        node = random.choice(kv_nodes(self.cluster))
        secret_id = create_secret(node, secret)
        n = 5
        for i in range(n):
            rotate_secret(node, secret_id)

        kek_ids = get_all_kek_ids(node, secret_id)
        testlib.assert_eq(len(kek_ids), n + 1)

        bucket_props = {'name': self.bucket_name,
                        'ramQuota': 100,
                        'encryptionAtRestKeyId': secret_id}
        self.cluster.create_bucket(bucket_props, sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        bucket_dek_ids = \
            get_key_list(node, '{bucketDek, <<"' + bucket_uuid + '">>}')
        assert len(bucket_dek_ids) > 0, \
                f'no deks found for bucket {self.bucket_name}'
        print(bucket_dek_ids)

        unknown_id = 'unknown'

        all_ids = [unknown_id] + kek_ids + bucket_dek_ids

        ## Test dump-keys:
        res = run_dump_keys(node, ['--key-kind', 'kek', \
                                   '--key-ids'] + all_ids)
        verify_key_presense_in_dump_key_response(res, kek_ids,
                                                 [unknown_id] + bucket_dek_ids)

        keys_path = Path(node.data_path()) / 'config' / 'keks'
        res = run_dump_keys(node, ['--key-dir', keys_path,
                                   '--key-ids'] + all_ids)
        verify_key_presense_in_dump_key_response(res, kek_ids,
                                                 [unknown_id] + bucket_dek_ids)

        res = run_dump_keys(node, ['--key-ids'] + all_ids)
        verify_key_presense_in_dump_key_response(res, kek_ids + bucket_dek_ids,
                                                 [unknown_id])

        res = run_dump_keys(node, ['--key-kind', 'bucketDek',
                                   '--key-ids'] + all_ids)
        verify_key_presense_in_dump_key_response(res, bucket_dek_ids,
                                                 [unknown_id] + kek_ids)

        keys_path = Path(node.data_path()) / 'data' / bucket_uuid / 'deks'
        res = run_dump_keys(node, ['--key-dir', keys_path,
                                   '--key-ids'] + all_ids)
        verify_key_presense_in_dump_key_response(res, bucket_dek_ids,
                                                 [unknown_id] + kek_ids)

        ## Test dump-bucket-deks:
        res = run_dump_bucket_deks(node, ['--bucket-uuid', bucket_uuid,
                                          '--key-ids'] + all_ids)
        verify_key_presense_in_dump_key_response(res, bucket_dek_ids,
                                                 [unknown_id] + kek_ids)

        keys_path = Path(node.data_path()) / 'data' / bucket_uuid / 'deks'
        res = run_dump_bucket_deks(node, ['--key-dir', keys_path,
                                          '--key-ids'] + all_ids)
        verify_key_presense_in_dump_key_response(res, bucket_dek_ids,
                                                 [unknown_id] + kek_ids)

        res = run_dump_bucket_deks(node, ['--key-ids'] + all_ids)
        verify_key_presense_in_dump_key_response(res, bucket_dek_ids,
                                                 [unknown_id] + kek_ids)

        # Make sure that we return correct code and error in case
        # of wrong password (other utilities can rely on that)
        wrong_password = testlib.random_str(8)
        error = run_dump_keys(node,
                              ['-p', wrong_password, '--key-kind', 'kek', \
                               '--key-ids'] + all_ids,
                              expected_return_code=2)
        assert error == 'Incorrect master password\n', \
               f'unexpected error: {error}'

        # Make sure incorrect args don't lead to exit code 2 (used to be
        # the case), so it is not mixed with incorrect password
        run_dump_keys(node,
                      ['-p', wrong_password, '--key-kind', 'kek'],
                       expected_return_code=1)

        # making sure that we return correct code and error in case
        # of wrong password (other utilities can rely on that)
        wrong_password = testlib.random_str(8)
        error = run_dump_bucket_deks(node,
                                     ['-p', wrong_password,
                                      '--bucket-uuid', bucket_uuid,
                                      '--key-ids'] + all_ids,
                                     expected_return_code=2)
        assert error == 'Incorrect master password\n', \
               f'unexpected error: {error}'

        # Make sure incorrect args don't lead to exit code 2 (used to be
        # the case), so it is not mixed with incorrect password
        run_dump_bucket_deks(node,
                             ['-p', wrong_password,
                              '--bucket-uuid', bucket_uuid],
                             expected_return_code=1)

        try:
            password = change_password(node)

            def verify_dump_keys_with_password():
                res = run_dump_keys(node, ['-p', password, '--key-ids'] +
                                          all_ids)
                verify_key_presense_in_dump_key_response(
                    res, kek_ids + bucket_dek_ids, [unknown_id])

                res = run_dump_bucket_deks(node, ['-p', password, '--key-ids']
                                                 + all_ids)
                verify_key_presense_in_dump_key_response(res, bucket_dek_ids,
                                                         [unknown_id] + kek_ids)

            testlib.poll_for_condition(
                verify_dump_keys_with_password,
                sleep_time=1, timeout=30, retry_on_assert=True)
        finally:
            change_password(node, password='')

    def config_dek_automatic_rotation_test(self):
        # Enable encryption and set dek rotation int = 1 sec
        # Wait some time and check if dek has rotated
        secret = cb_managed_secret(usage=['config-encryption'])
        secret_id = create_secret(self.random_node(), secret)
        kek_id = get_kek_id(self.random_node(), secret_id)
        set_cfg_encryption(self.random_node(), 'encryptionKey', secret_id,
                           dek_rotation=60*60*24*30)

        dek_path = Path() / 'config' / 'deks'

        current_dek_ids = poll_verify_deks_and_collect_ids(self.cluster,
                                                           dek_path,
                                                           verify_key_count=1)

        rotation_enabling_time = datetime.now(timezone.utc)
        print(f'Rotation enabling time: {rotation_enabling_time}')

        set_cfg_encryption(self.random_node(), 'encryptionKey', secret_id,
                           dek_rotation=1)

        time.sleep(2 + min_timer_interval) # let it rotate deks

        set_cfg_encryption(self.random_node(), 'encryptionKey', secret_id,
                           dek_rotation=60*60*24*30)

        # Verify that current dek was created after we've enabled the rotation
        def verify_ct(key_creation_time):
            return key_creation_time > rotation_enabling_time

        # Verify that config dek dir has only one dek (all old deks are
        # supposed to be removed immediatelly for config encryption)
        # Also verify that all deks existed before the rotation are gone now
        poll_verify_dek_files(self.cluster, dek_path,
                              verify_key_count=1,
                              verify_creation_time=verify_ct,
                              verify_encryption_kek=kek_id,
                              verify_id=lambda n: n not in current_dek_ids)

    def bucket_dek_automatic_rotation_test(self):
        # Enable encryption and set dek rotation int = 1 sec
        # Wait some time and check if dek has rotated
        secret = cb_managed_secret(usage=['bucket-encryption'])
        secret_id = create_secret(self.random_node(), secret)
        kek_id = get_kek_id(self.random_node(), secret_id)

        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': secret_id},
                                   sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1)

        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestDekRotationInterval': 1})

        time.sleep(2) # let it rotate deks

        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestDekRotationInterval': 0})

        # Verify that bucket has more than one dek now
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=lambda n: n > 1)

    def dont_remove_active_dek_test(self):
        # enable encryption and set dek lifetime = 1 sec,
        # wait some time and make sure active dek is not removed
        secret = cb_managed_secret(usage=['config-encryption'])
        secret_id = create_secret(self.random_node(), secret)
        kek_id = get_kek_id(self.random_node(), secret_id)
        set_cfg_encryption(self.random_node(), 'encryptionKey', secret_id,
                           dek_rotation=60*60*24*30,
                           dek_lifetime=1)

        dek_path = Path() / 'config' / 'deks'

        dek_ids1 = poll_verify_deks_and_collect_ids(self.cluster,
                                                    dek_path,
                                                    verify_key_count=1)

        time.sleep(2)

        dek_ids2 = poll_verify_deks_and_collect_ids(self.cluster,
                                                    dek_path,
                                                    verify_key_count=1)

        assert sorted(dek_ids1) == sorted(dek_ids2), \
               f'deks have changed: {dek_ids1} {dek_ids2}'

        set_cfg_encryption(self.random_node(), 'encryptionKey', secret_id,
                           dek_rotation=1,
                           dek_lifetime=3)

        # Make sure all deks have been rotated
        poll_verify_dek_files(self.cluster,
                              dek_path,
                              verify_id=lambda n: n not in dek_ids2)

        # Disable rotation and check that only one dek will be left eventually
        set_cfg_encryption(self.random_node(), 'encryptionKey', secret_id,
                           dek_rotation=60*60*24*30,
                           dek_lifetime=3)

        poll_verify_dek_files(self.cluster,
                              dek_path,
                              verify_key_count=1,
                              verify_id=lambda n: n not in dek_ids2)

    def _basic_kms_secret_test_logic(self, secret_builder, key_name,
                                     expected_key_type):
        # Create a KMS key (AWS/GCP) and use it to encrypt bucket, config,
        # and secrets
        secret_json = secret_builder(name=key_name,
                                     usage=['bucket-encryption',
                                            'config-encryption',
                                            'KEK-encryption'])
        secret_id = create_secret(self.random_node(), secret_json)
        testlib.post_succ(self.random_node(),
                          f'/settings/encryptionKeys/{secret_id}/test')
        kek_id = get_kek_id(self.random_node(), secret_id)

        # Create a bucket and encrypt it using the key
        bucket_props = {'name': self.bucket_name,
                        'ramQuota': 100,
                        'encryptionAtRestKeyId': secret_id}
        self.cluster.create_bucket(bucket_props)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)

        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=kek_id)

        # Use this key to encrypt configuration
        set_cfg_encryption(self.random_node(), 'encryptionKey', secret_id)
        dek_path = Path() / 'config' / 'deks'
        poll_verify_dek_files(self.cluster,
                              dek_path,
                              verify_key_count=1,
                              verify_encryption_kek=kek_id)

        # Create a generated secret and encrypt it with this KMS secret
        secret = cb_managed_secret(
                      name='test',
                      encrypt_with='encryptionKey',
                      encrypt_secret_id=secret_id)
        cb_managed_secret_id = create_secret(self.random_node(), secret)
        verify_kek_files(self.cluster,
                         get_secret(self.random_node(), cb_managed_secret_id),
                         verify_encryption_kek=kek_id,
                         verify_key_count=1)

        initial_secret = get_secret(self.random_node(), secret_id)
        verify_kek_files(self.cluster, initial_secret, verify_key_count=1,
                         verify_key_type=expected_key_type)

        set_remove_hist_keys_interval(self.cluster, 1000)

        rotate_secret(self.random_node(), secret_id)

        new_kek_id = get_kek_id(self.random_node(), secret_id)

        def verify_kek_ids_after_rotation():
            s = get_secret(self.random_node(), secret_id)
            # Verify that only one new KEK is left in chronicle and that it
            # is stored on disk
            verify_kek_files(self.cluster, s, verify_key_count=1,
                             verify_key_type=expected_key_type)
            assert s['data']['storedKeyIds'][0]['id'] != kek_id, \
                   'expected different key id'
            # Verify that old KEK is not present on disk
            verify_kek_files(self.cluster, initial_secret, verify_missing=True)

        testlib.poll_for_condition(
            verify_kek_ids_after_rotation,
            sleep_time=1, attempts=60, retry_on_assert=True, verbose=True)

        # Verify that generated secret and bucket deks have been re-encrypted
        # with new KEK
        poll_verify_kek_files(self.cluster,
                              get_secret(self.random_node(),
                                         cb_managed_secret_id),
                              verify_encryption_kek=new_kek_id,
                              verify_key_count=1)
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=new_kek_id)

        # Can't delete because it is in use
        delete_secret(self.random_node(), secret_id, expected_code=400)


    def basic_aws_secret_test(self):
        self._basic_kms_secret_test_logic(
            secret_builder=aws_test_secret,
            key_name='AWS Key',
            expected_key_type='awskms-symmetric')

    def basic_gcp_secret_test(self):
        self._basic_kms_secret_test_logic(
            secret_builder=gcp_test_secret,
            key_name='GCP Key',
            expected_key_type='gcpkms-symmetric')

    def basic_azure_secret_test(self):
        self._basic_kms_secret_test_logic(
            secret_builder=azure_test_secret,
            key_name='Azure Key',
            expected_key_type='azurekms')

    def basic_hashi_secret_test(self):
        self._basic_kms_secret_test_logic(
            secret_builder=hashi_test_secret,
            key_name='Hashi Key',
            expected_key_type='hashikms')

    def basic_kmip_secret_test(self):
        testKmipSecret = kmip_secret('Test KMIP Key', 1,
                                     is_test_secret=True)
        secret_id = create_secret(self.random_node(), testKmipSecret)

        self.cluster.create_bucket(
                {'name': self.bucket_name,
                 'ramQuota': 100,
                 'encryptionAtRestKeyId': secret_id})
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        kek_id = get_kek_id(self.random_node(), secret_id)
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=kek_id)

        testKmipSecret = kmip_secret('Kmip2', 5, is_test_secret=True)
        secret_id = update_secret(self.random_node(),
                                  secret_id, testKmipSecret)
        kek_id = get_kek_id(self.random_node(), secret_id)
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=1,
                                      verify_encryption_kek=kek_id)

    def dek_limit_test(self):
        set_cfg_dek_limit(self.cluster, 2)
        set_log_dek_limit(self.cluster, 2)

        secret = cb_managed_secret(usage=['config-encryption'])
        secret_id = create_secret(self.random_node(), secret)

        set_cfg_encryption(self.random_node(), 'encryptionKey', secret_id,
                           dek_rotation=1)
        set_log_encryption(self.cluster, 'nodeSecretManager', -1,
                           dek_rotation=1)

        time.sleep(3)

        verify_dek_files(self.cluster, Path() / 'config' / 'deks',
                         verify_key_count=lambda n: n <= 2)
        verify_dek_files(self.cluster, Path() / 'config' / 'logs_deks',
                         verify_key_count=lambda n: n <= 2)

    @tag(Tag.LowUrgency)
    def all_bucket_dek_limit_test(self):
        self.bucket_dek_limit_test_base(all_bucket_dek_limit=2)

    def specific_bucket_dek_limit_test(self):
        self.bucket_dek_limit_test_base(all_bucket_dek_limit=10,
                                        specific_bucket_dek_limit=2)

    def bucket_dek_limit_test_base(self,
                                   all_bucket_dek_limit = None,
                                   specific_bucket_dek_limit = None):
        secret = cb_managed_secret(usage=['bucket-encryption'])
        secret_id = create_secret(self.random_node(), secret)

        set_all_bucket_dek_limit(self.cluster, all_bucket_dek_limit)

        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': secret_id,
                                    'encryptionAtRestDekRotationInterval': 0},
                                   sync=True)
        # Set the limit after bucket is created, otherwise we don't have
        # bucket UUID yet
        set_bucket_dek_limit(self.cluster, self.bucket_name,
                             specific_bucket_dek_limit)

        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestDekRotationInterval': 1})

        # Write some documents to the bucket to make sure that we have
        # something to re-encrypt with every active DEK, otherwise
        # new DEKs are not really used and can be removed once new dek is
        # generated (it depends on implementation in memcached).
        # Note that we wait for 3 seconds here so at least 3 DEK can be
        # generated if the limit is not respected.
        for i in range(15):
            write_random_doc(self.cluster, self.bucket_name)
            time.sleep(0.2)

        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        verify_bucket_deks_files(self.cluster, bucket_uuid,
                                 verify_key_count=lambda n: n <= 2)


    def prepare_cluster_for_node_readd_testing(self):
        # Create cb-managed secret for bucket encryption
        secret = cb_managed_secret(
            usage=['bucket-encryption', 'config-encryption']
        )
        secret_id = create_secret(self.random_node(), secret)
        # Enable log and config encryption
        # Not using encryption key for log encryption because we want
        # be able to remove that key after the test in this case then
        # (logs don't support re-encryption currently)
        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        set_cfg_encryption(self.cluster, 'encryptionKey', secret_id)

        # Load sample bucket with encryption enabled
        self.load_and_assert_sample_bucket(self.cluster, self.sample_bucket)
        self.cluster.update_bucket({
            'name': self.sample_bucket,
            'encryptionAtRestKeyId': secret_id
        })

        # Get a KV node to remove
        candidate_for_removal = kv_nodes(self.cluster)[0]
        return (candidate_for_removal, secret_id)

    def modify_encryption_for_node_readd_testing(self, node, prev_secret_id):
        secret = cb_managed_secret(
            usage=['bucket-encryption', 'config-encryption']
        )
        node = self.cluster.connected_nodes[0]
        new_secret_id = create_secret(node, secret)
        set_cfg_encryption(node, 'encryptionKey', new_secret_id)
        self.cluster.update_bucket({
            'name': self.sample_bucket,
            'encryptionAtRestKeyId': new_secret_id
        })
        # Deleting old secret, so all keys that not maintained properly should
        # become not decryptable
        # Need polling because bucket reencryption takes some time and should
        # happen on all nodes, and delete_secret will only work when there is no
        # bucket DEKs encrypted with that secret
        testlib.poll_for_condition(
            lambda: delete_secret(self.random_node(), prev_secret_id),
            sleep_time=1, attempts=50, retry_on_assert=True, verbose=True)

    @tag(Tag.LowUrgency)
    def node_readd_test(self):
        node_to_remove, secret_id = \
            self.prepare_cluster_for_node_readd_testing()
        original_services = node_to_remove.get_services()
        bucket_uuid = self.cluster.get_bucket_uuid(self.sample_bucket)

        # Remove the node
        self.cluster.rebalance(ejected_nodes=[node_to_remove], wait=True,
                               verbose=True)
        self.cluster.wait_nodes_up(verbose=True)

        # When node is removed, it's config gets wiped out
        set_min_timer_interval(node_to_remove, min_timer_interval)

        # Node has left the cluster, but it still contains the bucket data
        # directory which will be removed only during rebalance
        # Make sure DEKs in that bucket directory doesn't obstruct encryption
        # at rest reconfiguration
        drop_config_deks_for_node(node_to_remove)
        drop_bucket_deks_and_verify_dek_info(self.cluster, self.sample_bucket)
        # node_to_remove should continue having 1 dek, drop deks should
        # not affect it
        poll_verify_bucket_deks_files(
            [node_to_remove],
            bucket_uuid,
            verify_key_count=1)

        # Add the node back
        self.cluster.add_node(node_to_remove, services=original_services,
                              verbose=True)

        self.modify_encryption_for_node_readd_testing(node_to_remove, secret_id)
        # Node is added back, and it still contains the bucket data directory
        # (it will be removed only during rebalance)
        # Try rotating config encryption DEK here to make sure those bucket
        # DEKs do not obstruct config DEK rotation
        drop_config_deks_for_node(node_to_remove)
        poll_verify_node_bucket_dek_info(node_to_remove, self.sample_bucket,
                                         missing=True)
        drop_bucket_deks_and_verify_dek_info(self.cluster, self.sample_bucket)

        # Rebalance to complete node addition
        self.cluster.rebalance(wait=True, verbose=True)

    @tag(Tag.LowUrgency)
    def node_readd_node_after_failover_test(self):
        node_to_failover, secret_id = \
            self.prepare_cluster_for_node_readd_testing()

        original_services = node_to_failover.get_services()

        self.cluster.stop_node(node_to_failover)

        # Must be safe failover, otherwise the node will not reset itself
        self.cluster.failover_node(node_to_failover,
                                   graceful=False,
                                   allow_unsafe=False,
                                   verbose=True)

        # Rebalance to remove the node completely
        self.cluster.rebalance(wait=True, wait_for_ejected_nodes=False,
                               verbose=True)

        self.cluster.restart_node(node_to_failover)

        self.cluster.wait_nodes_up(verbose=True)

        # Node will think it is still part of the cluster, but eventually
        # it should figure out that it was removed and reset itself.
        # Waiting for that to happen.
        testlib.wait_for_ejected_node(node_to_failover)

        self.cluster.add_node(node_to_failover, services=original_services,
                              verbose=True)
        self.cluster.rebalance(wait=True, verbose=True)

    @tag(Tag.LowUrgency)
    def restart_cluster_with_bad_secret_test(self):
        # When unavailable KEK is used for cfg encryption, cluster will
        # fail to start, but if that KEK hasn't been used yet, the cluster
        # should start successfully.
        # Basically this test verifies that problems with DEK reencryption
        # don't prevent cluster from starting. Config DEKs should be readable
        # and decryptable, this goes without saying.
        bad_secret = aws_test_secret(usage=['config-encryption'],
                                     good_arn=False)
        bad_aws_secret_id = create_secret(self.random_node(), bad_secret)
        set_cfg_encryption(self.cluster, 'encryptionKey', bad_aws_secret_id,
                           skip_encryption_key_test=True)
        self.cluster.restart_all_nodes()

    def attempt_to_use_bad_secret_test(self):
        cant_encrypt_err = 'Unable to perform encryption/decryption with ' \
                           'provided key, check encryption key settings'
        bad_encryption_key_error = 'Unable to encrypt (or decrypt) this key. ' \
                                   'Please verify the configuration of the ' \
                                   'encryption key used to protect this key';
        bad_secret = aws_test_secret(usage=['config-encryption',
                                            'bucket-encryption',
                                            'audit-encryption',
                                            'log-encryption'],
                                     good_arn=False)
        bad_aws_secret_id = create_secret(self.random_node(), bad_secret)

        err = create_secret(self.random_node(),
                            cb_managed_secret(
                                encrypt_with='encryptionKey',
                                encrypt_secret_id=bad_aws_secret_id),
                            expected_code=400)
        testlib.assert_in(bad_encryption_key_error, err['_'])

        err = set_cfg_encryption(self.cluster, 'encryptionKey',
                                 bad_aws_secret_id,
                                 expected_code=400)
        testlib.assert_in(cant_encrypt_err,  err['_'])

        err = set_audit_encryption(self.cluster, 'encryptionKey',
                                   bad_aws_secret_id,
                                   expected_code=400)
        testlib.assert_in(cant_encrypt_err,  err['_'])

        err = set_log_encryption(self.cluster, 'encryptionKey',
                                 bad_aws_secret_id,
                                 expected_code=400)
        testlib.assert_in(cant_encrypt_err,  err['_'])

        r = self.cluster.create_bucket(
                {'name': self.bucket_name,
                 'ramQuota': 100,
                 'encryptionAtRestKeyId': bad_aws_secret_id},
                sync=True,
                expected_code=400)

        errors = r.json()
        e = errors['errors']['encryptionAtRestKeyId']
        testlib.assert_in(cant_encrypt_err, e)

        self.cluster.create_bucket(
                {'name': self.bucket_name,
                 'ramQuota': 100,
                 'encryptionAtRestKeyId': -1},
                sync=True)

        r = self.cluster.update_bucket(
                {'name': self.bucket_name,
                 'encryptionAtRestKeyId': bad_aws_secret_id},
                expected_code=400)
        errors = r.json()
        e = errors['errors']['encryptionAtRestKeyId']
        testlib.assert_in(cant_encrypt_err, e)

    @tag(Tag.LowUrgency)
    def add_node_when_kek_is_unavailable_test(self):
        bad_secret = aws_test_secret(usage=['config-encryption'],
                                     good_arn=False)
        bad_aws_secret_id = create_secret(self.random_node(), bad_secret)

        # Trying to use bad AWS secret for encryption
        # This node will keep trying to reencrypt DEKs, which is ok
        set_cfg_encryption(self.cluster, 'encryptionKey', bad_aws_secret_id,
                           skip_encryption_key_test=True)

        node_to_remove = self.cluster.connected_nodes[-1]
        original_services = node_to_remove.get_services()

        # Remove the node
        self.cluster.rebalance(ejected_nodes=[node_to_remove], wait=True,
                               verbose=True)
        # Cluster is using bad secret for encryption, and its DEKs can't be
        # reencrypted. However, when this node leaves the cluster, it stops
        # using cluster secrets, switches to encryption service (default), and
        # reencrypts its DEKs.
        self.cluster.wait_nodes_up(verbose=True)

        # When node is removed, it's config gets wiped out
        set_min_timer_interval(node_to_remove, min_timer_interval)

        # Add the node back
        # During addition, the new node should encrypt its DEKs using cluster
        # secrets, but it will fail because current secret for cfg is bad.
        r = self.cluster.add_node(node_to_remove, services=original_services,
                                  expected_code=400, verbose=True)
        self.cluster.wait_nodes_up(verbose=True)
        testlib.assert_in('Failed to reencrypt some encryption-at-rest keys',
                          r.text)

        # Don't use bad secret for cfg encryption anymore...
        set_cfg_encryption(self.cluster, 'nodeSecretManager', -1)
        # ... and try again, this time addition should succeed
        self.cluster.add_node(node_to_remove, services=original_services,
                              verbose=True)
        # Rebalance to complete node addition
        self.cluster.rebalance(wait=True, verbose=True)

    @tag(Tag.LowUrgency)
    def node_failover_and_add_back_delta_test(self):
        self.node_failover_and_add_back_base(recovery_type="delta")

    @tag(Tag.LowUrgency)
    def node_failover_and_add_back_full_test(self):
        self.node_failover_and_add_back_base(recovery_type="full")

    def node_failover_and_add_back_base(self, recovery_type=None):
        node_to_failover, secret_id = \
            self.prepare_cluster_for_node_readd_testing()
        bucket_uuid = self.cluster.get_bucket_uuid(self.sample_bucket)

        self.cluster.failover_node(node_to_failover,
                                   graceful=False,
                                   allow_unsafe=False,
                                   verbose=True)
        self.modify_encryption_for_node_readd_testing(node_to_failover,
                                                      secret_id)
        drop_config_deks_for_node(node_to_failover)
        drop_bucket_deks_and_verify_dek_info(self.cluster, self.sample_bucket)
        # node_to_failover should have 2 deks now, because it can't drop
        # old dek, bucket is not created in memcached so it can't reencrypt
        # the data
        poll_verify_bucket_deks_files(
            [node_to_failover],
            bucket_uuid,
            verify_key_count=2)

        self.cluster.set_recovery_type(node_to_failover,
                                       recovery_type=recovery_type,
                                       verbose=True)
        drop_config_deks_for_node(node_to_failover)
        drop_bucket_deks_and_verify_dek_info(self.cluster, self.sample_bucket)
        # for the same reason as above node should have 3 deks now
        poll_verify_bucket_deks_files(
            [node_to_failover],
            bucket_uuid,
            verify_key_count=3)

        self.cluster.rebalance(wait=True, verbose=True)

    @tag(Tag.LowUrgency)
    def drop_deks_test(self):
        self.load_and_assert_sample_bucket(self.cluster, self.sample_bucket)
        bucket_uuid = self.cluster.get_bucket_uuid(self.sample_bucket)
        poll_verify_bucket_deks_files(self.cluster,
                                      bucket_uuid,
                                      verify_key_count=0)
        secret_json = aws_test_secret(usage=['bucket-encryption'])
        aws_secret_id = create_secret(self.random_node(), secret_json)
        self.cluster.update_bucket({'name': self.sample_bucket,
                                    'encryptionAtRestKeyId': aws_secret_id})

        # Compaction is needed to make sure the dek is actually used
        # If first key is not actually used, the drop key procedure will have
        # nothing to do, and the first key can simply be removed in this case
        testlib.post_succ(self.cluster,
                          f'/pools/default/buckets/{self.sample_bucket}' \
                          '/controller/compactBucket')
        dek_ids = poll_verify_and_get_mcd_deks_in_use(self.cluster,
                                                      self.sample_bucket,
                                                      verify_key_count=1)
        # Sleep to make sure drop bucket time that is generated by
        # the drop keys endpoint is strictly greater than current dek's
        # creation time.
        # In other words if we generate dek and call /drop immediately after
        # that, that dek will be considered up to date, because its datetime
        # is >= drop keys datetime.
        time.sleep(1.1)

        drop_time = datetime.now(timezone.utc).replace(microsecond=0)
        # 'Drop key' should force encryption of the whole bucket
        drop_bucket_deks_and_verify_dek_info(self.cluster,
                                             self.sample_bucket)

        new_dek_ids = assert_bucket_deks_have_changed(self.cluster,
                                                      self.sample_bucket,
                                                      verify_key_count=1,
                                                      min_time=drop_time,
                                                      old_dek_ids=dek_ids)

        # Sleeping for the same reason as above
        time.sleep(1.1)
        # call 'drop key' again, this time it should re-encrypt the data with
        # a new key. We can't verify that the data was re-encrypted but we can
        # check that deks changed
        drop_time = datetime.now(timezone.utc).replace(microsecond=0)
        drop_bucket_deks_and_verify_dek_info(self.cluster,
                                             self.sample_bucket)

        assert_bucket_deks_have_changed(self.cluster,
                                        self.sample_bucket,
                                        verify_key_count=1,
                                        min_time=drop_time,
                                        old_dek_ids=new_dek_ids)

    @tag(Tag.LowUrgency)
    def force_bucket_encryption_test(self):
        self.load_and_assert_sample_bucket(self.cluster, self.sample_bucket)
        poll_verify_cluster_bucket_dek_info(self.cluster, self.sample_bucket,
                                            data_statuses=['unencrypted'],
                                            dek_number=0)
        secret_json = aws_test_secret(usage=['bucket-encryption'])
        aws_secret_id = create_secret(self.random_node(), secret_json)
        self.cluster.update_bucket({'name': self.sample_bucket,
                                    'encryptionAtRestKeyId': aws_secret_id})
        poll_verify_cluster_bucket_dek_info(
            self.cluster, self.sample_bucket,
            data_statuses=['partiallyEncrypted'],
            dek_number=1)
        force_bucket_encryption(self.random_node(), self.sample_bucket)
        poll_verify_cluster_bucket_dek_info(self.cluster, self.sample_bucket,
                                            data_statuses=['encrypted'],
                                            dek_number=1)
        self.cluster.update_bucket({'name': self.sample_bucket,
                                    'encryptionAtRestKeyId': -1})
        force_bucket_encryption(self.random_node(), self.sample_bucket)
        poll_verify_cluster_bucket_dek_info(self.cluster, self.sample_bucket,
                                            data_statuses=['unencrypted'],
                                            dek_number=0)

    def remove_old_deks_test(self):
        secret = cb_managed_secret(usage=['bucket-encryption'])
        secret_id = create_secret(self.random_node(), secret)
        kek_id = get_kek_id(self.random_node(), secret_id)

        create_time = datetime.now(timezone.utc).replace(microsecond=0)
        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': secret_id},
                                   sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        # Memorize deks in use (there should one on each kv node)
        dek_ids = assert_bucket_deks_have_changed(self.cluster,
                                                  self.bucket_name,
                                                  verify_key_count=1,
                                                  min_time=create_time,
                                                  old_dek_ids=[])

        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestDekRotationInterval': 1})

        # Now it generates a dek every sec, but it won't have more than 2 deks
        # because we don't write any new data to the bucket, and we are not
        # running compactions, so there always should be two deks: the one that
        # was used to encrypt data (the very first dek), and the one that is
        # active now.
        poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                      verify_key_count=lambda n: n >= 2)

        # Change dek lifetime to 1 second and wait...
        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestDekLifetime': 1,
                                    'encryptionAtRestDekRotationInterval': 600})

        # Now the very first dek should expire and the system should start
        # a compaction in order to get rid of it
        time.sleep(2)

        assert_bucket_deks_have_changed(self.cluster,
                                        self.bucket_name,
                                        verify_key_count=1,
                                        min_time=create_time,
                                        old_dek_ids=dek_ids)

    def dek_reencryption_test(self):
        # reenable encryption using different secret
        # and check if old dek is the same and that it gets reencrypted
        secret1 = cb_managed_secret(usage=['bucket-encryption'])
        secret_id1 = create_secret(self.random_node(), secret1)
        kek_id1 = get_kek_id(self.random_node(), secret_id1)

        secret2 = cb_managed_secret(usage=['bucket-encryption'])
        secret_id2 = create_secret(self.random_node(), secret2)
        kek_id2 = get_kek_id(self.random_node(), secret_id2)

        create_time = datetime.now(timezone.utc).replace(microsecond=0)
        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': secret_id1},
                                   sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)

        # Memorize deks in use (there should one on each kv node)
        dek_ids1 = poll_verify_bucket_deks_and_collect_ids(
                     self.cluster,
                     bucket_uuid,
                     verify_key_count=1,
                     verify_encryption_kek=kek_id1)

        # Disable encryption for bucket
        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestKeyId': -1})

        # Now, while encryption is disabled, dek should still exist (because
        # there were no compaction). Rotate kek and verify that dek gets
        # re-encrypted.
        rotate_secret(self.random_node(), secret_id1)
        new_kek_id1 = get_kek_id(self.random_node(), secret_id1)

        dek_ids2 = poll_verify_bucket_deks_and_collect_ids(
                     self.cluster,
                     bucket_uuid,
                     verify_key_count=1,
                     verify_encryption_kek=new_kek_id1)

        assert dek_ids2 == dek_ids1, \
               f'deks have changed, old deks: {dek_ids1}, new deks: {dek_ids2}'

        # Enabled encryption again, but use another secret this time and
        # verify that dek gets rotated
        self.cluster.update_bucket({'name': self.bucket_name,
                                    'encryptionAtRestKeyId': secret_id2})

        dek_ids3 = poll_verify_bucket_deks_and_collect_ids(
                     self.cluster,
                     bucket_uuid,
                     verify_key_count=1,
                     verify_encryption_kek=kek_id2)

        assert dek_ids3 == dek_ids1, \
               f'deks have changed, old deks: {dek_ids1}, new deks: {dek_ids3}'

    def encrypted_logs_test(self):
        logs = ["debug.log", "ns_couchdb.log", "babysitter.log"]

        def encrypted(suffixes):
            return assert_logs_encrypted(self.cluster, logs,
                                         check_suffixes=suffixes)
        def unencrypted(suffixes):
            return assert_logs_unencrypted(self.cluster, logs,
                                           check_suffixes=suffixes)
        def poll(func):
            testlib.poll_for_condition(func, sleep_time=1, attempts=50,
                                       retry_on_assert=True, verbose=True)
        def dek_ids(filename):
            return get_log_file_dek_ids(self.cluster, filename)

        def assert_logs_dek_ids(log_suffix, dek_id_assert_func):
            for l in logs:
                assert_log_file_dek_ids(self.cluster, l, log_suffix,
                                        dek_id_assert_func)

        # make sure that debug.log is unencrypted
        poll(lambda: unencrypted(['']))

        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        # debug.log      - encrypted
        # debug.log.1.gz - unencrypted
        poll(lambda: (encrypted(['']), unencrypted(['.1.gz'])))

        # debug.log      - unencrypted
        # debug.log.1    - encrypted
        # debug.log.2.gz - unencrypted
        set_log_encryption(self.cluster, 'disabled', -1)
        poll(lambda: (unencrypted(['', '.2.gz']), encrypted(['.1'])))

        # debug.log      - encrypted
        # debug.log.1.gz - unencrypted
        # debug.log.2    - encrypted
        # debug.log.3.gz - unencrypted
        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        poll(lambda: (encrypted(['', '.2']), unencrypted(['.1.gz', '.3.gz'])))
        ids = dek_ids("debug.log")
        assert_logs_dek_ids('.2', lambda id: id in ids)
        assert_logs_dek_ids('', lambda id: id in ids)
        poll_verify_cluster_dek_info(self.cluster, 'logs',
                                     data_statuses=['partiallyEncrypted'])

        # debug.log      - encrypted
        # debug.log.1    - encrypted
        # debug.log.2    - encrypted
        # debug.log.3    - encrypted
        force_encryption(self.cluster, 'log')
        poll(lambda: encrypted(['', '.*']))
        poll_verify_cluster_dek_info(self.cluster, 'logs',
                                     data_statuses=['encrypted'])

        assert_logs_dek_ids('*', lambda id: id in ids)

        # All logs are still encrypted but deks should change
        drop_time = drop_deks(self.cluster, 'log')
        poll(lambda: assert_logs_dek_ids('*', lambda id: id not in ids))
        poll_verify_cluster_dek_info(self.cluster, 'logs',
                                     data_statuses=['encrypted'],
                                     dek_number=1,
                                     oldest_dek_time=drop_time)

        # all logs are unencrypted now
        set_log_encryption(self.cluster, 'disabled', -1)
        drop_deks(self.cluster, 'log')
        poll(lambda: unencrypted(['', '.*']))
        poll_verify_cluster_dek_info(self.cluster, 'logs',
                                     data_statuses=['unencrypted'],
                                     dek_number=0)

        # Enable encryption again, encrypt everything, and then check
        # that force_encryption also decrypts all logs after encryption
        # disablement
        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        drop_deks(self.cluster, 'log')
        poll(lambda: encrypted(['', '.*']))
        poll_verify_cluster_dek_info(self.cluster, 'logs',
                                     data_statuses=['encrypted'])

        set_log_encryption(self.cluster, 'disabled', -1)
        force_encryption(self.cluster, 'log')
        poll(lambda: unencrypted(['', '.*']))
        poll_verify_cluster_dek_info(self.cluster, 'logs',
                                     data_statuses=['unencrypted'])

    def stored_keys_file_encrypted_test(self):
        # verify that tokens file is encrypted and that it can be decrypted
        # using standard tool (cbcat), which basically verifies that
        # "couchbase-file-encryption" is implemented correctly in gosecrets
        node = self.random_node()
        data_dir = node.data_path()
        tokens_file = Path(data_dir) / 'config' / 'stored_keys_tokens'
        assert_file_is_decryptable(node, tokens_file)

    def config_dat_is_encrypted_test(self):
        # verify that config data is encrypted and that it can be decrypted
        # using standard tool (cbcat), which basically verifies that
        # "couchbase-file-encryption" is implemented correctly in cbcrypto
        node = self.random_node()
        data_dir = node.data_path()
        config_data_file = Path(data_dir) / 'config' / 'config.dat'
        assert_file_is_decryptable(node, config_data_file)

    def dek_settings_test_cases(self):
        return [ # (rotation, lifetime, success)
                (1, None, False),
                (min_dek_rotation - 1, None, False),
                (default_dek_lifetime - dek_lifetime_margin + 1, None, False),
                (default_dek_lifetime - dek_lifetime_margin, None, True),
                (None, 1, False),
                (None, min_dek_lifetime - 1, False),
                (None, default_dek_rotation + dek_lifetime_margin - 1, False),
                (None, default_dek_rotation + dek_lifetime_margin, True),
                (min_dek_rotation, default_max_deks * min_dek_rotation + 1,
                 False),
                (min_dek_rotation, default_max_deks * min_dek_rotation, True),
                (min_dek_rotation, min_dek_lifetime, True),
                (min_dek_lifetime, min_dek_lifetime + dek_lifetime_margin - 1,
                 False),
                (min_dek_lifetime, min_dek_lifetime + dek_lifetime_margin,
                 True),
                (0, min_dek_lifetime + 1, False),
                (0, 0, True),
                (min_dek_rotation + 1, 0, False),
               ]

    @tag(Tag.LowUrgency)
    def bucket_dek_bad_settings_validation_test(self):
        # Currently we expect dek lifetime to be at least 5 minutes more
        # than the rotation interval
        secret = cb_managed_secret(usage=['bucket-encryption'])
        secret_id = create_secret(self.random_node(), secret)

        # disable bypass to enable full validation
        set_ns_config_value(self.cluster, 'test_bypass_encr_cfg_restrictions',
                            None)

        bucket_props = {'name': self.bucket_name,
                        'ramQuota': 100,
                        'encryptionAtRestKeyId': secret_id}

        def test_create_bucket(rotation, lifetime, expected_code):
            props = bucket_props.copy()
            if rotation is not None:
                props['encryptionAtRestDekRotationInterval'] = rotation
            if lifetime is not None:
                props['encryptionAtRestDekLifetime'] = lifetime
            self.cluster.create_bucket(props, expected_code=expected_code,
                                       sync=True)
            if expected_code == 202:
                self.cluster.delete_bucket(self.bucket_name)

        def test_update_bucket(rotation, lifetime, expected_code):
            props = {'name': self.bucket_name}
            if rotation is not None:
                props['encryptionAtRestDekRotationInterval'] = rotation
            if lifetime is not None:
                props['encryptionAtRestDekLifetime'] = lifetime
            self.cluster.update_bucket(props, expected_code=expected_code)
            if expected_code == 200:
                # restore default values
                props['encryptionAtRestDekRotationInterval'] = default_dek_rotation
                props['encryptionAtRestDekLifetime'] = default_dek_lifetime
                self.cluster.update_bucket(props)

        try:
            for r, l, s in self.dek_settings_test_cases():
                test_create_bucket(r, l, 202 if s else 400)

            self.cluster.create_bucket(bucket_props, sync=True)
            r = testlib.get_succ(self.cluster,
                                 f'/pools/default/buckets/{self.bucket_name}')
            r = r.json()
            assert r['encryptionAtRestDekRotationInterval'] == default_dek_rotation, \
                   f'unexpected bucket rotation'
            assert r['encryptionAtRestDekLifetime'] == default_dek_lifetime, \
                   f'unexpected bucket lifetime'

            for r, l, s in self.dek_settings_test_cases():
                test_update_bucket(r, l, 200 if s else 400)

        finally:
            # re-enable bypass
            set_ns_config_value(self.cluster,
                                'test_bypass_encr_cfg_restrictions', 'true')

    def dek_bad_settings_validation_test(self):
        node = self.random_node()

        # disable bypass to enable full validation
        set_ns_config_value(node, 'test_bypass_encr_cfg_restrictions',
                            None)

        def test_dek_settings(rotation, lifetime, expected_code):
            set_log_encryption(node, 'nodeSecretManager', -1,
                               dek_lifetime=lifetime, dek_rotation=rotation,
                               expected_code=expected_code)
            if expected_code == 200:
                set_log_encryption(node, 'nodeSecretManager', -1,
                                   dek_lifetime=default_dek_lifetime,
                                   dek_rotation=default_dek_rotation)

        try:
            # Test config validation for just log encryption, the validation
            # logic being tested here is shared with other types
            for r, l, s in self.dek_settings_test_cases():
                test_dek_settings(r, l, 200 if s else 400)
        finally:
            # re-enable bypass
            set_ns_config_value(node, 'test_bypass_encr_cfg_restrictions',
                                'true')

    @tag(Tag.LowUrgency)
    def bucket_dir_migration_test(self):
        # Since bucket name is considered PII, we should stop using bucket
        # name as bucket dir name in 8.0
        # Node should move all existing bucket data to the new bucket dir (uuid)
        # during the first startup after the upgrade (and before creating
        # buckets in memcached)
        kv_node = random.choice(kv_nodes(self.cluster))
        bucket_props = {'name': self.bucket_name,
                        'ramQuota': 100}
        self.cluster.create_bucket(bucket_props, sync=True)
        bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
        data_dir = Path(kv_node.data_path()) / 'data'
        new_bucket_dir = data_dir / bucket_uuid
        pre_80_bucket_dir = data_dir / self.bucket_name
        assert new_bucket_dir.exists(), \
               f'new bucket dir {new_bucket_dir} does not exist'
        assert not pre_80_bucket_dir.exists(), \
               f'pre-8.0 bucket dir {pre_80_bucket_dir} exists'

        docs = {}
        for i in range(10):
            key, value = write_random_doc(self.cluster, self.bucket_name)
            docs[key] = value

        self.cluster.stop_node(kv_node)

        # move cluster data to old bucket dir, imitating pre-8.0 bucket
        print(f'renaming {new_bucket_dir} to {pre_80_bucket_dir}')
        moved_files = list(new_bucket_dir.iterdir())
        os.rename(new_bucket_dir, pre_80_bucket_dir)

        self.cluster.restart_node(kv_node)

        testlib.poll_for_condition(
            lambda: new_bucket_dir.exists(),
            sleep_time=1, attempts=50, verbose=True)

        testlib.poll_for_condition(
            lambda: not pre_80_bucket_dir.exists(),
            sleep_time=1, attempts=50, verbose=True)

        # Make sure that all documents are still present (basically making
        # sure that bucket dir migration is successful)
        for key, expected_value in docs.items():
            value = testlib.poll_for_condition(
                    lambda: get_doc(self.cluster, self.bucket_name, key),
                    sleep_time=1, attempts=50, retry_on_assert=True,
                    verbose=True)
            assert value == expected_value, f'doc {key} does not match'

        # Check that migration moved all files back to new bucket dir
        for f in moved_files:
            assert f.exists(), f'file {f} does not exist'

        # Now make sure we remove all bucket dirs when bucket is deleted
        self.cluster.delete_bucket(self.bucket_name)
        testlib.poll_for_condition(
            lambda: not new_bucket_dir.exists(),
            sleep_time=1, attempts=50, verbose=True)
        testlib.poll_for_condition(
            lambda: not pre_80_bucket_dir.exists(),
            sleep_time=1, attempts=50, verbose=True)

    def breakpad_state_changes_test(self):
        def assert_breakpad_status_on_all_nodes(expected_en):
            for node in self.cluster.connected_nodes:
                def check():
                    BaseP = Path(node.data_path())
                    memcached_file_path = BaseP / 'config' / 'memcached.json'
                    assert memcached_file_path.exists(), \
                           f'{memcached_file_path} not found on {node}'
                    stdout = node.run_cbcat("", memcached_file_path)
                    try:
                        content = json.loads(stdout)
                        return content['breakpad']['enabled'] == expected_en
                    except Exception as e:
                        msg = (f'Failed to get breakpad status from '
                               f'{memcached_file_path}: {e}')
                        assert False, msg
                testlib.poll_for_condition(
                    check, sleep_time=1, attempts=30, verbose=True)

        # Default is enabled so breakpad should be initially enabled
        assert_breakpad_status_on_all_nodes(True)

        # Enable log encryption and breakpad on all nodes should go disabled
        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        assert_breakpad_status_on_all_nodes(False)

        # Disable log encryption and breakpad on all nodes should go enabled
        set_log_encryption(self.cluster, 'disabled', -1)
        assert_breakpad_status_on_all_nodes(True)

        # Enable bucket encryption and breakpad on all nodes should go disabled
        secret = cb_managed_secret(usage=['bucket-encryption'])
        secret_id = create_secret(self.random_node(), secret)
        self.cluster.create_bucket({'name': self.bucket_name, 'ramQuota': 100,
                                    'encryptionAtRestKeyId': secret_id},
                                   sync=True)
        assert_breakpad_status_on_all_nodes(False)

        # Remove encrypted bucket and breakpad on all nodes should go enabled
        self.cluster.delete_bucket(self.bucket_name)
        assert_breakpad_status_on_all_nodes(True)

        # Enable log encryption and breakpad on all nodes should go disabled
        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        assert_breakpad_status_on_all_nodes(False)

        try:
            # forceCrashDumps=true and breakpad on all nodes should go enabled
            testlib.post_succ(self.cluster, '/settings/security',
                              data={'forceCrashDumps': 'true'})
            assert_breakpad_status_on_all_nodes(True)

            # forceCrashDumps=false and breakpad on all nodes should go disabled
            testlib.post_succ(self.cluster, '/settings/security',
                              data={'forceCrashDumps': 'false'})
            assert_breakpad_status_on_all_nodes(False)
        finally:
            testlib.delete(self.cluster, f'/settings/security/forceCrashDumps')

        # Disable log encryption and breakpad on all nodes should go enabled
        set_log_encryption(self.cluster, 'disabled', -1)
        assert_breakpad_status_on_all_nodes(True)


    def existing_key_test_endpoints_test(self):
        secret_id = create_secret(self.random_node(),
                                  cb_managed_secret(name='test secret',
                                                    usage=['KEK-encryption']))
        bad_aws_id = create_secret(self.random_node(),
                                   aws_test_secret(good_arn=False,
                                                   usage=['KEK-encryption']))
        good_aws_id = create_secret(self.random_node(),
                                    aws_test_secret(good_arn=True,
                                                    usage=['KEK-encryption']))

        # Existing key test endpoint tests:
        test_existing_secret(self.random_node(), secret_id)
        test_existing_secret(self.random_node(), good_aws_id)
        err = test_existing_secret(self.random_node(), bad_aws_id,
                                   expected_code=400)
        assert 'Unable to perform encryption/decryption with provided key' \
               in err['_'], 'unexpected error'

    def create_key_test_endpoints_test(self):
        # create a secret to make sure that we can create a secret with
        # a name that is already in use
        create_secret(self.random_node(),
                      cb_managed_secret(name='test secret',
                                        usage=['KEK-encryption']))

        bad_aws_id = create_secret(self.random_node(),
                                   aws_test_secret(good_arn=False,
                                                   usage=['KEK-encryption']))
        good_aws_id = create_secret(self.random_node(),
                                    aws_test_secret(good_arn=True,
                                                    usage=['KEK-encryption']))
        # Create key test endpoint tests:
        test_create_secret(self.random_node(),
                           cb_managed_secret(usage=['bucket-encryption']))

        err = test_create_secret(self.random_node(),
                                cb_managed_secret(name='test secret'),
                                expected_code=400)
        assert 'Must be unique' in err['name'], 'unexpected error'

        test_create_secret(self.random_node(),
                           aws_test_secret(good_arn=True))

        err = test_create_secret(self.random_node(),
                                 aws_test_secret(good_arn=False),
                                 expected_code=400)
        assert 'test encryption error' in err['_'], 'unexpected error'

        test_create_secret(self.random_node(),
                           cb_managed_secret(encrypt_with='encryptionKey',
                                             encrypt_secret_id=good_aws_id))

        err = test_create_secret(self.random_node(),
                                 cb_managed_secret(
                                     encrypt_with='encryptionKey',
                                     encrypt_secret_id=bad_aws_id),
                                 expected_code=400)
        assert 'Unable to encrypt (or decrypt) this key' in err['_'], \
               'unexpected error'


    def update_key_test_endpoints_test(self):
        secret_id = create_secret(self.random_node(),
                                  cb_managed_secret(name='test secret',
                                                    usage=['KEK-encryption']))
        bad_aws_id = create_secret(self.random_node(),
                                   aws_test_secret(good_arn=False,
                                                   usage=['KEK-encryption']))
        good_aws_id = create_secret(self.random_node(),
                                    aws_test_secret(good_arn=True,
                                                    usage=['KEK-encryption']))

        # Update key test endpoint tests:
        test_update_secret(self.cluster, 999999999,
                           cb_managed_secret(usage=['bucket-encryption']),
                           expected_code=404)

        test_update_secret(self.cluster, secret_id,
                           cb_managed_secret(name='test secret 2',
                                             usage=['bucket-encryption']))

        test_update_secret(self.cluster, secret_id,
                           cb_managed_secret(name='test secret 2',
                                             usage=['bucket-encryption'],
                                             encrypt_with='encryptionKey',
                                             encrypt_secret_id=good_aws_id))

        err = test_update_secret(self.cluster, secret_id,
                                 cb_managed_secret(
                                     name='test secret 2',
                                     usage=['bucket-encryption'],
                                     encrypt_with='encryptionKey',
                                     encrypt_secret_id=bad_aws_id),
                                 expected_code=400)
        assert 'Unable to encrypt (or decrypt) this key' in err['_'], \
               'unexpected error'

    def break_key_that_is_used_test(self):
        aws_secret = aws_test_secret(good_arn=True, usage=['KEK-encryption'])
        aws_id = create_secret(self.random_node(), aws_secret)
        secret_id = create_secret(self.random_node(),
                                  cb_managed_secret(name='test secret',
                                                    usage=['log-encryption',
                                                           'bucket-encryption'],
                                                    encrypt_with='encryptionKey',
                                                    encrypt_secret_id=aws_id))

        set_log_encryption(self.cluster, 'encryptionKey', secret_id)

        # Now secret_id is in use, try breaking it by breaking the key that
        # encrypts it:
        bad_creds_node = self.random_node()
        write_bad_aws_creds_file(bad_creds_node)
        bad_creds_file = aws_fake_creds_path(bad_creds_node)
        aws_secret['data']['credentialsFile'] = bad_creds_file
        r = update_secret(self.cluster, aws_id, aws_secret, expected_code=400)
        assert 'Encryption key test failed on' in r['_'], 'unexpected error'

        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        drop_deks(self.cluster, 'log')

        self.cluster.create_bucket(
                {'name': self.bucket_name, 'ramQuota': 100,
                 'encryptionAtRestKeyId': secret_id}, sync=True)

        r = update_secret(self.cluster, aws_id, aws_secret, expected_code=400)
        assert 'Encryption key test failed on' in r['_'], 'unexpected error'

    def use_bad_key_for_encryption_test(self):
        bad_creds_node = self.random_node()
        creds_file = write_good_aws_creds_file(bad_creds_node)
        aws_secret = aws_test_secret(good_arn=True, usage=['KEK-encryption'],
                                     creds_file=creds_file)
        aws_secret_id = create_secret(self.random_node(), aws_secret)
        int_secret_id = create_secret(
                          self.random_node(),
                          cb_managed_secret(name='intermediate secret',
                                            usage=['KEK-encryption'],
                                            encrypt_with='encryptionKey',
                                            encrypt_secret_id=aws_secret_id))
        bad_secret_id = create_secret(
                          self.random_node(),
                          cb_managed_secret(name='test secret',
                                            usage=['KEK-encryption',
                                                   'bucket-encryption',
                                                   'log-encryption'],
                                            encrypt_with='encryptionKey',
                                            encrypt_secret_id=int_secret_id))

        # So far everything should be working, but suddenly aws credentials
        # stop working on one of the nodes:
        write_bad_aws_creds_file(bad_creds_node)

        err = set_log_encryption(self.cluster, 'encryptionKey', bad_secret_id,
                                 expected_code=400)
        assert 'Unable to perform encryption/decryption with provided key, ' \
               'check encryption key settings' in err['_'], \
               'unexpected error'

        r = self.cluster.create_bucket(
                {'name': self.bucket_name, 'ramQuota': 100,
                 'encryptionAtRestKeyId': bad_secret_id},
                sync=True,
                expected_code=400)
        errors = r.json()
        e = errors['errors']['encryptionAtRestKeyId']
        assert e is not None, f'unexpected errors: {errors}'
        assert 'Unable to perform encryption/decryption with provided key, ' \
               'check encryption key settings' in e, \
               'unexpected error'

        self.cluster.create_bucket(
                {'name': self.bucket_name, 'ramQuota': 100,
                 'encryptionAtRestKeyId': -1}, sync=True)
        r = self.cluster.update_bucket({'name':self.bucket_name,
                                        'encryptionAtRestKeyId': bad_secret_id},
                                       expected_code=400)
        errors = r.json()
        e = errors['errors']['encryptionAtRestKeyId']
        assert 'Unable to perform encryption/decryption with provided key, ' \
               'check encryption key settings' in e, \
               'unexpected error'

    @tag(Tag.LowUrgency)
    def no_quorum_test(self):
        def assert_no_quorum_error(err):
            assert 'Operation temporarily cannot be performed possibly due ' \
                   'to loss of quorum' in err, 'unexpected error'

        secret_id = create_secret(self.random_node(),
                            cb_managed_secret(name='test secret',
                                              usage=['KEK-encryption']))
        # Make sure the system has time to test the just created secret,
        # so we can verify that it ignores stale test results from down nodes
        # received from ns_doctor (see assert_test_results).
        time.sleep(key_test_interval)
        to_shutdown = kv_nodes(self.cluster)
        non_kv_node = next(filter(lambda n: Service.KV not in n.get_services(),
                                  self.cluster.connected_nodes))

        [self.cluster.stop_node(n) for n in to_shutdown]

        err = create_secret(non_kv_node,
                            cb_managed_secret(name='test secret',
                                              usage=['KEK-encryption']),
                            expected_code=503)
        assert_no_quorum_error(err)

        err = update_secret(non_kv_node, secret_id,
                            cb_managed_secret(name='test secret2',
                                              usage=['KEK-encryption']),
                            expected_code=503)
        assert_no_quorum_error(err)

        err = delete_secret(non_kv_node, secret_id, expected_code=503)
        assert_no_quorum_error(err)

        secrets = get_secrets(non_kv_node)
        assert len(secrets) >= 1, f'unexpected secrets: {secrets}'

        secret = get_secret(non_kv_node, secret_id)
        assert secret['name'] == 'test secret', 'unexpected name'

        def assert_no_connection_error(err):
            assert re.search('Encryption key test failed on .*: ' \
                             'No connection to node', err['_']), \
                   'unexpected error'

        err = test_existing_secret(non_kv_node, secret_id, expected_code=400)
        assert_no_connection_error(err)

        err = test_create_secret(non_kv_node,
                                 cb_managed_secret(usage=['bucket-encryption']),
                                 expected_code=400)
        assert_no_connection_error(err)

        err = test_update_secret(non_kv_node, secret_id,
                                 cb_managed_secret(name='test secret2',
                                                   usage=['KEK-encryption']),
                                 expected_code=400)
        assert_no_connection_error(err)

        def assert_test_results():
            secret = get_secret(non_kv_node, secret_id)
            error_descr = "Missing test results for some nodes"
            assert_secret_test_results(secret, "unknown",
                                       expected_description=error_descr,
                                       expected_success_nodes=[non_kv_node],
                                       expected_missing_nodes=to_shutdown)

        testlib.poll_for_condition(
            assert_test_results,
            sleep_time=1, attempts=50, retry_on_assert=True, verbose=True)

        [self.cluster.restart_node(n) for n in to_shutdown]
        self.cluster.wait_for_nodes_to_be_healthy()

        def assert_test_results_after_restart():
            secret = get_secret(non_kv_node, secret_id)
            all_nodes = self.cluster.connected_nodes
            assert_secret_test_results(secret, "ok",
                                       expected_description="Test passed",
                                       expected_success_nodes=all_nodes)

        testlib.poll_for_condition(
            assert_test_results_after_restart,
            sleep_time=1, attempts=50, retry_on_assert=True, verbose=True)

    @tag(Tag.LowUrgency)
    def periodic_secret_test_test(self):
        bad_creds_node = self.random_node()
        all_nodes = self.cluster.connected_nodes
        creds_file = write_good_aws_creds_file(bad_creds_node)
        aws_secret = aws_test_secret(good_arn=True, usage=['KEK-encryption'],
                                     creds_file=creds_file)
        aws_id = create_secret(self.random_node(),
                               aws_test_secret(good_arn=True,
                                               usage=['KEK-encryption'],
                                               creds_file=creds_file))
        secret_id1 = create_secret(self.random_node(),
                      cb_managed_secret(name='encrypted with aws key',
                                        encrypt_with='encryptionKey',
                                        encrypt_secret_id=aws_id))
        secret_id2 = create_secret(self.random_node(),
                      cb_managed_secret(name='not encrypted with aws key',
                                        usage=['KEK-encryption']))

        def assert_test_results_are_ok():
            secrets = {s['id']: s for s in get_secrets(self.random_node())}
            descr = "Test passed"
            assert_secret_test_results(secrets[aws_id], "ok",
                                       expected_description=descr,
                                       expected_success_nodes=all_nodes)
            assert_secret_test_results(secrets[secret_id1], "ok",
                                       expected_description=descr,
                                       expected_success_nodes=all_nodes)
            assert_secret_test_results(secrets[secret_id2], "ok",
                                       expected_description=descr,
                                       expected_success_nodes=all_nodes)

        testlib.poll_for_condition(
            assert_test_results_are_ok,
            sleep_time=1, attempts=50, retry_on_assert=True, verbose=True)

        # Break the AWS key:
        write_bad_aws_creds_file(bad_creds_node)

        def assert_test_results_are_not_ok():
            secrets = {s['id']: s for s in get_secrets(self.random_node())}
            error_descr = "test encryption error"
            assert_secret_test_results(secrets[aws_id], "error",
                                       expected_description=error_descr,
                                       expected_error_nodes=all_nodes)
            assert_secret_test_results(secrets[secret_id1], "error",
                                       expected_description=error_descr,
                                       expected_error_nodes=all_nodes)
            # Still success, because it's not encrypted with the bad key
            assert_secret_test_results(secrets[secret_id2], "ok",
                                       expected_description="Test passed",
                                       expected_success_nodes=all_nodes)

        testlib.poll_for_condition(
            assert_test_results_are_not_ok,
            sleep_time=1, attempts=50, retry_on_assert=True, verbose=True)


# Set master password and restart the cluster
# Testing that we can decrypt deks when master password is set
# (testing all combinations of master_password and encryption here)
class NativeEncryptionNodeRestartTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes = 1,
                                           edition='Enterprise',
                                           buckets=[],
                                           balanced=True,
                                           test_generated_cluster=True,
                                           num_vbuckets=16)

    def setup(self):
        pass

    def teardown(self):
        set_log_encryption(self.cluster, 'disabled', -1)
        set_cfg_encryption(self.cluster, 'nodeSecretManager', -1)
        change_password(self.node(), password='')

    @tag(Tag.LowUrgency)
    def encryption_on_with_master_password_set_test(self):
        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        set_cfg_encryption(self.cluster, 'nodeSecretManager', -1)
        password = change_password(self.node())
        self.cluster.restart_all_nodes(master_passwords={0: password})

    @tag(Tag.LowUrgency)
    def encryption_on_master_password_is_not_set_test(self):
        set_log_encryption(self.cluster, 'nodeSecretManager', -1)
        set_cfg_encryption(self.cluster, 'nodeSecretManager', -1)
        change_password(self.node(), password='')
        self.cluster.restart_all_nodes()

    @tag(Tag.LowUrgency)
    def encryption_off_master_password_is_set_test(self):
        set_log_encryption(self.cluster, 'disabled', -1)
        set_cfg_encryption(self.cluster, 'disabled', -1)
        password = change_password(self.node())
        self.cluster.restart_all_nodes(master_passwords={0: password})

    @tag(Tag.LowUrgency)
    def encryption_off_master_password_is_not_set_test(self):
        set_log_encryption(self.cluster, 'disabled', -1)
        set_cfg_encryption(self.cluster, 'disabled', -1)
        change_password(self.node(), password='')
        self.cluster.restart_all_nodes()

    def node(self):
        return self.cluster.connected_nodes[0]

# The KMIP test is run in a separate test set because we want to run it on just
# a single node to improve test stability. There is a known issue with the
# pykmip test serverer that it can lock up if it gets a number of requests in
# a given period of time. Each node generates requests to pykmip server, so
# running it on a single node will minimize concurrent requests to the pykmip
# server and helpfully reduce/eliminate the chance of the test failing due to
# the pykmip server locking up.
class NativeEncryptionWithKmipServerTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes = 1,
                                           edition='Enterprise',
                                           buckets=[],
                                           balanced=True,
                                           test_generated_cluster=True,
                                           num_vbuckets=16)

    def setup(self):
        self.bucket_name = testlib.random_str(8)
        set_min_timer_interval(self.cluster, min_timer_interval)
        set_dek_info_update_interval(self.cluster, dek_info_update_interval)
        set_min_dek_gc_interval(self.cluster, min_dek_gc_interval)
        set_dek_counters_renew_interval(self.cluster,
                                        dek_counters_renew_interval)

    def teardown(self):
        set_min_timer_interval(self.cluster, None)
        set_dek_info_update_interval(self.cluster, None)
        set_min_dek_gc_interval(self.cluster, None)
        set_dek_counters_renew_interval(self.cluster, None)


    def test_teardown(self):
        self.cluster.delete_bucket(self.bucket_name)
        for s in get_secrets(self.cluster):
            delete_secret(self.cluster, s['id'])

    def random_node(self):
        return random.choice(self.cluster.connected_nodes)

    def kmip_key_test(self):
        with setup_kmip_server(self.cluster):
            # There is no such secret in the kmip server, so it should fail
            # bucket creation
            invalidKmip = kmip_secret('Kmip1', 99999)
            invalid_secret_id = create_secret(self.random_node(), invalidKmip)
            self.cluster.create_bucket(
                {'name': self.bucket_name,
                 'ramQuota': 100,
                 'encryptionAtRestKeyId': invalid_secret_id},
                expected_code=400)

            # There is such secret in the kmip server, so it should succeed in
            # creating a bucket with that secret and the dek file should get
            # encrypted with the KMIP key
            validKmip = kmip_secret('Kmip2', 1)
            valid_secret_id = create_secret(self.random_node(), validKmip)
            self.cluster.create_bucket(
                {'name': self.bucket_name,
                 'ramQuota': 100,
                 'encryptionAtRestKeyId': valid_secret_id})
            bucket_uuid = self.cluster.get_bucket_uuid(self.bucket_name)
            kek1_id = get_kek_id(self.random_node(), valid_secret_id)
            poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                          verify_key_count=1,
                                          verify_encryption_kek=kek1_id)

            # We rotate the underlying KMIP key for the secret bucket is
            # encrypted with and the bucket DEK should get encrypted by the new
            # KMIP key
            validKmip = kmip_secret('Kmip2', 5)
            valid_secret_id = update_secret(self.random_node(),
                                            valid_secret_id, validKmip)
            kek2_id = get_kek_id(self.random_node(), valid_secret_id)
            poll_verify_bucket_deks_files(self.cluster, bucket_uuid,
                                          verify_key_count=1,
                                          verify_encryption_kek=kek2_id)

class NativeEncryptionPermissionsTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition='Enterprise',
                                           num_vbuckets=16)

    def setup(self):
        set_cfg_encryption(self.cluster, 'nodeSecretManager', -1)
        self.bucket_name = testlib.random_str(8)
        self.password = testlib.random_str(8)
        bucket_props = {'name': self.bucket_name,
                        'ramQuota': 100,
                        'encryptionAtRestKeyId': -1}
        self.cluster.create_bucket(bucket_props, sync=True)
        self.pre_existing_ids = [s['id'] for s in get_secrets(self.cluster)]

        create_user = lambda n, r: put_user(self.cluster, 'local', n,
                                            password=self.password, roles=r)

        admin = 'admin_' + testlib.random_str(4)
        create_user(admin, 'admin')

        ro_security_admin = 'ro_security_admin_' + testlib.random_str(4)
        create_user(ro_security_admin, 'ro_security_admin')

        bucket_creator = 'bucket_creator_' + testlib.random_str(4)
        create_user(bucket_creator, 'cluster_admin')

        bucket_admin = 'bucket_admin_' + testlib.random_str(4)
        create_user(bucket_admin, f'bucket_admin[{self.bucket_name}]')

        bucket_reader = 'bucket_reader_' + testlib.random_str(4)
        create_user(bucket_reader, f'data_reader[{self.bucket_name}]')

        no_priv_user = 'no_priv_user_' + testlib.random_str(4)
        create_user(no_priv_user, f'external_stats_reader')

        # Usages:
        cfg = 'config-encryption'
        sec = 'KEK-encryption'
        all_b = 'bucket-encryption'
        b = f'bucket-encryption-{self.bucket_name}'

        self.writing = \
            {admin:          {cfg: True,  sec: True,  all_b: True,  b: True},
             ro_security_admin: {cfg: False, sec: False, all_b: False,
                                 b: False},
             bucket_creator: {cfg: False, sec: False, all_b: True,  b: True},
             bucket_admin:   {cfg: False, sec: False, all_b: False, b: True} ,
             bucket_reader:  {cfg: False, sec: False, all_b: False, b: False},
             no_priv_user:   {cfg: False, sec: False, all_b: False, b: False}}

        self.reading = \
            {admin:          {cfg: True,  sec: True,  all_b: True,  b: True},
             ro_security_admin: {cfg: True,  sec: True,  all_b: True,  b: True},
             bucket_creator: {cfg: False, sec: False, all_b: True,  b: True},
             bucket_admin:   {cfg: False, sec: False, all_b: True,  b: True} ,
             bucket_reader:  {cfg: False, sec: False, all_b: True,  b: True},
             no_priv_user:   {cfg: False, sec: False, all_b: False, b: False}}

    def teardown(self):
        self.cluster.delete_bucket(self.bucket_name)
        for u in self.writing:
            testlib.ensure_deleted(
                self.cluster, f'/settings/rbac/users/local/{u}')
        set_cfg_encryption(self.cluster, 'disabled', -1)

    def test_teardown(self):
        for s in get_secrets(self.cluster):
            if s['id'] not in self.pre_existing_ids:
                delete_secret(self.cluster, s['id'])

    def secrets_test_gen(self):
        tests = {}
        for user in self.writing:
            for usage in self.writing[user]:
                tests[f'create_secret({user}, {usage})'] = \
                    lambda s, n=user, u=usage: s.create_secret_test_(n, [u])
                tests[f'update_secret({user}, {usage})'] = \
                    lambda s, n=user, u=usage: s.update_secret_test_(n, [u])
                tests[f'read_secret({user}, {usage})'] = \
                    lambda s, n=user, u=usage: s.read_secret_test_(n, u)
        return tests

    def random_usage_sublist_test_gen(self):
        tests = {}
        for user in self.writing:
            all_usages = list(self.writing[user].keys())
            subset_size = random.randint(2, len(all_usages))
            usages = random.sample(all_usages, subset_size)
            tests[f'rand_create_secret({user}, {usages})'] = \
                lambda s, n=user, u=usages: s.create_secret_test_(n, u)
            tests[f'rand_update_secret({user}, {usages})'] = \
                lambda s, n=user, u=usages: s.update_secret_test_(n, u)
        return tests

    def create_secret_test_(self, username, usages):
        creds = (username, self.password)
        secret = cb_managed_secret(usage=usages)
        create_secret(self.cluster, secret, auth=creds,
                      expected_code=self.expected_write_res(username, usages))

    def update_secret_test_(self, username, usages):
        creds = (username, self.password)
        secret = cb_managed_secret(usage=usages)
        secret_id = create_secret(self.cluster, secret) # note: admin creates it
        secret['name'] = secret['name'] + ' foo'
        update_secret(self.cluster, secret_id, secret, auth=creds,
                      expected_code=self.expected_write_res(username, usages))
        delete_secret(self.cluster, secret_id, auth=creds,
                      expected_code=self.expected_write_res(username, usages))

    def read_secret_test_(self, name, usage):
        creds = (name, self.password)
        forbidden = [u for u in self.reading[name] if not self.reading[name][u]]
        secret_with_usage = cb_managed_secret(usage=[usage] + forbidden)
        # Note: admin creates this secret
        secret_with_usage_id = create_secret(self.cluster, secret_with_usage)

        # All usages but one are unreadable, so readability of this secret
        # depends on readability of 'usage'
        should_be_readable = self.reading[name][usage]
        expected_code = 200 if should_be_readable else 404
        get_secret(self.cluster, secret_with_usage_id, auth=creds,
                   expected_code=expected_code)

        secrets = get_secrets(self.cluster, auth=creds)
        filtered = [s for s in secrets if s['id'] == secret_with_usage_id]
        if should_be_readable:
            assert len(filtered) == 1, f'unexpected secrets: {secrets}'
        else:
            assert len(filtered) == 0, f'unexpected secrets: {secrets}'

        if len(forbidden) > 0:
            not_readable_secret = cb_managed_secret(usage=forbidden)
            # Note: admin creates this secret
            not_readable_secret_id = create_secret(self.cluster,
                                                   not_readable_secret)
            get_secret(self.cluster, not_readable_secret_id, auth=creds,
                       expected_code=404)
            secrets = get_secrets(self.cluster, auth=creds)
            filtered = [s for s in secrets if s['id'] == not_readable_secret]
            assert filtered == [], f'unexpected secrets: {secrets}'

    def expected_write_res(self, username, usages):
        expected_res = [self.writing[username][u] for u in usages]
        return 200 if all(expected_res) else 403


def get_key_list(node, kind_as_str):
    res = testlib.diag_eval(
            node,
            '{ok, {_, List, _}} = cb_deks:list(' + kind_as_str + '), ' \
            'lists:flatten(lists:join(",", [binary_to_list(K) || K <- List])).')
    if res.text == "[]":
        return []
    re_res = re.search('"(.*)"', res.text)
    assert re_res is not None, f'unexpected diag eval return: {res.text}'
    keys_str = re_res.group(1)
    return keys_str.split(',')


def run_dump_keys(node, *args, **kwargs):
    return run_dump_key_utility(node, 'dump-keys', *args, **kwargs)


def run_dump_bucket_deks(node, *args, **kwargs):
    return run_dump_key_utility(node, 'dump-bucket-deks', *args, **kwargs)


def run_dump_key_utility(node, name, args, expected_return_code=0):
    data_dir = node.data_path()
    gosecrets_cfg_path = os.path.join(data_dir, 'config', 'gosecrets.cfg')
    utility_path = os.path.join(testlib.get_scripts_dir(), name)
    pylib_path = testlib.get_pylib_dir()
    all_args = ['--config', gosecrets_cfg_path] + args
    env = {'PYTHONPATH': pylib_path, "PATH": os.environ['PATH']}
    print(f'running {utility_path} with {all_args}')
    r = subprocess.run([utility_path] + all_args, capture_output=True, env=env)
    assert r.returncode == expected_return_code, \
           f'{name} returned {r.returncode}\n' \
           f'stdout: {r.stdout.decode()}\n' \
           f'stderr: {r.stderr.decode()}'
    print(f'{name} reponse: {r.stdout.decode()}')
    if expected_return_code == 0:
        return json.loads(r.stdout)

    return r.stderr.decode()


def verify_key_presense_in_dump_key_response(response, good_ids, unknown_ids):
    for k in good_ids:
        testlib.assert_in(k, response)
        testlib.assert_in('result', response[k])
        testlib.assert_eq(response[k]['result'], 'raw-aes-gcm')
        testlib.assert_in('key', response[k]['response'])
        key = base64.b64decode(response[k]['response']['key'])
        testlib.assert_eq(len(key), 32)

    for k in unknown_ids:
        testlib.assert_in(k, response)
        testlib.assert_in('result', response[k])
        testlib.assert_eq(response[k]['result'], 'error')


def set_cfg_encryption(cluster, *args, **kwargs):
    return set_comp_encryption(cluster, 'config', *args, **kwargs)


def set_log_encryption(cluster, *args, **kwargs):
    return set_comp_encryption(cluster, 'log', *args, **kwargs)


def set_audit_encryption(cluster, *args, **kwargs):
    return set_comp_encryption(cluster, 'audit', *args, **kwargs)


def set_comp_encryption(cluster, component, mode, secret,
                        dek_lifetime=None, dek_rotation=None,
                        skip_encryption_key_test=False,
                        expected_code=200):
    data = {'encryptionMethod': mode,
            'encryptionKeyId': secret,
            'skipEncryptionKeyTest': skip_encryption_key_test}
    if dek_lifetime is not None:
        data['dekLifetime'] = dek_lifetime
    if dek_rotation is not None:
        data['dekRotationInterval'] = dek_rotation
    res = testlib.post_succ(cluster,
                      f'/settings/security/encryptionAtRest/{component}',
                      json=data,
                      expected_code=expected_code)
    if expected_code == 200:
        r = testlib.get_succ(cluster, '/settings/security/encryptionAtRest')
        r = r.json()
        testlib.assert_eq(r[component]['encryptionMethod'], mode)
        testlib.assert_eq(r[component]['encryptionKeyId'], secret)
        return None

    return res.json()['errors']


def cb_managed_secret(name=None,
                      usage=None,
                      auto_rotation=False, rotation_interval=7,
                      next_rotation_time=None,
                      encrypt_with='nodeSecretManager',
                      encrypt_secret_id=None):
    if usage is None:
        usage = ['bucket-encryption', 'KEK-encryption']
    if name is None:
        name = f'Test secret {testlib.random_str(5)}'
    optional = {}
    if encrypt_secret_id is not None:
        optional['encryptWithKeyId'] = encrypt_secret_id
    if next_rotation_time is not None:
        optional['nextRotationTime'] = next_rotation_time
    return {'name': name,
            'type': 'cb-server-managed-aes-key-256',
            'usage': usage,
            'data': {'autoRotation': auto_rotation,
                     'rotationIntervalInDays': rotation_interval,
                     'encryptWith': encrypt_with, **optional}}

# This secret does not actually go to aws when asked encrypt or decrypt data.
# All AWS secrets with special ARN=TEST_AWS_KEY_ARN simply encrypt data using
# dummy key.
def aws_test_secret(name=None, usage=None, good_arn=True, creds_file=None):
    if usage is None:
        usage = ['bucket-encryption', 'KEK-encryption']
    if name is None:
        name = f'Test secret {testlib.random_str(5)}'

    if good_arn:
        key_arn = 'TEST_AWS_KEY_ARN'
    else:
        key_arn = 'TEST_AWS_BAD_KEY_ARN'

    data = {'keyARN': key_arn}

    if creds_file is not None:
        data['credentialsFile'] = creds_file

    return {'name': name,
            'type': 'awskms-symmetric-key',
            'usage': usage,
            'data': data}

 # This secret does not actually go to GCP when asked encrypt or decrypt data.
# All GCP secrets with special keyResourceId=TEST_GCP_RESOURCE_ID simply
# encrypt data using dummy key.
def gcp_test_secret(name=None, usage=None, creds_file=None):
    if usage is None:
        usage = ['bucket-encryption',
                 'KEK-encryption',
                 'config-encryption',
                 'log-encryption',
                 'audit-encryption']

    if name is None:
        name = f'Test secret {testlib.random_str(5)}'

    data = {'keyResourceId': 'TEST_GCP_RESOURCE_ID'}

    if creds_file is not None:
        data['credentialsFile'] = creds_file

    return {'name': name,
            'type': 'gcpkms-symmetric-key',
            'usage': usage,
            'data': data}

def azure_test_secret(name=None, usage=None, key_url=None):
    if usage is None:
        usage = ['bucket-encryption',
                 'KEK-encryption',
                 'config-encryption',
                 'log-encryption',
                 'audit-encryption']

    if name is None:
        name = f'Test secret {testlib.random_str(5)}'

    data = {'keyURL': 'TEST_AZURE_KEY_URL',
            'encryptionAlgorithm': 'RSAOAEP256',
            'credentialsChain': 'environment, workloadIdentity,'
                                'managedIdentity, azureCli, azureDeveloperCli'}

    if key_url is not None:
        data['keyURL'] = key_url

    return {'name': name,
            'type': 'azurekms-key',
            'usage': usage,
            'data': data}

def hashi_test_secret(name = None, usage=None, key_url=None):
    if name is None:
        name = f'Test secret {testlib.random_str(5)}'

    return {
        'name': name,
        'type': 'hashikms-key',
        'usage': [
            'KEK-encryption',
            'bucket-encryption',
            'config-encryption',
            'log-encryption',
            'audit-encryption'
        ],
        'data':
        {
            'caSelection': 'skipServerCertVerification',
            'reqTimeoutMs': 5000,
            'encryptWith': 'nodeSecretManager',
            'encryptWithKeyId': -1,
            'keyPath': '/fake/test/path',
            'certPath': '/fake/test/path',
            'keyPassphrase': 'makeitso',
            'keyURL': 'TEST_HASHI_KEY_URL'
        }
    }


def write_good_aws_creds_file(node):
    path = aws_fake_creds_path(node)
    print(f'writing good aws creds file to {path}')
    with open(path, "w") as f:
        f.write("TEST_AWS_CREDS")
    return path

def write_bad_aws_creds_file(node):
    path = aws_fake_creds_path(node)
    print(f'writing bad aws creds file to {path}')
    with open(path, "w") as f:
        f.write("TEST_BAD_AWS_CREDS")
    return path

def delete_aws_fake_creds_file(node):
    path = aws_fake_creds_path(node)
    print(f'deleting aws creds file {path}')
    if os.path.exists(path):
        os.remove(path)

def aws_fake_creds_path(node):
    return os.path.join(node.tmp_path(), "tmp-fake-test-creds.tmp")


def kmip_secret(name, kmip_id, is_test_secret=False):
    if is_test_secret:
        key_path = 'TEST_KMIP_KEY_PATH'
        cert_path = '/fake/test/path'
    else:
        key_path = os.path.join(testlib.get_resources_dir(),
                                'pykmip/kmip_pkcs8.key')
        cert_path = os.path.join(testlib.get_resources_dir(),
                                 'pykmip/localhost.crt')
    return {
        'name': f'{name}',
        'type': 'kmip-aes-key-256',
        'usage': [
            'KEK-encryption',
            'bucket-encryption',
            'config-encryption',
            'log-encryption',
            'audit-encryption'
        ],
        'data':
        {
            'caSelection': 'skipServerCertVerification',
            'reqTimeoutMs': 5000,
            'encryptionApproach': 'useEncryptDecrypt',
            'encryptWith': 'nodeSecretManager',
            'encryptWithKeyId': -1,
            'activeKey': {'kmipId': f'{kmip_id}'},
            'keyPath': f'{key_path}',
            'certPath': f'{cert_path}',
            'keyPassphrase': 'makeitso',
            'host': 'localhost',
            'port': kmip_port
        }
    }


def get_secret(cluster, secret_id, expected_code=200, auth=None):
    if auth is None:
        auth = cluster.auth
    r = testlib.get_succ(cluster, f'/settings/encryptionKeys/{secret_id}',
                         expected_code=expected_code, auth=auth)
    if expected_code == 200:
        return r.json()
    return r.text


def get_historical_keys(cluster, secret_id):
    s = get_secret(cluster, secret_id)
    assert s['type'] == 'cb-server-managed-aes-key-256', \
           'not supported secret type'
    l = s['data']['keys']
    l.sort(key=lambda k: k['creationDateTime'], reverse=True)
    assert l[0]['active'] == True, 'active key is not the first one'
    return l


def get_secrets(cluster, auth=None):
    if auth is None:
        auth = cluster.auth
    return testlib.get_succ(cluster, '/settings/encryptionKeys',
                            auth=auth).json()

def delete_all_secrets(cluster):
    for s in get_secrets(cluster):
        delete_secret(cluster, s['id'])

def test_existing_secret(cluster, secret_id, expected_code=200):
    r = testlib.post_succ(cluster, f'/settings/encryptionKeys/{secret_id}/test',
                          expected_code=expected_code)

    if expected_code == 200:
        return None

    r = r.json()
    return r['errors']

def test_create_secret(cluster, json, expected_code=200):
    r = testlib.post_succ(cluster, '/settings/encryptionKeys/test', json=json,
                          expected_code=expected_code)


    if expected_code == 200:
        return None

    r = r.json()
    return r['errors']


def create_secret(cluster, json, expected_code=200, auth=None):
    if auth is None:
        auth = cluster.auth
    r = testlib.post_succ(cluster, '/settings/encryptionKeys', json=json,
                          expected_code=expected_code, auth=auth)


    if expected_code == 200:
        r = r.json()
        return r['id']
    elif expected_code == 403 or expected_code == 503:
        return r.text
    else:
        r = r.json()
        return r['errors']


def test_update_secret(cluster, secret_id, json, expected_code=200):
    r = testlib.put_succ(cluster, f'/settings/encryptionKeys/{secret_id}/test',
                         json=json,
                         expected_code=expected_code)

    if expected_code == 200:
        return None
    elif expected_code == 404:
        return r.text

    r = r.json()
    return r['errors']


def update_secret(cluster, secret_id, json, expected_code=200, auth=None):
    if auth is None:
        auth = cluster.auth
    r = testlib.put_succ(cluster, f'/settings/encryptionKeys/{secret_id}',
                         json=json,
                         expected_code=expected_code, auth=auth)
    if expected_code == 200:
        r = r.json()
        return r['id']
    elif expected_code == 403 or expected_code == 404 or expected_code == 503:
        return r.text
    else:
        r = r.json()
        return r['errors']


def delete_secret(cluster, secret_id, expected_code=200, auth=None):
    if auth is None:
        auth = cluster.auth
    r = testlib.delete(cluster, f'/settings/encryptionKeys/{secret_id}',
                       expected_code=expected_code, auth=auth)

    if expected_code == 200:
        return None

    return r.text


def delete_historical_key(cluster, secret_id, hist_key_id, expected_code=200):
    r = testlib.delete(
        cluster,
        f'/settings/encryptionKeys/{secret_id}/historicalKeys/{hist_key_id}',
        expected_code=expected_code)

    if expected_code == 200:
        return None
    elif expected_code == 404:
        return None
    else:
        return r.json()['errors']


def verify_kek_files(cluster, secret, verify_key_count=1, **kwargs):
    for node in cluster.connected_nodes:
        if secret['type'] == 'cb-server-managed-aes-key-256':
            if verify_key_count is not None:
                count = len(secret['data']['keys'])
                assert count == verify_key_count, \
                       f'kek count is unexpected: {count} ' \
                       f'(expected: {verify_key_count})'
            key_ids = [key['id'] for key in secret['data']['keys']]
        elif secret['type'] == 'awskms-symmetric-key' or \
             secret['type'] == 'gcpkms-symmetric-key' or \
             secret['type'] == 'azurekms-key' or \
             secret['type'] == 'hashikms-key':
            key_ids = [key['id'] for key in secret['data']['storedKeyIds']]
        else:
            assert False, f'unexpected secret type: {secret["type"]}'

        path = Path(node.data_path()) / 'config' / 'keks'
        for key_id in key_ids:
            verify_key_file_by_id(path, key_id, **kwargs)


def poll_verify_kek_files(*args, **kwargs):
    testlib.poll_for_condition(
      lambda: verify_kek_files(*args, **kwargs),
      sleep_time=0.3, attempts=50, retry_on_assert=True, verbose=True)


def verify_bucket_deks_files(cluster, bucket_uuid, **kwargs):
    deks_path = Path() / 'data' / bucket_uuid / 'deks'

    def is_kv_node(node):
        return Service.KV in node.get_services()

    def is_not_kv_node(node):
        return not is_kv_node(node)

    verify_dek_files(cluster, deks_path, node_filter=is_kv_node, **kwargs)
    verify_dek_files(cluster, deks_path, node_filter=is_not_kv_node,
                     verify_key_count=0)


def verify_dek_files(cluster, relative_path, verify_key_count=1,
                     node_filter=None, **kwargs):
    if isinstance(cluster, list):
        nodes = cluster
    else:
        nodes = cluster.connected_nodes
    for node in nodes:
        deks_path = Path(node.data_path()) / relative_path
        print(f'Checking deks in {deks_path} (cheking ' \
              f'verify_key_count={verify_key_count} and also {kwargs})... ')
        if node_filter is not None:
            if not node_filter(node):
                print(f'Skipping check of {node}')
                continue
        if not deks_path.exists():
            if verify_key_count == 0:
                return
            else:
                assert False, f'directory {deks_path} doesn\'t exist'
        checked_ids = {}
        for path in deks_path.iterdir():
            print(f'Found file {path}')
            key_id = parse_key_file_name(path.name)
            if key_id is not None:
                if key_id in checked_ids:
                    # It is possible to have two files with the same key id
                    # for short period of time,
                    # because new version is written first, and then old version
                    # gets removed.
                    print(f'Skipping duplicate key id {key_id}')
                    continue
                checked_ids[key_id] = True
                print(f'Verifying dek {key_id}')
                verify_key_file_by_id(path.parent, key_id, **kwargs)
            else:
                print(f'Skipping file {path} (doesn\'t seem to be a key file)')

        if verify_key_count is not None:
            c = len(checked_ids)
            print(f'dek count at {node}: {c}')
            if callable(verify_key_count):
                assert verify_key_count(c), f'dek count is unexpected: {c}'
            else:
                assert c == verify_key_count, f'dek count is unexpected: {c} ' \
                                              f'(expected: {verify_key_count})'

def verify_bucket_dek_info(node, bucket, missing=False, **kwargs):
    r = testlib.get(node, f'/pools/default/buckets/{bucket}')
    r = r.json()
    nodes = r['nodes']
    info = None
    for node_info in nodes:
        if 'thisNode' in node_info and node_info['thisNode']:
            info = node_info['bucketEncryptionAtRestInfo']
            break
    if missing:
        assert info is None, \
               f'bucketEncryptionAtRestInfo for bucket {bucket} ' \
               f'is present on node {node}, all nodes info {nodes}'
    else:
        assert info is not None, \
               f'bucketEncryptionAtRestInfo for bucket {bucket} ' \
               f'is not present on node {node}, all nodes info {nodes}'
    print(f'bucketEncryptionAtRestInfo for {bucket}: {info}')
    verify_dek_info(info, **kwargs)


def verify_node_dek_info(node, data_type, **kwargs):
    r = testlib.get_succ(node, '/nodes/self')
    r = r.json()
    info = r['encryptionAtRestInfo'][data_type]
    print(f'encryptionAtRestInfo for {data_type}: {info}')
    verify_dek_info(info, **kwargs)


def verify_dek_info(info, data_statuses=None, dek_number=None,
                    oldest_dek_time=None):
    if data_statuses is not None:
        assert 'dataStatus' in info, 'data status is not present in ' \
                                     'encryptionAtRestInfo'
        assert info['dataStatus'] in data_statuses, \
               f'data status is unexpected: {info["dataStatus"]}'
    if dek_number is not None:
        assert 'dekNumber' in info, 'dek number is not present in ' \
                                    'encryptionAtRestInfo'
        assert info['dekNumber'] == dek_number, \
               f'dek number is unexpected: {info["dekNumber"]}'
    if oldest_dek_time is not None:
        assert 'oldestDekCreationDatetime' in info, \
               'oldestDekCreationDatetime is not present in ' \
               'encryptionAtRestInfo'
        t = parse_iso8601(info['oldestDekCreationDatetime'])
        assert t >= oldest_dek_time, \
               f'dek time is unexpected: {t} (expected: {oldest_dek_time})'


def poll_verify_cluster_dek_info(cluster, *args, **kwargs):
    for node in cluster.connected_nodes:
        poll_verify_node_dek_info(node, *args, **kwargs)


def poll_verify_node_dek_info(*args, **kwargs):
    testlib.poll_for_condition(
      lambda: verify_node_dek_info(*args, **kwargs),
      sleep_time=1, attempts=120, retry_on_assert=True, verbose=True)


def poll_verify_cluster_bucket_dek_info(cluster, *args, **kwargs):
    for node in kv_nodes(cluster):
        poll_verify_node_bucket_dek_info(node, *args, **kwargs)


def poll_verify_node_bucket_dek_info(*args, **kwargs):
    testlib.poll_for_condition(
      lambda: verify_bucket_dek_info(*args, **kwargs),
      sleep_time=1, attempts=120, retry_on_assert=True, verbose=True)


def parse_key_file_name(base_name):
    tokens = base_name.split(".key.")
    if len(tokens) != 2:
        return None
    try:
        vsn = int(tokens[1])
    except:
        return None

    key_id = tokens[0]
    if not is_valid_key_id(key_id):
        return None
    return key_id


def is_valid_key_id(name):
    # Just testing that it is a uuid. This solution is not perfect but helps
    # most of the time (e.g. when some temporary file is found in dek directory)
    try:
        return name == str(uuid.UUID(name))
    except ValueError:
        return False


def poll_verify_bucket_deks_files(*args, **kwargs):
    testlib.poll_for_condition(
      lambda: verify_bucket_deks_files(*args, **kwargs),
      sleep_time=1, attempts=15 + min_timer_interval,
      retry_on_assert=True, verbose=True)


def poll_verify_bucket_deks_and_collect_ids(*args, **kwargs):
    current_dek_ids = []

    def collect_ids(dek_id):
        current_dek_ids.append(dek_id)
        return True

    def verify():
        current_dek_ids.clear()
        verify_bucket_deks_files(verify_id=collect_ids, *args, **kwargs)

    testlib.poll_for_condition(
      verify, sleep_time=0.3, attempts=50, retry_on_assert=True, verbose=True)

    return current_dek_ids


def poll_verify_dek_files(*args, **kwargs):
    testlib.poll_for_condition(
      lambda: verify_dek_files(*args, **kwargs),
      sleep_time=0.3, attempts=50, retry_on_assert=True, verbose=True)


def verify_key_file_by_id(dir_path, key_id, verify_missing=False, **kwargs):
    wildcard = "./" + key_id + '.key.*'

    if verify_missing:
        files = list(dir_path.glob(wildcard))
        assert len(files) == 0, f'key files exists: {files}'
        return True

    # Any given key file can be rewritten with a newer version at any moment,
    # so we can't really expect that:
    # 1. there is exactly one version of the key file
    # 2. the file still exists when we read it
    # We should simply retry if it happens.
    def verify():
        files = list(dir_path.glob(wildcard))
        if len(files) != 1:
            # We can have 2 files for a given key id for a short period of time,
            # because new version is written first, and then old version
            # gets removed.
            print(f'Found {len(files)} files for {key_id}')
            return False
        try:
            content = json.loads(files[0].read_bytes())
        except:
            print(f'Failed to load content of {files[0]}')
            return False
        return verify_key_file_content(content, key_id, **kwargs)

    testlib.poll_for_condition(
      verify, sleep_time=0.3, attempts=50, verbose=True)


def verify_key_file_content(content, key_id, verify_encryption_kek=None,
                            verify_creation_time=None,
                            verify_key_type=None,
                            verify_id=None):
    if verify_encryption_kek is not None:
        has_kek = content['keyData']['encryptionKeyName']
        assert has_kek == verify_encryption_kek, \
               f'key is encrypted by wrong kek {has_kek} ' \
               f'(expected: {verify_encryption_kek})'
    if verify_creation_time is not None:
        ct = content['keyData']['creationTime']
        assert verify_creation_time(parse_iso8601(ct)), \
               f'Unexpected key creation time: {ct} ' \
               f'(cur time: {datetime.now(timezone.utc)})'
    if verify_id is not None:
        assert verify_id(key_id), f'unexpected key id: {key_id}'
    expected_type = 'raw-aes-gcm' if verify_key_type is None \
                                  else verify_key_type
    assert content['type'] == expected_type, \
           f'unexpected key type: {content["type"]} (expected: {expected_type})'
    return True


def get_kek_id(cluster, secret_id):
    r = get_secret(cluster, secret_id)
    if r['type'] == 'cb-server-managed-aes-key-256':
        for k in r['data']['keys']:
            if k['active']:
                return k['id']
    if r['type'] == 'awskms-symmetric-key':
        return r['data']['storedKeyIds'][0]['id']
    if r['type'] == 'kmip-aes-key-256':
        return r['data']['activeKey']['id']

    return None


def get_all_kek_ids(cluster, secret_id):
    r = get_secret(cluster, secret_id)
    if r['type'] == 'cb-server-managed-aes-key-256':
        return [k['id'] for k in r['data']['keys']]
    return None


def encrypt_with_key(cluster, kek_id, string):
    res = testlib.diag_eval(
            cluster,
            '{ok, B} = ' f'encryption_service:encrypt_key(<<"{string}">>, '
                                                         '<<"ad">>,'
                                                        f'<<"{kek_id}">>),'
            'R = base64:encode_to_string(B),'
            '"Success:" ++ R.')
    search_res = re.search('"Success:(.*)"', res.text)
    assert search_res is not None, f'unexpected decrypt result: {res.text}'
    return search_res.group(1)


def check_decrypt(cluster, kek_id, encrypted_data, expected_data):
    decrypted_data = decrypt_with_key(cluster, kek_id, encrypted_data)
    assert decrypted_data == expected_data, \
           'decrypted data doesn\'t match the original. ' \
           f'Expected: {expected_data}, Got: {decrypted_data}'


def decrypt_with_key(cluster, kek_id, b64data):
    res = testlib.diag_eval(
            cluster,
            f'B = base64:decode("{b64data}"),'
            '{ok, R} = encryption_service:decrypt_key(B, <<"ad">>, '
                                                    f'<<"{kek_id}">>),'
            '"Success:" ++ binary_to_list(R).')
    search_res = re.search('"Success:(.*)"', res.text)
    assert search_res is not None, f'unexpected decrypt result: {res.text}'
    return search_res.group(1)


def rotate_secret(cluster, secret_id):
    testlib.post_succ(cluster, f'/controller/rotateEncryptionKey/{secret_id}')


def poll_verify_deks_and_collect_ids(*args, **kwargs):
    current_dek_ids = []

    def collect_ids(dek_id):
        current_dek_ids.append(dek_id)
        return True

    def verify():
        current_dek_ids = []
        verify_dek_files(verify_id=collect_ids, *args, **kwargs)

    testlib.poll_for_condition(
      verify, sleep_time=0.3, attempts=50, retry_on_assert=True, verbose=True)

    print(f'key list extracted: {current_dek_ids}')

    return current_dek_ids


def set_cfg_dek_limit(cluster, n):
    set_dek_limit(cluster, 'configDek', n)


def set_log_dek_limit(cluster, n):
    set_dek_limit(cluster, 'logDek', n)


def set_all_bucket_dek_limit(cluster, n):
    set_dek_limit(cluster, 'bucketDek', n)


def set_bucket_dek_limit(cluster, bucket, n):
    uuid = cluster.get_bucket_uuid(bucket, none_if_not_found=True)
    if uuid is None and n is None:
        # The bucket doesn't exist, no point in resetting the limit
        return
    key = '{bucketDek, <<"' + uuid + '">>}'
    set_dek_limit(cluster, key, n)


def set_dek_limit(cluster, kind, n):
    key = '{cb_cluster_secrets, {max_dek_num, ' + kind + '}}'
    set_ns_config_value(cluster, key, n)


def set_min_timer_interval(cluster, n):
    set_ns_config_value(cluster, '{cb_cluster_secrets, min_timer_interval}', n)


def set_dek_info_update_interval(cluster, n):
    set_ns_config_value(cluster,
                        '{cb_cluster_secrets, dek_info_update_interval}',
                        n)


def set_min_dek_gc_interval(cluster, n):
    set_ns_config_value(cluster, '{cb_cluster_secrets, min_dek_gc_interval}', n)


def set_remove_hist_keys_interval(cluster, n):
    set_ns_config_value(cluster,
                        '{cb_cluster_secrets, remove_historical_keys_interval}',
                        n)

def set_dek_counters_renew_interval(cluster, n):
    set_ns_config_value(cluster,
                        '{cb_cluster_secrets, dek_counters_renew_interval}',
                        n)


def set_ns_config_value(cluster, key_str, value):
    if value is None:
        testlib.diag_eval(cluster, f'ns_config:delete({key_str}).')
    else:
        testlib.diag_eval(cluster, f'ns_config:set({key_str}, {value}).')


def parse_iso8601(s):
    # can't use datetime.fromisoformat because it doesn't parse UTC datetimes
    # before version 3.11
    return dateutil.parser.parse(s)

def drop_deks(cluster, data_type):
    r = testlib.post_succ(cluster,
                          f'/controller/dropEncryptionAtRestDeks/{data_type}')
    return parse_iso8601(r.json()['dropKeysDate'])

def drop_bucket_keys(cluster, bucket):
    r = testlib.post_succ(
          cluster,
          f'/controller/dropEncryptionAtRestDeks/bucket/{bucket}')
    return parse_iso8601(r.json()['dropKeysDate'])

def force_encryption(cluster, data_type):
    testlib.post_succ(cluster,
                      f'/controller/forceEncryptionAtRest/{data_type}')

def force_bucket_encryption(cluster, bucket):
    testlib.post_succ(cluster,
                      f'/controller/forceEncryptionAtRest/bucket/{bucket}')

def poll_verify_and_get_mcd_deks_in_use(*args, **kwargs):
    return testlib.poll_for_condition(
             lambda: verify_and_get_mcd_deks_in_use(*args, **kwargs),
             sleep_time=0.3, attempts=50, retry_on_assert=True, verbose=True)

def verify_and_get_mcd_deks_in_use(cluster, bucket, verify_key_count=None):
    res = []
    for node in cluster.connected_nodes:
        if Service.KV not in node.get_services():
            continue
        deks = get_bucket_deks_in_use(cluster, node, bucket)
        if verify_key_count is not None:
            assert len(deks) == verify_key_count, \
                   f'unexpected number of deks, expected: {verify_key_count}, '\
                   f'got: {deks}'
        res.extend(deks)

    return res


def get_bucket_deks_in_use(cluster, node, bucket):
    bucket_uuid = cluster.get_bucket_uuid(bucket)
    r = testlib.diag_eval(node,
                          '{ok, L} = ns_memcached:get_dek_ids_in_use(<<"' +
                          bucket_uuid + '">>), {json, L}.')
    print(f'ns_memcached:get_dek_ids_in_use("{bucket}") for {node} '
          f'returned {r.text}')
    ids = json.loads(r.text)
    return [dek_id for dek_id in ids if dek_id != 'null_dek_id']


def assert_bucket_deks_have_changed(cluster, bucket, min_time=None,
                                    old_dek_ids=None, verify_key_count=None,
                                    **kwargs):
    bucket_uuid = cluster.get_bucket_uuid(bucket)
    # checking that bucket deks have changed
    poll_verify_bucket_deks_files(
      cluster,
      bucket_uuid,
      verify_key_count=verify_key_count,
      verify_creation_time=lambda ct: ct >= min_time,
      verify_id=lambda n: n not in old_dek_ids,
      **kwargs)

    # checking that bucket actually uses a dek
    dek_ids = poll_verify_and_get_mcd_deks_in_use(
                cluster, bucket, verify_key_count=verify_key_count)
    assert len(dek_ids) > 0, f'bucket deks are empty: {dek_ids}'
    # ... and checking that on disk we have the same dek and nothing else
    poll_verify_bucket_deks_files(cluster,
                                  bucket_uuid,
                                  verify_key_count=verify_key_count,
                                  verify_id=lambda n: n in dek_ids,
                                  **kwargs)

    return dek_ids

def assert_logs_encrypted(cluster, *args, **kwargs):
    assert_logs_encryption(cluster, True, *args, **kwargs)


def assert_logs_unencrypted(cluster, *args, **kwargs):
    assert_logs_encryption(cluster, False, *args, **kwargs)


def assert_logs_encryption(cluster, must_be_encrypted, files_to_check,
                           check_suffixes=None):
    if check_suffixes is None:
        check_suffixes = [""]
    for n in cluster.connected_nodes:
        for file_to_check in files_to_check:
            for suffix in check_suffixes:
                found = False
                for f in Path(n.logs_path()).glob(file_to_check + suffix):
                    found = True
                    if must_be_encrypted:
                        assert_file_encrypted(f)
                    else:
                        assert_file_unencrypted(f)
                assert found, f'file {file_to_check + suffix} not found'
    return True


def get_log_file_dek_ids(cluster, file):
    dek_ids = []
    for n in cluster.connected_nodes:
        dek_ids.append(get_file_dek_id(Path(n.logs_path()) / file))
    return sorted(dek_ids)


def assert_log_file_dek_ids(cluster, file, suffix, assert_dek_ids_func):
    for n in cluster.connected_nodes:
        found = False
        for f in Path(n.logs_path()).glob(file + suffix):
            found = True
            assert_dek_ids_func(get_file_dek_id(f))
        assert found, f'file {file + suffix} not found'


def get_file_dek_id(path):
    key_id_start = 28
    key_id_len = 36
    assert_file_encrypted(path)
    with open(path, 'rb') as f:
        head = f.read(key_id_start + key_id_len)
        return head[key_id_start:key_id_start+key_id_len]


def assert_file_encrypted(path):
    print(f'Checking file {path} is encrypted')
    try:
        with open(path, 'rb') as f:
            magic_len = len(encrypted_file_magic)
            magic = f.read(magic_len)
            print(f'magic: {magic}')
            assert magic == encrypted_file_magic, \
                f'file {path} doesn\'t seem to be encrypted, ' \
                f'first {magic_len} bytes are {magic}'
    except FileNotFoundError as e:
        # Converting FileNotFoundError to AssertionError to help
        # testlib.poll_for_condition (it catches AssertionError)
        print(f'Error checking file {path}: {e}')
        assert False, f'file {path} doesn\'t exist'


def assert_file_unencrypted(path):
    print(f'Checking file {path} is unencrypted')
    try:
        with open(path, 'rb') as f:
            magic_len = len(encrypted_file_magic)
            magic = f.read(magic_len)
            print(f'magic: {magic}')
            # Note that we have to treat empty files as unencrypted because
            # empty unencrypted log will have empty len and we don't want to
            # wait until something is written to it.
            assert magic != encrypted_file_magic, \
                f'file {path} seems to be encrypted, ' \
                f'first {magic_len} bytes are {magic}'
    except FileNotFoundError as e:
        # Converting FileNotFoundError to AssertionError to help
        # testlib.poll_for_condition (it catches AssertionError)
        print(f'Error checking file {path}: {e}')
        assert False, f'file {path} doesn\'t exist'


def assert_file_is_decryptable(node, file_path):
    data_dir = node.data_path()
    assert_file_encrypted(file_path)
    gosecrets_cfg_path = Path(data_dir) / 'config' / 'gosecrets.cfg'
    cbcat_path = testlib.get_utility_path('cbcat')
    cbcat_args = ['--with-gosecrets', gosecrets_cfg_path, file_path]
    r = subprocess.run([cbcat_path] + cbcat_args, capture_output=True)
    assert r.returncode == 0, f'cbcat returned {r.returncode}\n' \
                                f'stdout: {r.stdout.decode()}\n' \
                                f'stderr: {r.stderr.decode()}'


def drop_config_deks_for_node(node):
    drop_time = drop_deks(node, 'config')
    poll_verify_node_dek_info(node, 'configuration',
                              data_statuses=['encrypted'],
                              dek_number=1,
                              oldest_dek_time=drop_time)

def drop_bucket_deks_and_verify_dek_info(cluster, bucket):
    drop_time = drop_bucket_keys(cluster, bucket)

    for node in cluster.connected_nodes:
        if node.get_cluster_membership() == 'active' and \
           Service.KV in node.get_services():
            poll_verify_node_bucket_dek_info(node, bucket,
                                             data_statuses=['encrypted'],
                                             dek_number=1,
                                             oldest_dek_time=drop_time)
        else:
            poll_verify_node_bucket_dek_info(node, bucket,
                                             missing=True)


def copy_dek_files_to_node(source_node, target_node, relative_path):
    """Copy DEK files from source_node to a temp dir on target_node.
    Return list of absolute path strings on target for use in importEaRDEK."""
    src_dir = Path(source_node.data_path()) / relative_path
    assert src_dir.exists(), f'source DEK dir does not exist: {src_dir}'
    import_dir = Path(target_node.data_path()) / 'tmp_import_deks' / str(
        uuid.uuid4())
    import_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for p in src_dir.iterdir():
        if parse_key_file_name(p.name) is not None:
            print(f'Copying DEK file {p} to {import_dir}')
            dst = import_dir / p.name
            shutil.copy2(p, dst)
            paths.append(str(dst))
    return paths


def post_import_ear_dek(node, type_str, dek_paths, bucket_uuid=None):
    """POST /_cbauth/importEaRDEK. type_str: 'config','log','audit','bucket'."""
    body = {'type': type_str, 'dekPaths': dek_paths}
    if bucket_uuid is not None:
        body['bucketUUID'] = bucket_uuid
    return testlib.post_succ(node, '/_cbauth/importEaRDEK', json=body)


def kv_nodes(cluster):
    return [n for n in cluster.connected_nodes
            if Service.KV in n.get_services()]


def write_random_doc(cluster, bucket_name):
    random_key = testlib.random_str(10)
    random_value = testlib.random_str(10)
    testlib.post_succ(cluster,
                      f"/pools/default/buckets/{bucket_name}/docs/{random_key}",
                      data={"value": random_value})
    return random_key, random_value


def get_doc(cluster, bucket_name, key):
    r = testlib.get_succ(cluster,
                         f"/pools/default/buckets/{bucket_name}/docs/{key}")
    r = r.json()
    assert 'base64' in r, f'no base64 in response: {r}'
    return str(base64.b64decode(r['base64']), 'utf-8')


def start_kmip_server(server_conf_path, server_log_path):
    # The pykmip server log is verbose on standard out, it logs to
    # server.log anyway so we redirect stdout to avoid cluttering
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull_fd, 1)
    os.dup2(devnull_fd, 2)

    server = KmipServer(config_path=server_conf_path,
                        log_path=server_log_path)
    server.start()
    server.serve()


@contextmanager
def setup_kmip_server(cluster):
    server_process = None
    pykmip_path = os.path.join(testlib.get_resources_dir(), 'pykmip')
    pykmip_tmp_dir_path = os.path.join(pykmip_path, 'tmp')
    server_log_path = os.path.join(cluster.get_cluster_path(),
                                   'pykmip_server.log')

    def create_server_config_file():
        cert_path = os.path.join(pykmip_path, 'localhost.crt')
        key_path = os.path.join(pykmip_path, 'private.key')
        db_path = os.path.join(pykmip_tmp_dir_path, 'pykmip.db')

        server_conf_content = textwrap.dedent(f"""
            [server]
            hostname=127.0.0.1
            port={kmip_port}
            certificate_path={cert_path}
            key_path={key_path}
            ca_path={cert_path}
            auth_suite=TLS1.2
            enable_tls_client_auth=True
            logging_level=DEBUG
            database_path={db_path}
            """).strip()

        server_conf_path = os.path.join(pykmip_tmp_dir_path, 'server.conf')
        with open(server_conf_path, 'w') as f:
            f.write(server_conf_content)

        return server_conf_path

    def create_client_config_file():
        cert_path = os.path.join(pykmip_path, 'localhost.crt')
        key_path = os.path.join(pykmip_path, 'private.key')

        client_conf_content = textwrap.dedent(f"""
            [client]
            host=127.0.0.1
            port={kmip_port}
            certfile={cert_path}
            keyfile={key_path}
            ca_certs={cert_path}
            cert_reqs=CERT_REQUIRED
            ssl_version=PROTOCOL_SSLv23
            do_handshake_on_connect=True
            suppress_ragged_eofs=True
            """).strip()

        client_conf_path = os.path.join(pykmip_tmp_dir_path, 'client.conf')
        with open(client_conf_path, 'w') as f:
            f.write(client_conf_content)

        return client_conf_path

    def create_key(key_id, clientHandle):
        key_id = clientHandle.create(
            enums.CryptographicAlgorithm.AES,
            256,
            operation_policy_name='default',
            name=f'Test_256_AES_Symmetric_Key3_{key_id+1}',
            cryptographic_usage_mask=[
                enums.CryptographicUsageMask.ENCRYPT,
                enums.CryptographicUsageMask.DECRYPT,
            ])
        clientHandle.activate(key_id)

    def is_kmip_port_open():
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            result = sock.connect_ex(('127.0.0.1', kmip_port))
            return result == 0

    def check_server_ready_for_client(client):
        if not is_kmip_port_open():
            print('PyKMIP server is not running, waiting for it to start...')
            return False

        try:
            with client as c:
                create_key(0, c)
            return True
        except Exception as e:
            print(f'PyKMIP server is running, but not ready for client:: {e}')
            return False

    def cleanup_pykmip_server(server_process):
        if server_process is not None and server_process.is_alive():
            print('Terminating PyKMIP server...')
            server_process.terminate()
            server_process.join(timeout=5)
            if server_process.is_alive():
                print('PyKMIP server did not terminate gracefully, killing...')
                server_process.kill()
                server_process.join(timeout=5)
                if server_process.is_alive():
                    print('PyKMIP server force kill initiated ' \
                          '(may still be running)')
                else:
                    print('PyKMIP server killed')
            else:
                print('PyKMIP server terminated gracefully')
        elif server_process is not None:
            print('PyKMIP server already terminated')
        else:
            print('PyKMIP server was never started')

    if is_kmip_port_open():
        assert False, 'PyKMIP server is already running'


    try:
        if os.path.exists(pykmip_tmp_dir_path):
            shutil.rmtree(pykmip_tmp_dir_path)

        os.mkdir(pykmip_tmp_dir_path)

        client_conf_path = create_client_config_file()
        client = ProxyKmipClient(hostname='127.0.0.1', port=kmip_port,
                                config='client',
                                config_file=f'{client_conf_path}',
                                kmip_version=enums.KMIPVersion.KMIP_1_2)


        server_conf_path = create_server_config_file()
        server_process = Process(target=start_kmip_server,
                                 args=(server_conf_path, server_log_path))
        server_process.start()

        if not server_process.is_alive():
            assert False, 'PyKMIP server process failed to start or ' \
                          'exited immediately'

        testlib.poll_for_condition(
            lambda: check_server_ready_for_client(client),
            sleep_time=2, attempts=15,
            retry_on_assert=True, verbose=True)

        print('PyKMIP server started successfully, adding more keys...')
        with client as c:
            for i in range(4):
                create_key(i + 1, c)

        yield server_process
    finally:
        try:
            cleanup_pykmip_server(server_process)
        finally:
            if not testlib.config['keep_tmp_dirs'] \
                 and os.path.exists(pykmip_tmp_dir_path):
                try:
                    shutil.rmtree(pykmip_tmp_dir_path)
                    print('Cleaned up tmp server data')
                except Exception as e:
                    assert False, f'Warning: Could not remove tmp server'\
                                  f'data: {e}'


def poll_assert_secret_used_by(node, SecretId, UsedByList):
    testlib.poll_for_condition(
        lambda: assert_secret_used_by(node, SecretId, UsedByList),
        sleep_time=1, attempts=15,
        retry_on_assert=True, verbose=True)


def assert_secret_used_by(node, secret_id, expected_list):
    secret = get_secret(node, secret_id)
    used_by = secret['usedBy']
    assert len(used_by) == len(expected_list), \
           f'unexpected usedBy {used_by}, expected {expected_list}'
    for used_by_item in used_by:
        for expected_list_item in expected_list:
            if used_by_item['usage'] == expected_list_item['usage'] and \
               used_by_item['description'] == expected_list_item['description']:
                break
        else:
            assert False, \
                   f'unexpected usedBy {used_by_item}, expected {expected_list}'


def used_by_bucket(bucket_name):
    return {'usage': f'bucket-encryption-{bucket_name}',
            'description': f'bucket "{bucket_name}"'}


def used_by_key(key_name):
    return {'usage': f'KEK-encryption',
            'description': f'key "{key_name}"'}


def used_by_cfg():
    return {'usage': f'config-encryption',
            'description': f'configuration'}


def used_by_log():
    return {'usage': f'log-encryption',
            'description': f'logs'}


def assert_secret_test_results(secret, expected_status,
                               expected_description=None,
                               expected_missing_nodes=[],
                               expected_error_nodes=[],
                               expected_success_nodes=[]):
    r = secret["testResults"]

    print(f'secret {secret["id"]} ({secret["name"]}) test results: {r}')

    assert r["status"] == expected_status, \
           f'unexpected status: {r["status"]}, expected {expected_status}'

    if expected_description is not None:
        assert expected_description in r["description"], \
           f'unexpected description: {r["description"]},' \
           f'expected {expected_description}'

    mn = sorted([n.hostname() for n in expected_missing_nodes])
    en = sorted([n.hostname() for n in expected_error_nodes])
    sn = sorted([n.hostname() for n in expected_success_nodes])
    assert sorted(r["missingNodes"]) == mn, \
           f'unexpected missing nodes: {r["missingNodes"]},' \
           f'expected {mn}'
    assert sorted(r["errorNodes"]) == en, \
           f'unexpected error nodes: {r["errorNodes"]},' \
           f'expected {en}'
    assert sorted(r["successNodes"]) == sn, \
           f'unexpected success nodes: {r["successNodes"]},' \
           f'expected {sn}'
