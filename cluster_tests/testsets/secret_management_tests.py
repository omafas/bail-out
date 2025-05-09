# @author Couchbase <info@couchbase.com>
# @copyright 2023 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys

import testlib
import re
import os
import signal
import subprocess
import random

resourcedir = os.path.join(testlib.get_resources_dir(), "secrets_management")


class SecretManagementTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise",
                                           master_password_state="default")

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        reset_es_config(self.cluster)
        change_password(self.cluster, password='')

    def password_cmd_test(self):
        script_path = os.path.join(resourcedir, "getpass.sh")
        password = change_password(self.cluster)
        post_es_config(self.cluster, {'keyStorageType': 'file',
                                 'keyEncrypted': 'true',
                                 'passwordSource': 'script',
                                 'passwordCmd': script_path})

    # Try to reuse the same file for key. Password cmd returns the password
    # that is currently set
    def password_cmd_existing_keyfile_test(self):
        script_path = os.path.join(resourcedir, "getpass.sh")
        change_password(self.cluster,
                        password=subprocess.check_output([script_path]))
        data_dir = self.cluster.connected_nodes[0].data_path()
        datakey_path = os.path.join(data_dir, "config", "encrypted_data_keys")
        post_es_config(self.cluster, {'keyStorageType': 'file',
                                      'keyEncrypted': 'true',
                                      'keyPath': 'custom',
                                      'customKeyPath': datakey_path,
                                      'passwordSource': 'script',
                                      'passwordCmd': script_path})
        post_es_config(self.cluster, {'keyStorageType': 'file',
                                      'keyEncrypted': 'true',
                                      'keyPath': 'auto',
                                      'passwordSource': 'script',
                                      'passwordCmd': script_path})

    # Try to reuse the same file for key. Password cmd returns the password
    # that is wrong
    def password_cmd_existing_keyfile_wrong_password_test(self):
        script_path = os.path.join(resourcedir, "getpass_wrong.sh")
        password = change_password(self.cluster)
        data_dir = self.cluster.connected_nodes[0].data_path()
        datakey_path = os.path.join(data_dir, "config", "encrypted_data_keys")
        r = testlib.post_fail(
              self.cluster,
              "/node/controller/secretsManagement/encryptionService",
              400,
              data={'keyStorageType': 'file',
                    'keyEncrypted': 'true',
                    'keyPath': 'custom',
                    'customKeyPath': datakey_path,
                    'passwordSource': 'script',
                    'passwordCmd': script_path}).json()
        assert r['errors']['_'].startswith('secret already exists')

    def not_existing_password_cmd_test(self):
        try_wrong_password_cmd(self.cluster, "getpass_does_not_exist.sh")

    def not_executable_password_cmd_test(self):
        try_wrong_password_cmd(self.cluster, "getpass_notexec.sh")

    def password_cmd_fail_test(self):
        try_wrong_password_cmd(self.cluster, "getpass_fail.sh")

    def change_password_test(self):
        data = testlib.random_str(32)
        encrypted_data = encrypt(self.cluster, data)
        check_decrypt(self.cluster, encrypted_data, data)

        password = change_password(self.cluster)
        check_decrypt(self.cluster, encrypted_data, data)
        encrypted_data = encrypt(self.cluster, data)
        change_password(self.cluster, password='')
        check_decrypt(self.cluster, encrypted_data, data)

    def unchanged_config_reload_test(self):
        data = testlib.random_str(32)
        encrypted_data = encrypt(self.cluster, data)
        change_password(self.cluster)
        check_decrypt(self.cluster, encrypted_data, data)
        # Checking that reloading of unchanged config doesn't break anything
        post_es_config(self.cluster, {'keyStorageType': 'file',
                                      'keyEncrypted': 'true',
                                      'passwordSource': 'env'})
        check_decrypt(self.cluster, encrypted_data, data)
        post_es_config(self.cluster, {'keyStorageType': 'file',
                                      'keyEncrypted': 'true',
                                      'passwordSource': 'env'})
        # ... even when password changes between reloads
        change_password(self.cluster)
        post_es_config(self.cluster, {'keyStorageType': 'file',
                                      'keyEncrypted': 'true',
                                      'passwordSource': 'env'})
        check_decrypt(self.cluster, encrypted_data, data)

    def gosecrets_crash_test(self):
        password = change_password(self.cluster)

        data = testlib.random_str(32)
        encrypted_data = encrypt(self.cluster, data)

        kill_gosecrets(self.cluster)
        testlib.poll_for_condition(gosecrets_started_fun(self.cluster),
                                   sleep_time=0.2,
                                   timeout=60,
                                   verbose=True)
        check_decrypt(self.cluster, encrypted_data, data)

    def gosecret_crash_after_config_change_test(self):
        password = change_password(self.cluster)

        data = testlib.random_str(32)
        encrypted_data = encrypt(self.cluster, data)

        post_es_config(self.cluster, {'keyStorageType': 'file',
                                      'keyEncrypted': 'false'})

        post_es_config(self.cluster, {'keyStorageType': 'file',
                                      'keyEncrypted': 'true'})

        kill_gosecrets(self.cluster)
        testlib.poll_for_condition(gosecrets_started_fun(self.cluster),
                                   sleep_time=0.2,
                                   timeout=60,
                                   verbose=True)
        check_decrypt(self.cluster, encrypted_data, data)

    def config_change_test(self):
        passwordcmd = os.path.join(resourcedir, "getpass.sh")
        smcmd = os.path.join(resourcedir, "secretmngmt.py")
        tmpFile = os.path.join(resourcedir, "secret.tmp")
        seedFile = os.path.join(resourcedir, "seed.tmp")
        with open(seedFile, "w") as f:
            f.write(testlib.random_str(16))

        ensure_removed(tmpFile)
        rand = testlib.random_str(8)

        configs = [{'keyStorageType': 'file',
                    'keyEncrypted': 'true',
                    'passwordSource': 'env'},
                   {'keyStorageType': 'file',
                    'keyEncrypted': 'true',
                    'passwordSource': 'script',
                    'passwordCmd': passwordcmd},
                   {'keyStorageType': 'file',
                    'keyEncrypted': 'false'},
                   {'keyStorageType': 'script',
                    'readCmd': f'{smcmd} read {seedFile} {tmpFile} {rand}',
                    'writeCmd': f'{smcmd} write {seedFile} {tmpFile} {rand}',
                    'deleteCmd':
                        f'{smcmd} delete {seedFile} {tmpFile} {rand}'}]

        try:
            # Testing config change From -> To
            # Config change depends on both configs: From and To, so we want to
            # test different combinations of (From, To) instead of just trying
            # each config separately
            for i in range(50):
                if random.uniform(0, 1) < 0.5:
                    try:
                        change_password(self.cluster)
                    except AssertionError:
                        # For some configurations change_password returns error
                        # which is expected (here we are testing that
                        # changed password doesn't affect configuration
                        # changes)
                        pass
                post_es_config(self.cluster, random.choice(configs))
        finally:
            # Before removing seedFile and tmpFile we need to make sure
            # encryption service is not recovering right now. A config
            # change will make sure it has recovered and is not using those
            # files anymore
            reset_es_config(self.cluster)
            ensure_removed(tmpFile)
            ensure_removed(seedFile)

    def recover_during_set_config_test(self):
        smcmd = os.path.join(resourcedir, "secretmngmt.py")
        tmpFile = os.path.join(resourcedir, "secret.tmp")
        seedFile = os.path.join(resourcedir, "seed.tmp")
        with open(seedFile, "w") as f:
            f.write(testlib.random_str(16))

        # bad_read, bad_write, bad_delete - they work but not all the time,
        # they can fail with some probability
        def generate_cfg():
            rand = testlib.random_str(8)
            return {'keyStorageType': 'script',
                    'readCmd': f'{smcmd} bad_read {seedFile} {tmpFile} {rand}',
                    'writeCmd':
                        f'{smcmd} bad_write {seedFile} {tmpFile} {rand}',
                    'deleteCmd':
                        f'{smcmd} bad_delete {seedFile} {tmpFile} {rand}'}

        default_config = {'keyStorageType': 'file',
                          'keyEncrypted': 'true',
                          'passwordSource': 'env'}

        ensure_removed(tmpFile)
        print()
        print(f"posting config: {generate_cfg()}")
        try:
            for i in range(50):
                print("******************************************************")
                r = testlib.post(
                      self.cluster,
                      "/node/controller/secretsManagement/encryptionService",
                      data=generate_cfg())

                print(f"POST result: {r.status_code}")
                if r.status_code != 200:
                    print(r.text)

                assert r.status_code != 500
        finally:
            # Before removing seedFile and tmpFile we need to make sure
            # encryption service is not recovering right now. A config
            # change will make sure it has recovered and is not using those
            # files anymore
            reset_es_config(self.cluster)
            ensure_removed(tmpFile)
            ensure_removed(seedFile)


def post_es_config(cluster, cfg):
    print(f"posting the following config: {cfg}")
    testlib.post_succ(
      cluster,
      "/node/controller/secretsManagement/encryptionService",
      data=cfg)


def reset_es_config(cluster):
    post_es_config(cluster, {'keyStorageType': 'file',
                             'keyEncrypted': 'true',
                             'keyPath': 'auto',
                             'passwordSource': 'env'})


def encrypt(cluster, s):
    res = testlib.diag_eval(
            cluster,
            '{ok, B} = ' f'encryption_service:encrypt(<<"{s}">>),'
            'R = base64:encode_to_string(B),'
            '"Success:" ++ R.')
    search_res = re.search('"Success:(.*)"', res.text)
    assert search_res is not None, f"unexpected encrypt result: {res.text}"
    return search_res.group(1)


def decrypt(cluster, s):
    res = testlib.diag_eval(
            cluster,
            f'B = base64:decode("{s}"),'
            '{ok, R} = encryption_service:decrypt(B),'
            '"Success:" ++ binary_to_list(R).')
    search_res = re.search('"Success:(.*)"', res.text)
    assert search_res is not None, f"unexpected decrypt result: {res.text}"
    return search_res.group(1)


def check_decrypt(cluster, encrypted_data, expected_data):
    decrypted_data = decrypt(cluster, encrypted_data)

    assert expected_data == decrypted_data, \
           f"decrypted data doesn't match the original. " \
           "Expected: {expected_data}, Got: {decrypted_data}"


def kill_gosecrets(cluster):
    r = testlib.diag_eval(cluster, "encryption_service:os_pid().")
    gosecrets_pid = int(r.text)
    os.kill(gosecrets_pid, signal.SIGKILL)


def try_wrong_password_cmd(cluster, script):
    cmd = os.path.join(resourcedir, script)
    r = testlib.post_fail(
          cluster,
          "/node/controller/secretsManagement/encryptionService",
          400,
          data={'keyStorageType': 'file',
                'keyEncrypted': 'true',
                'passwordSource': 'script',
                'passwordCmd': cmd}).json()
    assert r['errors']['_'].startswith(
                                f'command \'{cmd}\' finished with error')


def change_password(cluster, password=None):
    if password is None:
        password = testlib.random_str(8)

    testlib.post_succ(cluster, "/node/controller/changeMasterPassword",
                      data={'newPassword': password})

    return password


def gosecrets_started_fun(cluster):
    def fun():
        r = testlib.diag_eval(cluster, "catch encryption_service:os_pid().")
        try:
            int(r.text)
            return True
        except ValueError:
            return False
    return fun


def ensure_removed(path):
    if os.path.exists(path):
        os.remove(path)
