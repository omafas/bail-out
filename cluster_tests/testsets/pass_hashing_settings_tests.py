# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
from testsets.authn_tests import scram_sha_auth

class PassHashingSettingsTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return [testlib.ClusterRequirements(edition="Provisioned"),
                testlib.ClusterRequirements(edition="Enterprise")]

    def setup(self):
        self.username_argon = testlib.random_str(16)
        self.username_pbkdf2 = testlib.random_str(16)
        self.username_sha = testlib.random_str(16)
        self.test_username = testlib.random_str(16)

    def test_teardown(self):
        delete_user(self.cluster, self.test_username)
        update_pwd_hash_migration_setting(self.cluster, "false")
        disable_all_scram_sha_settings(self.cluster)
        testlib.delete_succ(self.cluster,
                            '/settings/security/scramShaIterations')

    def teardown(self):
        delete_user(self.cluster, self.username_argon)
        delete_user(self.cluster, self.username_pbkdf2)
        delete_user(self.cluster, self.username_sha)
        testlib.delete_succ(self.cluster, '/settings/security/passwordHashAlg')
        testlib.delete_succ(self.cluster,
                            '/settings/security/scramSha512Enabled')
        testlib.delete_succ(self.cluster,
                            '/settings/security/scramSha256Enabled')
        testlib.delete_succ(self.cluster,
                            '/settings/security/scramSha1Enabled')
        testlib.delete_succ(self.cluster,
                            '/settings/security/scramShaIterations')
        testlib.delete_succ(self.cluster,
                            '/settings/security/argon2idTime')
        testlib.delete_succ(self.cluster,
                            '/settings/security/argon2idMem')
        testlib.delete_succ(self.cluster,
                            '/settings/security/pbkdf2HmacSha512Iterations')
        testlib.delete_succ(self.cluster,
                            '/settings/security/allowHashMigrationDuringAuth')

    def change_admin_pass_respects_hash_alg_test(self):
        assert_cluster_uses_admin_creds(self.cluster)
        admin = self.cluster.auth[0]
        password = self.cluster.auth[1]

        def test_hash(expected_hash):
            # changing admin password to make sure password hash is updated
            touch_user_password(self.cluster, admin, password)
            backup = get_backup(self.cluster, 'admin')
            verify_hash_type([backup['admin']], admin, expected_hash)

        testlib.delete_succ(self.cluster, '/settings/security/passwordHashAlg')
        test_hash('argon2id')

        testlib.post_succ(self.cluster, '/settings/security/passwordHashAlg',
                          data='pbkdf2-hmac-sha512')
        test_hash('pbkdf2-hmac-sha512')

        testlib.post_succ(self.cluster, '/settings/security/passwordHashAlg',
                          data='SHA-1')
        test_hash('SHA-1')

        testlib.post_succ(self.cluster, '/settings/security/passwordHashAlg',
                          data='argon2id')
        test_hash('argon2id')

    def admin_hash_migration_test(self):
        assert_cluster_uses_admin_creds(self.cluster)

        def verify_admin_hash_params(to_verify):
            # authenticate as admin in order to trigger automatic migration
            testlib.get_succ(self.cluster, "/whoami")
            backup = get_backup(self.cluster, 'admin')
            auth = backup['admin']['auth']
            for auth_type in to_verify:
                for key in to_verify[auth_type]:
                    testlib.assert_eq(auth[auth_type][key],
                                      to_verify[auth_type][key])

        testlib.post_succ(self.cluster, '/internalSettings/',
                          data={'argon2idMem': 9000,
                                'argon2idTime': 5,
                                'scramShaIterations': 20})

        testlib.post_succ(self.cluster, '/settings/security/',
                          data={'passwordHashAlg': 'argon2id',
                                'argon2idMem': 8192,
                                'argon2idTime': 1,
                                'allowHashMigrationDuringAuth': 'true',
                                'scramShaIterations': 12})

        # Checking that correct settings are used
        verify_admin_hash_params({'hash': {'algorithm': 'argon2id',
                                           'time': 1,
                                           'memory': 8192},
                                  'scram-sha-512': {'iterations': 12},
                                  'scram-sha-256': {'iterations': 12},
                                  'scram-sha-1': {'iterations': 12}})

        # ----------------------------------------------------------------------

        testlib.post_succ(self.cluster, '/internalSettings/',
                          data={'argon2idMem': 9001,
                                'argon2idTime': 6,
                                'scramShaIterations': 21})

        # Checking that there was no migration
        verify_admin_hash_params({'hash': {'algorithm': 'argon2id',
                                           'time': 1,
                                           'memory': 8192},
                                  'scram-sha-512': {'iterations': 12},
                                  'scram-sha-256': {'iterations': 12},
                                  'scram-sha-1': {'iterations': 12}})

        # ----------------------------------------------------------------------

        testlib.post_succ(self.cluster, '/settings/security/',
                          data={'argon2idMem': 8193})

        # Checking that there was a migration
        verify_admin_hash_params({'hash': {'algorithm': 'argon2id',
                                           'time': 1,
                                           'memory': 8193},
                                  'scram-sha-512': {'iterations': 12},
                                  'scram-sha-256': {'iterations': 12},
                                  'scram-sha-1': {'iterations': 12}})

        # ----------------------------------------------------------------------

        testlib.post_succ(self.cluster, '/settings/security/',
                          data={'argon2idTime': 2})

        # Checking that there was a migration
        verify_admin_hash_params({'hash': {'algorithm': 'argon2id',
                                           'time': 2,
                                           'memory': 8193},
                                  'scram-sha-512': {'iterations': 12},
                                  'scram-sha-256': {'iterations': 12},
                                  'scram-sha-1': {'iterations': 12}})

        # ----------------------------------------------------------------------

        testlib.post_succ(self.cluster, '/settings/security/',
                          data={'scramShaIterations': 13})

        # Checking that there was a migration
        verify_admin_hash_params({'hash': {'algorithm': 'argon2id',
                                           'time': 2,
                                           'memory': 8193},
                                  'scram-sha-512': {'iterations': 13},
                                  'scram-sha-256': {'iterations': 13},
                                  'scram-sha-1': {'iterations': 13}})

        # ----------------------------------------------------------------------

        testlib.post_succ(self.cluster, '/internalSettings/',
                          data={'pbkdf2HmacSha512Iterations': 20})

        testlib.post_succ(self.cluster, '/settings/security/',
                          data={'passwordHashAlg': 'pbkdf2-hmac-sha512',
                                'pbkdf2HmacSha512Iterations': 15})

        # Checking that there was a migration
        verify_admin_hash_params({'hash': {'algorithm': 'pbkdf2-hmac-sha512',
                                           'iterations': 15},
                                  'scram-sha-512': {'iterations': 13},
                                  'scram-sha-256': {'iterations': 13},
                                  'scram-sha-1': {'iterations': 13}})

        # ----------------------------------------------------------------------

        testlib.post_succ(self.cluster, '/settings/security/',
                          data={'allowHashMigrationDuringAuth': 'false',
                                'passwordHashAlg': 'SHA-1',
                                'scramShaIterations': 14})
        testlib.post_succ(self.cluster, '/internalSettings/',
                          data={'scramShaIterations': 22})

        # Checking that there was no migration
        verify_admin_hash_params({'hash': {'algorithm': 'pbkdf2-hmac-sha512',
                                           'iterations': 15},
                                  'scram-sha-512': {'iterations': 13},
                                  'scram-sha-256': {'iterations': 13},
                                  'scram-sha-1': {'iterations': 13}})


    def user_creation_respects_hash_alg_test(self):
        testlib.post_succ(self.cluster, '/settings/security/passwordHashAlg',
                          data='argon2id')
        pass_argon = create_user(self.cluster, self.username_argon)

        testlib.post_succ(self.cluster, '/settings/security/passwordHashAlg',
                          data='pbkdf2-hmac-sha512')
        pass_pbkdf2 = create_user(self.cluster, self.username_pbkdf2)

        # should not be used but we still need to support it for backward
        # compat
        testlib.post_succ(self.cluster, '/settings/security/passwordHashAlg',
                          data='SHA-1')
        pass_sha = create_user(self.cluster, self.username_sha)

        testlib.delete_succ(self.cluster, '/settings/security/passwordHashAlg')

        r = testlib.get_succ(self.cluster, '/settings/security/passwordHashAlg')
        assert r.text == '"argon2id"', 'unexpected default hash type'

        # Use backup to check that correct hash functions are used
        backup = get_backup(self.cluster, 'user:local:*')

        users = backup['users']

        verify_hash_type(users, self.username_argon, 'argon2id')
        verify_hash_type(users, self.username_pbkdf2, 'pbkdf2-hmac-sha512')
        verify_hash_type(users, self.username_sha, 'SHA-1')

        # Users still should be able to authenticate
        testlib.get_succ(self.cluster, '/pools/default',
                         auth=(self.username_argon, pass_argon))
        testlib.get_succ(self.cluster, '/pools/default',
                         auth=(self.username_pbkdf2, pass_pbkdf2))
        testlib.get_succ(self.cluster, '/pools/default',
                         auth=(self.username_sha, pass_sha))

    def change_pass_respects_hash_alg_test(self):
        testlib.post_succ(self.cluster, '/settings/security/passwordHashAlg',
                          data='argon2id')
        password = create_user(self.cluster, self.test_username)
        testlib.post_succ(self.cluster, '/settings/security/passwordHashAlg',
                          data='pbkdf2-hmac-sha512')

        touch_user_password(self.cluster, self.test_username, password)

        backup = get_backup(self.cluster, f'user:local:{self.test_username}')

        verify_hash_type(backup['users'], self.test_username,
                         'pbkdf2-hmac-sha512')

    def scram_sha1_disable_test(self):
        scram_sha_disable_test(self.cluster, self.test_username,
                               '/settings/security/scramSha1Enabled',
                               'SCRAM-SHA-1')

    def scram_sha256_disable_test(self):
        scram_sha_disable_test(self.cluster, self.test_username,
                               '/settings/security/scramSha256Enabled',
                               'SCRAM-SHA-256')

    def scram_sha512_disable_test(self):
        scram_sha_disable_test(self.cluster, self.test_username,
                               '/settings/security/scramSha512Enabled',
                               'SCRAM-SHA-512')

    def scram_sha_iterations_test(self):
        def assert_iterations(expected):
            backup = get_backup(self.cluster,
                                f'user:local:{self.test_username}')
            auth = backup['users'][0]['auth']
            assert expected == auth['scram-sha-512']['iterations']
            assert expected == auth['scram-sha-256']['iterations']
            assert expected == auth['scram-sha-1']['iterations']

        testlib.delete_succ(self.cluster,
                            '/settings/security/scramSha512Enabled')
        testlib.delete_succ(self.cluster,
                            '/settings/security/scramSha256Enabled')
        testlib.delete_succ(self.cluster, '/settings/security/scramSha1Enabled')
        testlib.delete_succ(self.cluster,
                            '/settings/security/scramShaIterations')

        password = create_user(self.cluster, self.test_username)
        assert_iterations(15000)

        iterations = 12345
        testlib.post_succ(self.cluster, '/settings/security/scramShaIterations',
                          data=str(iterations))

        password = create_user(self.cluster, self.test_username)
        assert_iterations(iterations)

        iterations = 1234
        testlib.post_succ(self.cluster, '/settings/security/scramShaIterations',
                          data=str(iterations))
        touch_user_password(self.cluster, self.test_username, password)
        assert_iterations(iterations)

    def pwd_hash_migration_sha1_to_argon2id_pos_test(self):
        pwd_hash_migration_alg_change_test(self.cluster, self.test_username,
                                           "SHA-1", "argon2id", migrate='true')

    def pwd_hash_migration_sha1_to_argon2id_neg_test(self):
        pwd_hash_migration_alg_change_test(self.cluster, self.test_username,
                                           "SHA-1", "argon2id", migrate='false')

    def pwd_hash_migration_sha1_to_pbkdf2_pos_test(self):
        pwd_hash_migration_alg_change_test(self.cluster, self.test_username,
                                           "SHA-1", "pbkdf2-hmac-sha512",
                                           migrate='true')

    def pwd_hash_migration_sha1_to_pbkdf2_neg_test(self):
        pwd_hash_migration_alg_change_test(self.cluster, self.test_username,
                                           "SHA-1", "pbkdf2-hmac-sha512",
                                           migrate='false')

    def pwd_hash_migration_pbkdf2_to_argon2id_pos_test(self):
        pwd_hash_migration_alg_change_test(self.cluster, self.test_username,
                                           "pbkdf2-hmac-sha512", "argon2id",
                                           migrate='true')

    def pwd_hash_migration_pbkdf2_to_argon2id_neg_test(self):
        pwd_hash_migration_alg_change_test(self.cluster, self.test_username,
                                           "pbkdf2-hmac-sha512", "argon2id",
                                           migrate='false')

    def pwd_hash_migration_argon2id_to_pbkdf2_pos_test(self):
        pwd_hash_migration_alg_change_test(self.cluster, self.test_username,
                                           "argon2id", "pbkdf2-hmac-sha512",
                                           migrate='true')

    def pwd_hash_migration_argon2id_to_pbkdf2_neg_test(self):
        pwd_hash_migration_alg_change_test(self.cluster, self.test_username,
                                           "argon2id", "pbkdf2-hmac-sha512",
                                           migrate='false')

    def pwd_hash_migration_change_argon2id_settings_pos_test(self):
        pwd_hash_migration_alg_settings_change_test(
            self.cluster, self.test_username, "argon2id",
            old_settings={"argon2idMem": f"{10 * 1024}", "argon2idTime": "5"},
            new_settings={"argon2idMem": f"{12 * 1024}", "argon2idTime": "7"},
            migrate='true')

    def pwd_hash_migration_change_argon2id_settings_neg_test(self):
        pwd_hash_migration_alg_settings_change_test(
            self.cluster, self.test_username, "argon2id",
            old_settings={"argon2idMem": f"{10 * 1024}", "argon2idTime": "5"},
            new_settings={"argon2idMem": f"{12 * 1024}", "argon2idTime": "7"},
            migrate='false')

    def pwd_hash_migration_change_pbdkf2_settings_pos_test(self):
        pwd_hash_migration_alg_settings_change_test(
            self.cluster, self.test_username, "pbkdf2-hmac-sha512",
            old_settings={"pbkdf2HmacSha512Iterations": "20000"},
            new_settings={"pbkdf2HmacSha512Iterations": "25000"},
            migrate='true')

    def pwd_hash_migration_change_pbdkf2_settings_neg_test(self):
        pwd_hash_migration_alg_settings_change_test(
            self.cluster, self.test_username, "pbkdf2-hmac-sha512",
            old_settings={"pbkdf2HmacSha512Iterations": "20000"},
            new_settings={"pbkdf2HmacSha512Iterations": "25000"},
            migrate='false')

    def pwd_hash_migration_enable_scram_sha1_pos_test(self):
        update_scram_sha_hashes_test(
            self.cluster, self.test_username, enable="true",
            type="sha1", migrate="true")

    def pwd_hash_migration_disable_scram_sha1_pos_test(self):
        update_scram_sha_hashes_test(
            self.cluster, self.test_username, enable="false",
            type="sha1", migrate="true")

    def pwd_hash_migration_enable_scram_sha1_neg_test(self):
        update_scram_sha_hashes_test(
            self.cluster, self.test_username, enable="true",
            type="sha1", migrate="false")

    def pwd_hash_migration_disable_scram_sha1_neg_test(self):
        update_scram_sha_hashes_test(
            self.cluster, self.test_username, enable="false",
            type="sha1", migrate="false")

    def pwd_hash_migration_enable_scram_sha256_pos_test(self):
        update_scram_sha_hashes_test(
            self.cluster, self.test_username, enable="true",
            type="sha256", migrate="true")

    def pwd_hash_migration_enable_scram_sha256_neg_test(self):
        update_scram_sha_hashes_test(
            self.cluster, self.test_username, enable="true",
            type="sha256", migrate="false")

    def pwd_hash_migration_enable_scram_sha512_pos_test(self):
        update_scram_sha_hashes_test(
            self.cluster, self.test_username, enable="true",
            type="sha512", migrate="true")

    def pwd_hash_migration_enable_scram_sha512_neg_test(self):
        update_scram_sha_hashes_test(
            self.cluster, self.test_username, enable="true",
            type="sha256", migrate="false")

    def pwd_hash_migration_change_iterations_scram_sha1_pos_test(self):
        update_scram_sha_iterations_test(
            self.cluster, self.test_username,
            type="sha1", iterations="20000", migrate="true")

    def pwd_hash_migration_change_iterations_scram_sha1_neg_test(self):
        update_scram_sha_iterations_test(
            self.cluster, self.test_username,
            type="sha1", iterations="20000", migrate="false")

    def pwd_hash_migration_change_iterations_scram_sha512_pos_test(self):
        update_scram_sha_iterations_test(
            self.cluster, self.test_username,
            type="sha512", iterations="20000", migrate="true")

    def pwd_hash_migration_change_iterations_scram_sha512_neg_test(self):
        update_scram_sha_iterations_test(
            self.cluster, self.test_username,
            type="sha512", iterations="20000", migrate="false")

class PasswordHashMigrationProvisionedProfileTests(testlib.BaseTestSet):
    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Provisioned")

    def setup(self):
        pass

    def test_teardown(self):
        pass

    def teardown(self):
        pass

    def default_hash_migration_settings_test(self):
        r = testlib.get_succ(self.cluster,
                             '/settings/security').json()
        testlib.assert_eq(r["allowHashMigrationDuringAuth"], True)

class PasswordHashMigrationOnPremTests(testlib.BaseTestSet):
    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise")

    def setup(self):
        pass

    def test_teardown(self):
        pass

    def teardown(self):
        pass

    def default_hash_migration_settings_test(self):
        r = testlib.get_succ(self.cluster,
                             '/settings/security').json()
        testlib.assert_eq(r["allowHashMigrationDuringAuth"], False)


class Argon2SettingsTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    def setup(self):
        pass

    def test_teardown(self):
        testlib.delete_config_key(self.cluster, 'argon2id_max_params_product')
        testlib.delete_config_key(self.cluster, 'argon2id_max_exec_time')

    def teardown(self):
        pass

    def too_big_params_test(self):
        testlib.set_config_key(self.cluster, 'argon2id_max_params_product', 1)
        r = testlib.post_fail(self.cluster, f"/settings/security",
                              expected_code=400,
                              data={'argon2idTime': 2,
                                    'argon2idMem': 8192}).json()
        err = 'The product of argon2id time and memory parameters ' \
              'must not exceed 1'
        assert err in r['errors'], f'Expected error is missing in response'

    def too_long_calculation_test(self):
        testlib.set_config_key(self.cluster, 'argon2id_max_exec_time', 0)
        r = testlib.post_fail(self.cluster, f"/settings/security",
                              expected_code=400,
                              data={'argon2idTime': 100,
                                    'argon2idMem': 10000000}).json()
        err = 'Argon2id test hash calculation with provided parameters took ' \
              'more than 0 ms'
        assert err in r['errors'], f'Expected error is missing in response'

    def incorrect_params_test(self):
        # Preformat payload because we want to specify argon2idTime multiple
        # times
        data = 'argon2idTime=abc&argon2idMem=0&argon2idTime=0'
        r = testlib.post_fail(self.cluster, f"/settings/security",
                              expected_code=400, data=data).json()
        err = 'Argon2id test hash calculation with provided parameters took ' \
              'more than 0 ms'

        msgs = ['argon2idTime - The value must be between 1 and 4294967295.',
                'argon2idMem - The value must be between 8192 and 1073741824.',
                'argon2idTime - duplicate key']
        for m in msgs:
            assert m in r['errors'], f'Expected error is missing in response'


def verify_hash_type(users, username, expected_hash_type):
    record = next(filter(lambda x: x['id'] == username, users))
    hash_type = record['auth']['hash']['algorithm']
    assert hash_type == expected_hash_type


def create_user(cluster, username):
    password = testlib.random_str(16)
    testlib.put_succ(cluster, f'/settings/rbac/users/local/{username}',
                     data={'password': password, 'roles': 'ro_admin'})
    return password


def delete_user(cluster, username):
    testlib.ensure_deleted(cluster, f'/settings/rbac/users/local/{username}')


def get_backup(cluster, what):
    return testlib.get_succ(cluster, '/settings/rbac/backup',
                            params={'include': what}).json()


def scram_sha_disable_test(cluster, user, setting_endpoint, scram_type):
    # By default it is enabled: check that hash is present and user
    # can authenticate
    testlib.delete_succ(cluster, setting_endpoint)

    password = create_user(cluster, user)
    backup = get_backup(cluster, f'user:local:{user}')
    assert scram_type.lower() in backup['users'][0]['auth']
    r = scram_sha_auth(scram_type, '/pools/default', (user, password), cluster)
    testlib.assert_http_code(200, r)

    # Disable scram-sha, and checking that user can't authenticate now
    # even if hash is present, also check that hash disappear after
    # password change
    testlib.post_succ(cluster, setting_endpoint, data='false')

    def assert_scram_auth_is_disabled():
        try:
            scram_sha_auth(scram_type, '/pools/default',
                           (user, password), cluster)
            assert False, 'scram auth is expected to crash'
        except ValueError as e:
            err = str(e)
            assert err.startswith('wrong \'WWW-Authenticate\' value: Basic')

    assert_scram_auth_is_disabled()

    touch_user_password(cluster, user, password)

    assert_scram_auth_is_disabled()

    backup = get_backup(cluster, f'user:local:{user}')
    assert scram_type.lower() not in backup['users'][0]['auth']

    # Enable it explicitly and check that it works again
    testlib.post_succ(cluster, setting_endpoint, data='true')

    touch_user_password(cluster, user, password)

    r = scram_sha_auth(scram_type, '/pools/default', (user, password), cluster)
    testlib.assert_http_code(200, r)


def touch_user_password(cluster, user, password):
    # Need to actually change the password, for it to be updated
    password1 = testlib.random_str(16)
    testlib.post_succ(cluster, '/controller/changePassword',
                      auth=(user, password), data={'password': password1})
    # Change password back
    testlib.post_succ(cluster, '/controller/changePassword',
                      auth=(user, password1), data={'password': password})


def pwd_hash_migration_alg_change_test(
        cluster, user, old_alg, new_alg, migrate):
    update_pwd_hash_alg_type(cluster, old_alg)
    pwd = create_user(cluster, user)

    update_pwd_hash_migration_setting(cluster, migrate)
    update_pwd_hash_alg_type(cluster, new_alg)

    # Trigger a pwd hash migration via authenticating the user.
    authenticate_user(cluster, user, pwd)
    if migrate == "true":
        validate_pwd_hash_type(cluster, user, new_alg)
        # Once the pwd hash have been migrated check if the user can be
        # authenticated.
        authenticate_user(cluster, user, pwd)
    else:
        validate_pwd_hash_type(cluster, user, old_alg)


def pwd_hash_migration_alg_settings_change_test(
        cluster, user, alg, old_settings, new_settings, migrate):
    update_pwd_hash_settings(cluster, alg, old_settings)
    pwd = create_user(cluster, user)

    update_pwd_hash_migration_setting(cluster, migrate)
    update_pwd_hash_settings(cluster, alg, new_settings)

    # Trigger a pwd hash migration via authenticating the user.
    authenticate_user(cluster, user, pwd)
    if migrate == "true":
        validate_pwd_hash_settings(cluster, user, alg, new_settings)
        # Once the pwd hash have been migrated check if the user can be
        # authenticated.
        authenticate_user(cluster, user, pwd)
    else:
        validate_pwd_hash_settings(cluster, user, alg, old_settings)


def validate_pwd_hash_type(cluster, user, alg):
    backup = get_backup(cluster, f"user:local:{user}")
    verify_hash_type(backup['users'], user, alg)


def update_pwd_hash_alg_type(cluster, alg):
    testlib.post_succ(cluster, '/settings/security/passwordHashAlg',
                      data=alg)


def validate_pwd_hash_settings(cluster, user, alg, expected_settings):
    backup = get_backup(cluster, f"user:local:{user}")
    users = backup['users']

    assert len(users) == 1, "Failed retrieving backup info for user - {user}"

    verify_hash_type(users, user, alg)
    auth_info = users[0]

    if alg == "argon2id":
        validate_pwd_hash_argon2id(auth_info, expected_settings)
    elif alg == "pbkdf2-hmac-sha512":
        validate_pwd_hash_pbkdf2(auth_info, expected_settings)
    else:
        assert False, f"Invalid hash algorithm: {alg}"


def validate_pwd_hash_argon2id(auth_info, expected_settings):
    for setting, val in expected_settings.items():
        if setting == "argon2idTime":
            assert auth_info['auth']['hash']['time'] == int(val)
        elif setting == "argon2idMem":
            assert auth_info['auth']['hash']['memory'] == int(val)
        else:
            assert False, f"Invalid pwd hash setting for argon2id: {setting}"


def validate_pwd_hash_pbkdf2(auth_info, expected_settings):
    for setting, val in expected_settings.items():
        if setting == "pbkdf2HmacSha512Iterations":
            assert auth_info['auth']['hash']['iterations'] == int(val)
        else:
            assert False,  f"Invalid pwd hash setting for pbkdf2: {setting}"


def update_pwd_hash_settings(cluster, alg, settings):
    update_pwd_hash_alg_type(cluster, alg)

    if alg == "argon2id":
        valid_settings = ["argon2idTime", "argon2idMem"]
    elif alg == "pbkdf2-hmac-sha512":
        valid_settings = ["pbkdf2HmacSha512Iterations"]

    for setting, value in settings.items():
        assert setting in valid_settings, \
            f"Invalid pwd hash setting ({setting}) for {alg}"

        testlib.post_succ(
            cluster, f"/settings/security/{setting}", data=value)


def update_pwd_hash_migration_setting(cluster, enable):
    enabled_by_default = testlib.diag_eval(
        cluster,
        code="config_profile:get_bool(allow_hash_migration_during_auth).").text
    is_set = testlib.diag_eval(
        cluster,
        code="ns_config:read_key_fast(allow_hash_migration_during_auth, "
        "undefined).").text

    if enable == "true" and enabled_by_default == "true" \
            and is_set == "undefined":
        return

    testlib.post_succ(
        cluster, '/settings/security/allowHashMigrationDuringAuth',
        data=enable)


def authenticate_user(cluster, user, pwd):
    testlib.get_succ(cluster, "/whoami", auth=(user, pwd))


def update_scram_sha_hashes_test(cluster, user, enable, type, migrate):
    # Enable the settings intitally to test disable later.
    if enable == "false":
        update_scram_sha_setting(cluster, type, "true")

    pwd = create_user(cluster, user)
    update_pwd_hash_migration_setting(cluster, migrate)

    update_scram_sha_setting(cluster, type, enable)

    # Trigger a pwd hash migration via authenticating the user.
    authenticate_user(cluster, user, pwd)
    validate_scram_sha_settings(cluster, user, enable, type, migrate)

    ## Check the user can still authenticate using the migrated scram-sha
    # hashes.
    if migrate == "true" and enable == "true":
        validate_scram_sha_auth(cluster, user, pwd, type)    

def update_scram_sha_iterations_test(cluster, user, type, iterations, migrate):
    # Always enable the scra-sha 'type' and the hashes should be update to the
    # new iteration count only when the pwd_hash_migration_settings is enabled.
    update_scram_sha_setting(cluster, type, "true")
    pwd = create_user(cluster, user)

    old_iterations = get_scram_sha_iterations(cluster)
    update_pwd_hash_migration_setting(cluster, migrate)
    update_scram_sha_iterations(cluster, iterations)

    # Trigger a pwd hash migration via authenticating the user.
    authenticate_user(cluster, user, pwd)
    validate_scram_sha_iterations(
        cluster, user, type, old_iterations, iterations, migrate)

    ## Check the user can still authenticate against the scram-sha hashes.
    validate_scram_sha_auth(cluster, user, pwd, type)

def update_scram_sha_setting(cluster, type, enable):
    if type == "sha1":
        uri = "scramSha1Enabled"
    elif type == "sha256":
        uri = "scramSha256Enabled"
    elif type == "sha512":
        uri = "scramSha512Enabled"
    else:
        assert False, "Invalid scram-sha type"

    testlib.post_succ(cluster, f"/settings/security/{uri}", data=enable)


def update_scram_sha_iterations(cluster, iterations):
    testlib.post_succ(
        cluster, "/settings/security/scramShaIterations", data=iterations)


def validate_scram_sha_iterations(
    cluster, user, type, old_iterations, iterations, migrate):
    auth_info = get_auth_info(cluster, user)
    scram_sha_key = get_scram_sha_key(type)
    if migrate == "true":
        expected = iterations
    else:
        expected = old_iterations

    assert auth_info[scram_sha_key]["iterations"] == int(expected), \
        f"incorrect iterations for {scram_sha_key}"


def validate_scram_sha_settings(cluster, user, enable, type, migrate):
    auth_info = get_auth_info(cluster, user)
    scram_sha_key = get_scram_sha_key(type)

    # Conditions:
    # 1. enable=true, migrate=true.
    #    The sha type hash should be found.
    # 2. enable=false, migrate=true.
    #    The sha type hash shouldn't be found.
    # 3. enable=true, migrate=false.
    #    The sha type hash shouldn't be found.
    # 4. enable=false, migrate=false.
    #    The sha type hash should be found. 
    if migrate == enable:
        assert scram_sha_key in auth_info.keys(), \
            f"{scram_sha_key} not present"
    else:
        assert scram_sha_key not in auth_info.keys(), \
            f"{scram_sha_key} present"


def get_scram_sha_iterations(cluster):
    r = testlib.get_succ(cluster, "/settings/security/scramShaIterations")
    return int(r.text)


def get_scram_sha_key(type):
    if type == "sha1":
        hash_key = "scram-sha-1"
    elif type == "sha256":
        hash_key = "scram-sha-256"
    elif type == "sha512":
        hash_key = "scram-sha-512"
    else:
        assert False, "Invalid scram-sha type"

    return hash_key


def get_auth_info(cluster, user):
    backup = get_backup(cluster, f"user:local:{user}")
    users = backup['users']
    assert len(users) == 1, "Failed retrieving backup info for user - {user}"
    return users[0]['auth']


def disable_all_scram_sha_settings(cluster):
    for type in ["sha1", "sha256", "sha512"]:
        update_scram_sha_setting(cluster, type, enable="false")


def validate_scram_sha_auth(cluster, user, pwd, type):
    scram_type = get_scram_sha_key(type).upper()
    r = scram_sha_auth(scram_type, '/whoami', (user, pwd), cluster)
    testlib.assert_http_code(200, r)


def assert_cluster_uses_admin_creds(cluster):
    admin = testlib.get_succ(cluster, '/settings/web').json()['username']
    assert admin == cluster.auth[0], \
           'this test expects that cluster uses administrator auth'
