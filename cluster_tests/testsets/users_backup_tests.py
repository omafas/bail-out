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
import os
from testsets.users_tests import put_user, delete_user, lock_user, unlock_user
from copy import deepcopy


class UsersBackupTests(testlib.BaseTestSet):
    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise")


    def setup(self):
        self.username = testlib.random_str(16)
        self.username_ext = testlib.random_str(16)
        self.groupname = testlib.random_str(16)
        self.group_description = testlib.random_str(16)
        self.password = testlib.random_str(16)
        put_group(self.cluster, self.groupname, self.group_description)
        put_user(self.cluster, 'local', self.username, password=self.password,
                 groups=self.groupname)
        put_user(self.cluster, 'external', self.username_ext,
                 groups=self.groupname)
        local_users = testlib.get_succ(self.cluster,
                                       '/settings/rbac/users/local').json()
        self.local_users_count = len(local_users)
        ext_users = testlib.get_succ(self.cluster,
                                     '/settings/rbac/users/external').json()
        self.external_users_count = len(ext_users)
        groups = testlib.get_succ(self.cluster, '/settings/rbac/groups').json()
        self.groups_count = len(groups)


    def teardown(self):
        testlib.ensure_deleted(
          self.cluster, f'/settings/rbac/users/local/{self.username}')
        testlib.ensure_deleted(
          self.cluster, f'/settings/rbac/users/external/{self.username_ext}')
        testlib.ensure_deleted(
          self.cluster, f'/settings/rbac/groups/{self.groupname}')


    def basic_backup_test(self):
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup').json()
        assert 'version' in backup
        assert backup['version'] == '1'
        assert 'admin' in backup
        assert len(backup['users']) == self.local_users_count + \
                                       self.external_users_count
        assert len(backup['groups']) == self.groups_count


    def no_admin_backup_test(self):
        params = {'exclude': 'admin'}
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup',
                                  params=params).json()
        assert 'admin' not in backup
        assert len(backup['users']) == self.local_users_count + \
                                      self.external_users_count
        assert len(backup['groups']) == self.groups_count


    def empty_backup_test(self):
        params = {'exclude': '*'}
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup',
                                  params=params).json()
        assert 'admin' not in backup
        assert len(backup['users']) == 0
        assert len(backup['groups']) == 0


    def include_exclude_mutually_exclusive_test(self):
        params = {'exclude': 'user:*:*', 'include': 'admin'}
        res = testlib.get_fail(self.cluster, '/settings/rbac/backup', 400,
                               params=params).json()
        assert res['errors']['include'] == \
               'include and exclude are mutually exclusive'


    def only_admin_backup_test(self):
        params = {'include': 'admin'}
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup',
                                  params=params).json()
        assert 'admin' in backup
        assert len(backup['users']) == 0
        assert len(backup['groups']) == 0


    def only_groups_backup_test(self):
        params = {'exclude': ['user:*:*', 'admin']}
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup',
                                  params=params).json()
        assert 'admin' not in backup
        assert len(backup['users']) == 0
        assert len(backup['groups']) == self.groups_count


    def only_local_users_backup_test(self):
        params = {'exclude': ['user:external:*', 'admin', 'group:*']}
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup',
                                  params=params).json()
        assert 'admin' not in backup
        assert len(backup['users']) == self.local_users_count
        assert len(backup['groups']) == 0
        for u in backup['users']:
            assert u['domain'] == 'local'


    def only_local_users_backup2_test(self):
        params = {'include': 'user:local:*'}
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup',
                                  params=params).json()
        assert 'admin' not in backup
        assert len(backup['users']) == self.local_users_count
        assert len(backup['groups']) == 0
        for u in backup['users']:
            assert u['domain'] == 'local'


    def only_specific_user_and_group_backup_test(self):
        params = {'include': [f'user:local:{self.username}',
                              f'group:{self.groupname}']}
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup',
                                  params=params).json()
        assert 'admin' not in backup
        assert len(backup['users']) == 1
        assert len(backup['groups']) == 1
        assert backup['users'][0]['id'] == self.username
        assert backup['groups'][0]['name'] == self.groupname


    def restore_users_and_groups_test(self):
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup').json()

        existing_users_count = self.local_users_count + \
                               self.external_users_count
        users_count = len(backup['users'])
        assert users_count == existing_users_count, \
               f'incorrect users count in backup: {users_count}'
        groups_count = len(backup['groups'])
        assert groups_count == self.groups_count, \
               f'incorrect groups count in backup: {users_count}'

        # Changing password for our user, so we can check later that backup
        # restore has not overwritten that user
        new_password = testlib.random_str(16)
        new_group_description = testlib.random_str(16)
        put_user(self.cluster, 'local', self.username, password=new_password,
                 groups=self.groupname)
        put_group(self.cluster, self.groupname, new_group_description)
        restore(self.cluster, backup, can_overwrite=False,
                expected_counters={'usersSkipped': users_count + 1,
                                   'groupsSkipped': groups_count})
                                                 # +1 because we also skip Admin
        check_user_pass(self.cluster, self.username, new_password)
        check_user_pass_fail(self.cluster, self.username, self.password)
        check_group_description(self.cluster, self.groupname,
                                new_group_description)

        # Now trying to restore users again, but now overwriting users
        restore(self.cluster, backup, can_overwrite=True,
                expected_counters={'usersOverwritten': users_count + 1,
                                   'groupsOverwritten': groups_count})
                                            # +1 because we also overwrite Admin

        # Now new_password should not work, because we have restored old user
        # from the backup
        check_user_pass(self.cluster, self.username, self.password)
        check_user_pass_fail(self.cluster, self.username, new_password)
        check_group_description(self.cluster, self.groupname,
                                self.group_description)

        # Lock the user, to check that locked status is correctly restored
        lock_user(self.cluster, self.username)
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup').json()

        # Now delete the user and check that it will be recreated
        delete_user(self.cluster, 'local', self.username)
        testlib.delete_succ(self.cluster,
                            f'/settings/rbac/groups/{self.groupname}')

        restore(self.cluster, backup, can_overwrite=False,
                expected_counters={'usersCreated': 1,
                                   'usersSkipped': users_count,
                                   'groupsCreated': 1,
                                   'groupsSkipped': groups_count - 1})

        # Password should not yet work, because user is locked
        check_user_pass_fail(self.cluster, self.username, self.password)

        unlock_user(self.cluster, self.username)

        # Now password should work, because the user is restored from backup
        # (and unlocked)
        check_user_pass(self.cluster, self.username, self.password)
        check_group_description(self.cluster, self.groupname,
                                self.group_description)

        # Test user with a temporary password
        put_user(self.cluster, 'local', self.username, password=self.password,
                 temporary_password="true")

        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup').json()

        # Now delete the user and check that it will be recreated
        delete_user(self.cluster, 'local', self.username)

        restore(self.cluster, backup, can_overwrite=False,
                expected_counters={'usersCreated': 1,
                                   'usersSkipped': users_count,
                                   'groupsCreated': 0,
                                   'groupsSkipped': groups_count})

        # Now password should give a 403 error as it is single use
        check_user_pass_fail(self.cluster, self.username, self.password,
                             status=403)


    # Restore a backup taken on a 7.6.x system containing roles that have
    # been replaced with new ones. The restoration should transform the old
    # roles into the new ones.
    def secure_restore_from_older_release_backup_file_test(self):
        # File is a result of /setting/rbac/backup on 7.6.x system.
        backup_file_path = os.path.join(testlib.get_resources_dir(), "fixtures",
                                        "full_backup.json")
        with open(backup_file_path) as f:
            backup = json.load(f)

        try:
            # Create a user with admin role to do the restore. The admin is
            # needed as the backup contains users with security roles. It's
            # some of these security roles whose transformation we're verifying.
            user = 'FullAdmin'
            name = testlib.random_str(10)
            password = testlib.random_str(10)
            put_user(self.cluster, 'local', user, password, roles='admin',
                     full_name=name, groups='', validate_user_props=True)

            restore(self.cluster, backup, can_overwrite=False,
                    expected_counters={'usersCreated': 11,
                                       # Skip 'Administrator'
                                       'usersSkipped': 1,
                                       'groupsCreated': 0,
                                       'groupsSkipped': 0},
                    auth_user=(user, password))

            # Ensure these tranformations have occurred where old roles, no
            # longer supported on 8.0 are replaced with supported roles.
            #
            #   localsecurityadmin76:
            #       security_admin_local => security_admin + user_admin_local
            #   externalsecurityadmin76:
            #       security_admin_external => security_admin +
            #                                   user_admin_external
            #   localsecurityandbackupadmin76:
            #       data_backup + local_admin_security =>
            #           data_backup + Local User Admin + Security Admin
            #
            # In addition, the 8.1 release made Web Console Access (ui_access)
            # a separate role.

            verify_roles(self.cluster, 'localsecurityadmin76',
                         ['security_admin', 'user_admin_local', 'ui_access'])
            verify_roles(self.cluster, 'externalsecurityadmin76',
                         ['security_admin', 'user_admin_external', 'ui_access'])
            verify_roles(self.cluster, 'localsecurityandbackupadmin76',
                         ['data_backup', 'security_admin', 'user_admin_local',
                          'ui_access'])

            # We now have a mixture of security and non-security roles. Do
            # a backup from a non-security role and ensure none of the security
            # roles get backed up.
            user2 = 'UserAdminLocal'
            name2 = testlib.random_str(10)
            password2 = testlib.random_str(10)
            put_user(self.cluster, 'local', user2, password2,
                     roles='user_admin_local', full_name=name2, groups='',
                     validate_user_props=True)

            kwargs = {"auth": (user2, password2)}
            backup2 = testlib.get_succ(self.cluster, '/settings/rbac/backup',
                                   **kwargs).json()
            users2 = [user["id"] for user in backup2.get("users", [])]

            assert 'localsecurityadmin76' not in users2
            assert 'externalsecurityadmin76' not in users2
            assert 'localsecurityandbackupadmin76' not in users2
            assert 'roadmin76' not in users2
            # User admins cannot CRUD user admins
            assert user2 not in users2

            assert len(users2) == 7

        finally:
            # Cleanup the users created by the restore
            users = [user["id"] for user in backup.get("users", [])]
            for u in users:
                testlib.ensure_deleted(
                    self.cluster, f'/settings/rbac/users/local/{u}')

            # Cleanup the users created by this test
            testlib.ensure_deleted(
                    self.cluster, f'/settings/rbac/users/local/{user}')
            testlib.ensure_deleted(
                    self.cluster, f'/settings/rbac/users/local/{user2}')

    # Restore a backup taken on a 7.6.x system containing roles that
    # are security roles. This is done by a user without a security role
    # and so the result should be that none of the security roles get
    # restored.
    def unsecure_restore_from_older_release_backup_file_test(self):
        # File is a result of /setting/rbac/backup on 7.6.x system.
        backup_file_path = os.path.join(testlib.get_resources_dir(), "fixtures",
                                        "full_backup.json")
        with open(backup_file_path) as f:
            backup = json.load(f)

        try:
            # Create a user with the backup_admin + local_user_admin roles
            user = 'BackupLocalUserAdmin'
            name = testlib.random_str(10)
            password = testlib.random_str(10)
            put_user(self.cluster, 'local', user, password,
                     roles='backup_admin,user_admin_local',
                     full_name=name, groups='', validate_user_props=True)
            restore(self.cluster, backup, can_overwrite=False,
                    expected_counters={'usersCreated': 6,
                                       'usersSkipped': 6,
                                       'groupsCreated': 0,
                                       'groupsSkipped': 0},
                    auth_user=(user, password))

        finally:
            # Cleanup the users created by the restore
            users = [user["id"] for user in backup.get("users", [])]
            for u in users:
                testlib.ensure_deleted(
                        self.cluster, f'/settings/rbac/users/local/{u}')

            # Cleanup the admin created by this test
            testlib.ensure_deleted(
                    self.cluster, f'/settings/rbac/users/local/{user}')


    def local_and_external_user_admin_restore_test(self):
        # The backup file contains the 'Administrator' user, one local
        # user 'eventingAdmin' and one external user 'gauss'.
        backup_file_path = os.path.join(testlib.get_resources_dir(), "fixtures",
                                        "backup_with_users.json")
        with open(backup_file_path) as f:
            backup = json.load(f)

        try:
            # Create a local user admin
            local_admin = 'LocalUserAdmin'
            local_admin_password = testlib.random_str(10)
            put_user(self.cluster, 'local', local_admin, local_admin_password,
                     roles='user_admin_local',
                     full_name=testlib.random_str(10),
                     groups='', validate_user_props=True)

            # Create an external user admin
            external_admin = 'ExternalUserAdmin'
            external_admin_password = testlib.random_str(10)
            put_user(self.cluster, 'local', external_admin,
                     external_admin_password, roles='user_admin_external',
                     full_name=testlib.random_str(10),
                     groups='', validate_user_props=True)

            # The local user admin can only restore the local user
            restore(self.cluster, backup, can_overwrite=False,
                    expected_counters={'usersCreated': 1,
                                       'usersSkipped': 2,
                                       'groupsCreated': 0,
                                       'groupsSkipped': 0},
                    auth_user=(local_admin, local_admin_password))

            # Use 'Administrator' to get list of users and verify the
            # expected users are present/not present
            backup_results = testlib.get_succ(self.cluster,
                                              '/settings/rbac/backup').json()
            users = [user["id"] for user in backup_results.get("users", [])]
            assert 'eventingAdmin' in users
            assert 'gauss' not in users

            # Remove the restored 'eventingAdmin'
            testlib.ensure_deleted(
                    self.cluster, '/settings/rbac/users/local/eventingAdmin')

            # The external user admin can only restore the external user
            restore(self.cluster, backup, can_overwrite=False,
                    expected_counters={'usersCreated': 1,
                                       'usersSkipped': 2,
                                       'groupsCreated': 0,
                                       'groupsSkipped': 0},
                    auth_user=(external_admin, external_admin_password))

            # Use 'Administrator' to get list of users and verify the
            # expected users are present/not present
            backup_results = testlib.get_succ(self.cluster,
                                              '/settings/rbac/backup').json()
            users = [user["id"] for user in backup_results.get("users", [])]
            assert 'eventingAdmin' not in users
            assert 'gauss' in users

        finally:
            # Clean up the users restored by the test
            testlib.ensure_deleted(
                    self.cluster, '/settings/rbac/users/local/eventingAdmin')
            testlib.ensure_deleted(
                    self.cluster, '/settings/rbac/users/external/gauss')

            # Clean up the user created by the test
            testlib.ensure_deleted(
                    self.cluster, f'/settings/rbac/users/local/{local_admin}')
            testlib.ensure_deleted(
                    self.cluster,
                    f'/settings/rbac/users/local/{external_admin}')


    # Operations involving a backup containing a group with an ldap reference.
    def restore_group_with_ldap_ref_test(self):
        # The backup file contains the 'Adminstrator' user, one group with
        # a ldap reference and one group without a ldap reference.
        backup_file_path = os.path.join(testlib.get_resources_dir(), "fixtures",
                                        "backup_with_ldap_group.json")
        with open(backup_file_path) as f:
            backup = json.load(f)

        try:
            # Create a local user admin
            local_admin = 'LocalUserAdmin'
            local_admin_password = testlib.random_str(10)
            put_user(self.cluster, 'local', local_admin, local_admin_password,
                     roles='user_admin_local',
                     full_name=testlib.random_str(10), groups='',
                     validate_user_props=True)

            # Create an external user admin
            external_admin = 'ExternalUserAdmin'
            external_admin_password = testlib.random_str(10)
            put_user(self.cluster, 'local', external_admin,
                     external_admin_password, roles='user_admin_external',
                     full_name=testlib.random_str(10),
                     groups='', validate_user_props=True)

            # The local user admin cannot restore the group with the ldap
            # reference.
            restore(self.cluster, backup, can_overwrite=False,
                    expected_counters={'usersCreated': 0,
                                       'usersSkipped': 1,
                                       'groupsCreated': 1,
                                       'groupsSkipped': 1},
                    auth_user=(local_admin, local_admin_password))

            # Delete the restored group
            testlib.ensure_deleted(self.cluster,
                                   '/settings/rbac/groups/testgroup')

            # The external user admin is able to restore the group with the
            # ldap reference as well as the one without
            restore(self.cluster, backup, can_overwrite=False,
                    expected_counters={'usersCreated': 0,
                                       'usersSkipped': 1,
                                       'groupsCreated': 2,
                                       'groupsSkipped': 0},
                    auth_user=(external_admin, external_admin_password))

        finally:
            # Cleanup the users created by this test
            testlib.ensure_deleted(
                    self.cluster, f'/settings/rbac/users/local/{local_admin}')
            testlib.ensure_deleted(
                    self.cluster,
                    f'/settings/rbac/users/local/{external_admin}')
            # Delete the groups that were created
            testlib.ensure_deleted(self.cluster,
                                   '/settings/rbac/groups/testgroup')
            testlib.ensure_deleted(self.cluster,
                                   '/settings/rbac/groups/externalGroup')

    # Verify backing up and then restoring a role doesn't perform any
    # substitution as the version of the backup is the same. If the back
    # is taken on an older release the role substitution might occur.
    def backup_restore_user_without_substitution_test(self):
        def get_roles(name):
            user = testlib.get_succ(self.cluster,
                                    f"/settings/rbac/users/local/{name}").json()
            roles = [role['role'] for role in user.get('roles', [])]
            roles.sort()
            return roles

        put_user(self.cluster, 'local', 'readonlyadmin',
                 password=testlib.random_str(8), roles='ro_admin')
        roles_before = get_roles('readonlyadmin')
        backup = testlib.get_succ(self.cluster, '/settings/rbac/backup').json()
        testlib.ensure_deleted(self.cluster,
                               '/settings/rbac/users/local/readonlyadmin')

        # Simulate an older version by removing the compat info.
        old_backup = deepcopy(backup)
        old_backup.pop("compat_version", None)
        res = testlib.put_succ(self.cluster, '/settings/rbac/backup',
                               data={'backup': json.dumps(old_backup),
                                     'canOverwrite': 'false'}, **{}).json()
        old_backup_roles = get_roles('readonlyadmin')
        # Ensure the roles were upgraded for backwards compatibility or
        # for new role (ui_access).
        assert old_backup_roles == ['ro_admin', 'ro_security_admin',
                                    'ui_access']
        testlib.ensure_deleted(self.cluster,
                               '/settings/rbac/users/local/readonlyadmin')

        # Simulate a 8.0 version
        old_backup["compat_version"] = "8.0"
        res = testlib.put_succ(self.cluster, '/settings/rbac/backup',
                               data={'backup': json.dumps(old_backup),
                                     'canOverwrite': 'false'}, **{}).json()
        old_backup_roles = get_roles('readonlyadmin')
        # Ensure new role (ui_access) was added for backwards compatibility.
        assert old_backup_roles == ['ro_admin', 'ui_access']
        testlib.ensure_deleted(self.cluster,
                               '/settings/rbac/users/local/readonlyadmin')

        # Now use the "real" backup containing the compat info.
        res = testlib.put_succ(self.cluster, '/settings/rbac/backup',
                               data={'backup': json.dumps(backup),
                                     'canOverwrite': 'false'}, **{}).json()
        # Ensure no role changes were made
        roles_after = get_roles('readonlyadmin')
        assert roles_after == ['ro_admin']
        testlib.ensure_deleted(self.cluster,
                               '/settings/rbac/users/local/readonlyadmin')

        # Set compat version to something "from the future"
        old_backup["compat_version"] = "12.4"
        res = testlib.put_fail(self.cluster, '/settings/rbac/backup',
                               expected_code=400,
                               data={'backup': json.dumps(old_backup),
                                     'canOverwrite': 'false'}, **{}).json()
        assert (res["errors"]["backup"]["compat_version"] ==
                "Cannot restore a backup with cluster version '12.4' "
                "as it is greater than what is supported (cluster compat "
                "version is '8.1').")

        # Set compat version to something earlier than is supported. This
        # would only occur if someone manually edited the backup results.
        old_backup["compat_version"] = "7.6"
        res = testlib.put_fail(self.cluster, '/settings/rbac/backup',
                               expected_code=400,
                               data={'backup': json.dumps(old_backup),
                                     'canOverwrite': 'false'}, **{}).json()
        assert (res["errors"]["backup"]["compat_version"] ==
                "Cannot restore a backup with cluster version '7.6' as "
                "it predates any supported release.")

        # Set compat version to garbage
        old_backup["compat_version"] = "invalid&^%*2"
        res = testlib.put_fail(self.cluster, '/settings/rbac/backup',
                               expected_code=400,
                               data={'backup': json.dumps(old_backup),
                                     'canOverwrite': 'false'}, **{}).json()
        assert (res["errors"]["backup"]["compat_version"] ==
                'Found invalid cluster version: "invalid&^%*2".')

        # Set compat version to an unsupported type (should be string)
        old_backup["compat_version"] = {"test": 4}
        res = testlib.put_fail(self.cluster, '/settings/rbac/backup',
                               expected_code=400,
                               data={'backup': json.dumps(old_backup),
                                     'canOverwrite': 'false'}, **{}).json()
        assert (res["errors"]["backup"]["compat_version"] ==
                'Value must be json string')


def verify_roles(cluster, username, expected_roles):
    path = f'/settings/rbac/users/local/{username}'
    user_info = testlib.get_succ(cluster, path).json()
    user_roles = [role["role"] for role in user_info.get("roles", [])]

    for r in expected_roles:
        assert r in user_roles, f"Missing {r} in {user_roles}"


def restore(cluster, backup, expected_counters=None, can_overwrite=False,
            auth_user=None):
    can_overwrite_str = 'true' if can_overwrite else 'false'
    kwargs = {"auth": auth_user} if auth_user is not None else {}
    res = testlib.put_succ(cluster, '/settings/rbac/backup',
                           data={'backup': json.dumps(backup),
                                 'canOverwrite': can_overwrite_str},
                           **kwargs).json()

    assert 'stats' in res
    assert 'usersSkipped' in res
    assert 'usersOverwritten' in res
    assert 'groupsSkipped' in res
    assert 'groupsOverwritten' in res
    assert 'usersCreated' in res['stats']
    assert 'usersSkipped' in res['stats']
    assert 'usersOverwritten' in res['stats']
    assert 'groupsCreated' in res['stats']
    assert 'groupsSkipped' in res['stats']
    assert 'groupsOverwritten' in res['stats']

    for k in res['stats']:
        expected = expected_counters[k] if k in expected_counters else 0
        assert res['stats'][k] == expected,\
               f'invalid \'{k}\' counter returned by PUT /backup ('\
               f'got: {res[k]}, expected: {expected})'

    assert res['stats']['usersSkipped'] == len(res['usersSkipped'])
    assert res['stats']['usersOverwritten'] == len(res['usersOverwritten'])
    assert res['stats']['groupsSkipped'] == len(res['groupsSkipped'])
    assert res['stats']['groupsOverwritten'] == len(res['groupsOverwritten'])


def put_group(cluster, group, description):
    testlib.put_succ(cluster, f'/settings/rbac/groups/{group}',
                     data={'roles': 'admin', 'description': description})


def check_user_pass(cluster, username, password):
    testlib.get_succ(cluster, '/pools/default', auth=(username, password))


def check_user_pass_fail(cluster, username, password, status=401):
    testlib.get_fail(cluster, '/pools/default', status,
                     auth=(username, password))


def check_group_description(cluster, group, description):
    res = testlib.get_succ(cluster, f'/settings/rbac/groups/{group}').json()
    assert 'description' in res
    got = res['description']
    assert got == description, \
           f'group {group} has unexpected description ' \
           f'(got: {got}, expected: {description})'
