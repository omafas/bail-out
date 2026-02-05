# @author Couchbase <info@couchbase.com>
# @copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import requests

import testlib
from testlib import Service

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.exceptions import AuthenticationException


class UsersTestSet(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(
            include_services=[Service.KV,
                              Service.QUERY,
                              Service.CBAS],
            # i.e. wait for service up
            balanced=True,
            num_vbuckets=16,
            buckets=[{"name": "test",
                      "ramQuota": 100,
                      "storageBackend": "couchstore"}])

    def setup(self):
        self.user = testlib.random_str(10)

    def teardown(self):
        post_activity(self.cluster,
                      {'enabled': False,
                       'trackedRoles': [],
                       'trackedGroups': []})

    def test_teardown(self):
        testlib.ensure_deleted(
          self.cluster, f'/settings/rbac/users/local/{self.user}')

    def local_user_create_update_delete_test(self):
        user = self.user
        name1 = testlib.random_str(10)
        password1 = testlib.random_str(10)
        assert_wrong_password(self.cluster, user, password1)

        # User creation
        put_user(self.cluster, 'local', user, password=password1,
                 roles='admin', full_name=name1, groups='',
                 validate_user_props=True)
        assert_authn_and_roles(self.cluster, user, password1, ['admin'])

        # Change password
        password2 = testlib.random_str(10)
        change_user_password(self.cluster, user, password1, password2)
        assert_wrong_password(self.cluster, user, password1)
        assert_authn_and_roles(self.cluster, user, password2, ['admin'])

        # Update user
        name2 = testlib.random_str(10)
        put_user(self.cluster, 'local', user, password=None,
                 roles='ro_admin,admin', full_name=name2,
                 validate_user_props=True)
        # Password should still work
        assert_authn_and_roles(self.cluster, user, password2,
                               ['ro_admin', 'admin'])

        # Update password via user update
        password3 = testlib.random_str(10)
        put_user(self.cluster, 'local', user, password=password3,
                 roles='ro_admin,admin', full_name=name2,
                 validate_user_props=True)
        assert_wrong_password(self.cluster, user, password2)
        assert_authn_and_roles(self.cluster, user, password3,
                               ['ro_admin', 'admin'])

        # Delete user
        delete_user(self.cluster, 'local', user)
        assert_wrong_password(self.cluster, user, password3)

    def local_user_lock_unlock_test(self):
        user = self.user
        name = testlib.random_str(10)
        password = testlib.random_str(10)

        # User creation
        put_user(self.cluster, 'local', user, password=password,
                 roles='admin,views_admin[test]', full_name=name, groups='')
        assert_not_locked(self.cluster, user, password)

        # Start UI session
        session, headers, node = start_ui_session(self.cluster, user, password)

        # Use UI session
        testlib.get_succ(node, '/pools/default/buckets/test', headers=headers,
                         session=session, auth=None)

        # Test query endpoint
        testlib.get_succ(self.cluster, '/admin/vitals', service=Service.QUERY,
                         expected_code=200, auth=(user, password))

        # Test CBAS endpoint
        testlib.get_succ(self.cluster, '/analytics/admin/active_requests',
                         service=Service.CBAS, expected_code=200,
                         auth=(user, password))

        # Test views endpoint
        testlib.get_succ(self.cluster, '/test', service=Service.VIEWS,
                         expected_code=404, auth=(user, password))

        # Start SDK/KV session
        kv_url = self.cluster.connected_nodes[0].service_url(Service.KV)
        auth = PasswordAuthenticator(user, password)
        sdk_cluster = assert_sdk_pass(kv_url, auth)

        # SDK/KV session connected
        assert_sdk_connected(sdk_cluster)

        # Lock user via PATCH
        lock_user(self.cluster, user)
        assert_locked(self.cluster, user, password)

        # UI session terminated with status 401, such that the UI correctly
        # shows as logged out
        testlib.get_fail(node, '/pools', headers=headers,
                         session=session, expected_code=401, auth=None)

        # Query service gives authentication failure error
        testlib.get_fail(self.cluster, '/admin/vitals', service=Service.QUERY,
                         expected_code=401, auth=(user, password))

        # CBAS gives authentication failure error
        testlib.get_fail(self.cluster, '/analytics/admin/active_requests',
                         service=Service.CBAS, expected_code=401,
                         auth=(user, password))

        # Views error
        testlib.get_fail(self.cluster, '/test', service=Service.VIEWS,
                         expected_code=401, auth=(user, password))

        # SDK/KV session disconnected
        assert_sdk_disconnected(sdk_cluster)

        # SDK/KV new connection fails
        assert_sdk_fail(kv_url, auth)

        # Unlock user via PATCH
        unlock_user(self.cluster, user)
        assert_not_locked(self.cluster, user, password)

        # SDK/KV passes
        assert_sdk_pass(kv_url, auth)

        # SDK/KV session connected
        assert_sdk_connected(sdk_cluster)

        # Lock via PUT
        put_user(self.cluster, 'local', user, password=password,
                 roles='admin', full_name=name, groups='', locked="true")
        assert_locked(self.cluster, user, password)

        # SDK/KV error
        assert_sdk_fail(kv_url, auth)

        # SDK/KV new connection fails
        assert_sdk_fail(kv_url, auth)

        # Unlock via PUT
        put_user(self.cluster, 'local', user, password=password,
                 roles='admin', full_name=name, groups='', locked="false")
        assert_not_locked(self.cluster, user, password)

        # SDK/KV passes
        assert_sdk_pass(kv_url, auth)

        # SDK/KV session connected
        assert_sdk_connected(sdk_cluster)

    def patch_user_errors_test(self):
        # Can't lock an invalid user
        testlib.patch_fail(self.cluster,
                           '/settings/rbac/users/local/@invalid_user',
                           data={"locked": "true"},
                           expected_code=400)
        # Can't lock an unknown user
        testlib.patch_fail(self.cluster,
                           '/settings/rbac/users/local/unknown_user',
                           data={"locked": "true"},
                           expected_code=404)
        # Can't change the password
        testlib.patch_fail(self.cluster,
                           '/settings/rbac/users/local/unknown_user',
                           data={"password": "password"},
                           expected_code=404)

    def expired_user_test(self):
        user = self.user
        name1 = testlib.random_str(10)
        password1 = testlib.random_str(10)
        assert_wrong_password(self.cluster, user, password1)

        # User creation
        put_user(self.cluster, 'local', user, password=password1,
                 roles='admin', full_name=name1, groups='',
                 validate_user_props=True)
        assert_password_not_expired(self.cluster, user, password1)

        # Set password to temporary and test that this can be reverted
        put_user(self.cluster, 'local', user,
                 roles='admin', full_name=name1, groups='',
                 temporary_password="true", validate_user_props=True)
        assert_password_expired(self.cluster, user, password1)

        put_user(self.cluster, 'local', user,
                 roles='admin', full_name=name1, groups='',
                 temporary_password="false", validate_user_props=True)
        assert_password_not_expired(self.cluster, user, password1)

        # Set password to temporary again
        put_user(self.cluster, 'local', user, password=password1,
                 roles='admin', full_name=name1, groups='',
                 temporary_password="true", validate_user_props=True)
        assert_password_expired(self.cluster, user, password1)

        # UI session should fail
        node = self.cluster.connected_nodes[0]  # Need consistent node for UI
        session = requests.Session()
        headers = {'Host': testlib.random_str(8), 'ns-server-ui': 'yes'}
        testlib.post_fail(node, '/uilogin', headers=headers,
                          session=session, auth=None, expected_code=403,
                          data={'user': user, 'password': password1})

        # Changing password to the existing value should fail
        r = change_user_password(self.cluster, user, password1, password1,
                                 expected_code=400)
        testlib.assert_eq(
            {
                "errors": {
                    "password": "Password has already been used."
                }
            }, r.json())
        assert_password_expired(self.cluster, user, password1)

        # Change password
        password2 = testlib.random_str(10)
        change_user_password(self.cluster, user, password1, password2)

        # New password should work
        assert_password_not_expired(self.cluster, user, password2)

        # Start UI session
        session, headers, node = start_ui_session(self.cluster, user, password2)

        # UI session works
        testlib.get_succ(node, '/pools', headers=headers,
                         session=session, auth=None)

        # Delete user
        delete_user(self.cluster, 'local', user)
        assert_wrong_password(self.cluster, user, password2)

    def activity_tracking_test(self):
        user = self.user
        name1 = testlib.random_str(10)
        password1 = testlib.random_str(10)

        roles = 'admin'

        # User creation
        put_user(self.cluster, 'local', user, password=password1,
                 roles=roles, full_name=name1, groups='',
                 validate_user_props=True)

        # Enable activity tracking without yet covering this user
        post_activity(self.cluster, {'enabled': True,
                                     'trackedRoles': [],
                                     'trackedGroups': []})

        testlib.get_succ(self.cluster, '/pools/default',
                         auth=(user, password1))

        sync_activity(self.cluster)

        # No activity
        testlib.assert_eq(is_activity_in_ets(self.cluster), False)
        testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

        # Enable activity tracking directly with trackedRoles
        post_activity(self.cluster,
                      {'enabled': True,
                       'trackedRoles': ['admin'],
                       'trackedGroups': []})

        testlib.get_succ(self.cluster, '/pools/default',
                         auth=(user, password1))

        sync_activity(self.cluster)

        # New activity
        testlib.assert_eq(is_activity_in_ets(self.cluster), True)
        testlib.assert_eq(get_activity_for_user(self.cluster, user) is not None,
                          True)

        # Disable activity tracking to test clearing of activity
        post_activity(self.cluster,
                      {'enabled': False,
                       'trackedRoles': [],
                       'trackedGroups': []})

        sync_activity(self.cluster)

        # No activity
        testlib.assert_eq(is_activity_in_ets(self.cluster), False)
        testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

        # Re-enable activity to test that old activity doesn't return
        post_activity(self.cluster,
                      {'enabled': True,
                       'trackedRoles': ['admin'],
                       'trackedGroups': []})

        sync_activity(self.cluster)

        # No activity
        testlib.assert_eq(is_activity_in_ets(self.cluster), False)
        testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

        # Remove the tracked role from the user to test activity clearing
        put_user(self.cluster, 'local', user, password=password1,
                 roles=None, full_name=name1, groups='',
                 validate_user_props=True)

        sync_activity(self.cluster)

        # No activity
        testlib.assert_eq(is_activity_in_ets(self.cluster), False)
        testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

        # Add the tracked role back to the user to test that old activity
        # doesn't return
        put_user(self.cluster, 'local', user, password=password1,
                 roles=roles, full_name=name1, groups='',
                 validate_user_props=True)

        sync_activity(self.cluster)

        # No activity
        testlib.assert_eq(is_activity_in_ets(self.cluster), False)
        testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

    def activity_tracking_with_group_role_test(self):
        user = self.user
        name1 = testlib.random_str(10)
        password1 = testlib.random_str(10)
        roles = 'data_reader[*]'  # Basic role for /pools/default
        group = "test_group"

        testlib.put_succ(self.cluster, f'/settings/rbac/groups/{group}',
                         data={'roles': ''})
        try:
            # User creation
            put_user(self.cluster, 'local', user, password=password1,
                     roles=roles, full_name=name1, groups=group,
                     validate_user_props=True)

            # Enable activity tracking without yet covering this user
            post_activity(self.cluster,
                          {'enabled': True,
                           'trackedRoles': ['admin'],
                           'trackedGroups': []})

            testlib.get_succ(self.cluster, '/pools/default',
                             auth=(user, password1))

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

            # Enable activity tracking by adding a tracked role to the group
            testlib.put_succ(self.cluster, f'/settings/rbac/groups/{group}',
                             data={'roles': 'admin'})

            testlib.get_succ(self.cluster, '/pools/default',
                             auth=(user, password1))

            sync_activity(self.cluster)

            # New activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), True)
            testlib.assert_eq(get_activity_for_user(self.cluster, user)
                              is not None, True)

            # Disable activity tracking for that role to test activity clearing
            post_activity(self.cluster,
                          {'enabled': True,
                           'trackedRoles': [],
                           'trackedGroups': []})

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

            # Re-enable tracking role to test that old activity doesn't return
            post_activity(self.cluster,
                          {'enabled': True,
                           'trackedRoles': ['admin'],
                           'trackedGroups': []})

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

            # Remove the tracked role from the group to test activity clearing
            testlib.put_succ(self.cluster, f'/settings/rbac/groups/{group}',
                             data={'roles': ''})

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

            # Add the tracked role back to the group to test that old activity
            # doesn't return
            testlib.put_succ(self.cluster, f'/settings/rbac/groups/{group}',
                             data={'roles': 'admin'})

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)
        finally:
            testlib.ensure_deleted(self.cluster,
                                   f'/settings/rbac/groups/{group}')

    def activity_tracking_with_group_test(self):
        user = self.user
        name1 = testlib.random_str(10)
        password1 = testlib.random_str(10)
        roles = 'data_reader[*]'  # Basic role for /pools/default
        group = "test_group"

        testlib.put_succ(self.cluster, f'/settings/rbac/groups/{group}',
                         data={'roles': ''})
        try:
            # User creation
            put_user(self.cluster, 'local', user, password=password1,
                     roles=roles, full_name=name1, groups='',
                     validate_user_props=True)

            # Enable activity tracking without yet covering this user
            post_activity(self.cluster,
                          {'enabled': True,
                           'trackedRoles': [],
                           'trackedGroups': [group]})

            testlib.get_succ(self.cluster, '/pools/default',
                             auth=(user, password1))

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

            # Enable activity tracking by adding a tracked group to the user
            put_user(self.cluster, 'local', user, password=password1,
                     roles=roles, full_name=name1, groups=group,
                     validate_user_props=True)

            testlib.get_succ(self.cluster, '/pools/default',
                             auth=(user, password1))

            sync_activity(self.cluster)

            # New activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), True)
            testlib.assert_eq(get_activity_for_user(self.cluster, user)
                              is not None, True)

            # Disable activity tracking for that group to test activity clearing
            post_activity(self.cluster,
                          {'enabled': True,
                           'trackedRoles': [],
                           'trackedGroups': []})

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

            # Re-enable tracking group to test that old activity doesn't return
            post_activity(self.cluster,
                          {'enabled': True,
                           'trackedRoles': [],
                           'trackedGroups': [group]})

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

            # Remove the tracked group from the user to test activity clearing
            put_user(self.cluster, 'local', user, password=password1,
                     roles=roles, full_name=name1, groups='',
                     validate_user_props=True)

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)

            # Add the tracked group back to the user to test that old activity
            # doesn't return
            put_user(self.cluster, 'local', user, password=password1,
                     roles=roles, full_name=name1, groups=group,
                     validate_user_props=True)

            sync_activity(self.cluster)

            # No activity
            testlib.assert_eq(is_activity_in_ets(self.cluster), False)
            testlib.assert_eq(get_activity_for_user(self.cluster, user), None)
        finally:
            testlib.ensure_deleted(self.cluster,
                                   f'/settings/rbac/groups/{group}')

    def admin_user_lock_unlock_test(self):
        admin_auth = self.cluster.auth
        admin_user, admin_password = admin_auth

        # Start UI session
        session, headers, node = start_ui_session(self.cluster, admin_user,
                                                  admin_password)

        # Use UI session
        testlib.get_succ(node, '/pools', headers=headers,
                         session=session, auth=None)

        # Test query endpoint
        testlib.get_succ(self.cluster, '/admin/vitals', service=Service.QUERY,
                         expected_code=200)

        # Test CBAS endpoint
        testlib.get_succ(self.cluster, '/analytics/admin/active_requests',
                         service=Service.CBAS, expected_code=200)

        # Test views endpoint
        testlib.get_succ(self.cluster, '/test', service=Service.VIEWS,
                         expected_code=404)
        try:
            # Lock admin
            lock_admin(self.cluster)
            testlib.get_fail(self.cluster, '/pools/default', expected_code=401)

            # UI session terminated with status 401, such that the UI correctly
            # shows as logged out
            testlib.get_fail(node, '/pools', headers=headers,
                             session=session, expected_code=401, auth=None)

            # Query service gives authentication failure error
            testlib.get_fail(self.cluster, '/admin/vitals',
                             service=Service.QUERY, expected_code=401)

            # CBAS gives authentication failure error
            testlib.get_fail(self.cluster, '/analytics/admin/active_requests',
                             service=Service.CBAS, expected_code=401)

            # Views error
            testlib.get_fail(self.cluster, '/test', service=Service.VIEWS,
                             expected_code=401)
        finally:
            # Unlock admin
            unlock_admin(self.cluster)
            testlib.get_succ(self.cluster, '/pools/default')

    # This test verifies a user with user_admin_local cannot create
    # a user in a group that has an admin role. This would be a privilege
    # escalation.
    def prevent_role_elevation_test(self):
        user = "localuseradmin"
        name = testlib.random_str(10)
        password = testlib.random_str(10)
        put_user(self.cluster, 'local', user, password=password,
                 roles='user_admin_local', full_name=name, groups='',
                 validate_user_props=True)
        assert_authn_and_roles(self.cluster, user, password,
                               ['user_admin_local'])

        # Create a secure group
        testlib.put_succ(self.cluster, f'/settings/rbac/groups/securegroup',
                         data={'roles': 'admin'})

        # Try to create a user in the secure group. This will fail as a
        # 'user_admin_local' role cannot create a user with an 'admin'
        # role...even doing it indirectly via a 'group'.
        testlib.put_fail(self.cluster,
                         f'/settings/rbac/users/local/securityAdminFail',
                         403, data={'groups': 'securegroup',
                                    'password': testlib.random_str(10)},
                         auth=(user, password))

        # Delete user and group
        delete_user(self.cluster, 'local', user)
        testlib.delete_succ(self.cluster,
                            f'/settings/rbac/groups/securegroup')


    # This test verifies a user with user_admin_external role cannot
    # get info for a local user.
    def witness_for_mb65113_test(self):
        user = "externaluseradmin"
        name = testlib.random_str(10)
        password = testlib.random_str(10)
        put_user(self.cluster, 'local', user, password=password,
                 roles='user_admin_external', full_name=name, groups='',
                 validate_user_props=True)

        # Create a local user.
        user2 = "eventingadmin"
        name2 = testlib.random_str(10)
        password2 = testlib.random_str(10)
        put_user(self.cluster, 'local', user2, password=password2,
                 roles='eventing_admin', full_name=name2, groups='',
                 validate_user_props=True)

        # Verify info on local user cannot be obtained
        testlib.get_fail(self.cluster,
                         f'/settings/rbac/users/local/{user2}',
                         expected_code=403, auth=(user, password))

        # Clean up users created by this test
        delete_user(self.cluster, 'local', user)
        delete_user(self.cluster, 'local', user2)

    def user_admin_role_test(self):
        try:
            user = 'localUserAdmin'
            name = testlib.random_str(10)
            password = testlib.random_str(10)
            put_user(self.cluster, 'local', user, password,
                     roles='user_admin_local',
                     full_name=name, validate_user_props=True)
            assert_authn_and_roles(self.cluster, user, password,
                                   ['user_admin_local'])

            # 'user_admin_local' isn't allowed to "elevate" its role
            data = build_payload(roles='admin,ro_admin', password=password,
                                 full_name=name)
            testlib.put_fail(self.cluster, f'/settings/rbac/users/local/{user}',
                             403, data=data, auth=(user, password))

            # 'user_admin_local' is allowed to add a user
            newuser = testlib.random_str(10)
            data = build_payload(roles='eventing_admin',
                                 password=testlib.random_str(10),
                                 full_name=newuser)
            testlib.put_succ(self.cluster,
                             f'/settings/rbac/users/local/{newuser}',
                             data=data, auth=(user, password))

            # ...and is allowed to modify a user's role as long as it's not
            # being "elevated" to a security role
            data['roles'] = 'eventing_admin, cluster_admin'
            testlib.put_succ(self.cluster,
                             f'/settings/rbac/users/local/{newuser}',
                             data=data, auth=(user, password))

            # ...but cannot modify a user to "elevate" it's role to a security
            # role
            data['roles'] = 'security_admin'
            testlib.put_fail(self.cluster,
                             f'/settings/rbac/users/local/{newuser}',
                             403, data=data, auth=(user, password))

            # ...and allowed to delete a user
            testlib.delete_succ(self.cluster,
                                f'/settings/rbac/users/local/{newuser}',
                                auth=(user, password))

            # Create an 'admin' user
            admin_user = 'FullAdmin'
            admin_name = testlib.random_str(10)
            admin_password = testlib.random_str(10)
            put_user(self.cluster, 'local', admin_user, admin_password,
                     roles='admin', full_name=admin_name,
                     validate_user_props=True)
            assert_authn_and_roles(self.cluster, admin_user, admin_password,
                                   ['admin'])

            # 'user_admin_local' is not allowed to delete a user with a
            # security role.
            testlib.delete_fail(self.cluster,
                                f'/settings/rbac/users/local/{admin_user}',
                                403, auth=(user, password))
        finally:
            # Clean up the users created by this test
            delete_user(self.cluster, 'local', user)
            delete_user(self.cluster, 'local', admin_user)

    def security_admin_role_test(self):
        try:
            # Create a user with 'security_admin' role
            user = 'securityAdmin'
            name = testlib.random_str(10)
            password = testlib.random_str(10)
            put_user(self.cluster, 'local', user, password,
                     roles='security_admin', full_name=name,
                     validate_user_props=True)
            assert_authn_and_roles(self.cluster, user, password,
                                   ['security_admin'])

            # 'security_admin' cannot add a user...
            name2 = testlib.random_str(10)
            password2 = testlib.random_str(10)
            data = build_payload(roles='cluster_admin', password=password2,
                                 full_name=name2)
            testlib.put_fail(self.cluster,
                             f'/settings/rbac/users/local/{name2}',
                             403, data=data, auth=(user, password))

            # 'security_admin' cannot promote itself
            data = build_payload(roles='admin', full_name=name,
                                 password=password)
            testlib.put_fail(self.cluster, f'/settings/rbac/users/local/{user}',
                             403, data=data, auth=(user, password))

            # Create a user (note: not being done by 'security_admin'
            user3 = 'anotherUser'
            name3 = testlib.random_str(10)
            password3 = testlib.random_str(10)
            put_user(self.cluster, 'local', user3, password3, roles='ro_admin',
                     full_name=name3, validate_user_props=True)
            assert_authn_and_roles(self.cluster, user3, password3,
                                   ['ro_admin'])

            # 'security_admin' cannot modify the user
            data = build_payload(roles='eventing_admin', full_name=name3,
                                 password=password3)
            testlib.put_fail(self.cluster,
                             f'/settings/rbac/users/local/{user3}',
                             403, data=data, auth=(user, password))

            # 'security_admin' cannot delete the user
            testlib.delete_fail(self.cluster,
                                f'/settings/rbac/users/local/{user3}',
                                403, auth=(user, password))
        finally:
            # Delete the user (note: not being done by 'security_admin'
            delete_user(self.cluster, 'local', user3)

            # Clean up the security_admin
            delete_user(self.cluster, 'local', user)

    def security_user_admin_role_test(self):
        try:
            # Create a user with 'security_admin' and 'user_admin_local' roles.
            user = 'securityUserAdmin'
            name = testlib.random_str(10)
            password = testlib.random_str(10)
            put_user(self.cluster, 'local', user, password,
                     roles='security_admin,user_admin_local',
                     full_name=name, validate_user_props=True)
            assert_authn_and_roles(self.cluster, user, password,
                                   ['security_admin','user_admin_local'])

            # securityUserAdmin is allowed to create a user with a non-security
            # role
            non_secure_user = 'eventingAdmin'
            non_secure_name = testlib.random_str(10)
            non_secure_password = testlib.random_str(10)
            data = build_payload(roles='eventing_admin',
                                 password=non_secure_password,
                                 full_name=non_secure_name)
            testlib.put_succ(self.cluster,
                             f'/settings/rbac/users/local/{non_secure_user}',
                             data=data, auth=(user, password))

            # securityUserAdmin is not allowed to create a user with a security
            # role.
            data = build_payload(roles='security_admin',
                                 password=testlib.random_str(10),
                                 full_name=testlib.random_str(10))
            testlib.put_fail(self.cluster,
                             f'/settings/rbac/users/local/securityAdminFail',
                             403, data=data, auth=(user, password))

            # create an admin user
            admin_user = "fullAdmin"
            admin_password = testlib.random_str(10)

            put_user(self.cluster, 'local', admin_user, password=admin_password,
                     roles='admin', full_name=testlib.random_str(10),
                     validate_user_props=True)
            assert_authn_and_roles(self.cluster, admin_user, admin_password,
                                   ['admin'])

            # securityUserAdmin is not allowed to delete the admin user.
            testlib.delete_fail(self.cluster,
                                f'/settings/rbac/users/local/{admin_user}',
                                403, auth=(user, password))
        finally:
            # Clean up users created in this test
            delete_user(self.cluster, 'local', user)
            delete_user(self.cluster, 'local', non_secure_user)
            delete_user(self.cluster, 'local', admin_user)


    def external_user_admin_test(self):
        try:
            # Create an external user admin
            user = 'externalUserAdmin'
            name = testlib.random_str(10)
            password = testlib.random_str(10)
            put_user(self.cluster, 'local', user, password,
                     roles='user_admin_external', full_name=name,
                     validate_user_props=True)

            # Create a local user
            user2 = 'eventingAdmin'
            name2 = testlib.random_str(10)
            password2 = testlib.random_str(10)
            put_user(self.cluster, 'local', user2, password2,
                     roles='eventing_admin', full_name=name2,
                     validate_user_props=True)

            # The external user admin cannot patch a local user
            testlib.patch_fail(self.cluster,
                               f'/settings/rbac/users/local/{user2}',
                               403,
                               data={"locked": "true"},
                               auth=(user, password))

        finally:
            # Delete the created users
            delete_user(self.cluster, 'local', user)
            delete_user(self.cluster, 'local', user2)


    # This tests ensures the 'user_admin_local' and 'user_admin_external'
    # roles cannot manage (CRUD) each other.
    def user_admin_conflicting_test(self):
        try:
            # Create local user admin
            local_user_admin = 'localUserAdmin'
            local_user_admin_password = testlib.random_str(10)
            put_user(self.cluster, 'local', local_user_admin,
                     password=local_user_admin_password,
                     roles='user_admin_local',
                     full_name=testlib.random_str(10),
                     validate_user_props=True)

            # Create external user admin
            external_user_admin = 'externalUserAdmin'
            put_user(self.cluster, 'local', external_user_admin,
                     password=testlib.random_str(10),
                     roles='user_admin_external',
                     full_name=testlib.random_str(10),
                     validate_user_props=True)

            # Create a group with an external user admin role and a backup
            # admin role.
            external_user_admin_group = 'externalUserAdminGroup'
            testlib.put_succ(
                    self.cluster,
                    f'/settings/rbac/groups/{external_user_admin_group}',
                    data={'roles': 'user_admin_external, backup_admin'})

            # Create user in group containing an external user admin role.
            # The backup_admin is specified as a role but is also a role
            # contained in the group. This is to show dedup'ing works.
            user_in_group = 'userInGroup'
            put_user(self.cluster, 'local', user_in_group,
                     password=testlib.random_str(10),
                     roles='eventing_admin, backup_admin',
                     groups=f'{external_user_admin_group}',
                     full_name=testlib.random_str(10),
                     validate_user_props=True)

            # Run tests on the above users/group...

            # "local user admin" cannot get an existing "external user admin"
            testlib.get_fail(
                    self.cluster,
                    f'/settings/rbac/users/local/{external_user_admin}',
                    403, auth=(local_user_admin, local_user_admin_password))

            # "local user admin" cannot delete an existing "external user admin"
            testlib.delete_fail(
                    self.cluster,
                    f'/settings/rbac/users/local/{external_user_admin}',
                    403, auth=(local_user_admin, local_user_admin_password))

            # "local user admin" cannot patch an existing "external user admin"
            testlib.patch_fail(
                    self.cluster,
                    f'/settings/rbac/users/local/{external_user_admin}',
                    403, data={"locked": "true"},
                    auth=(local_user_admin, local_user_admin_password))

            # "local user admin" cannot get a user in a group containing an
            # "external user admin" role
            testlib.get_fail(
                    self.cluster,
                    f'/settings/rbac/users/local/{user_in_group}', 403,
                    auth=(local_user_admin, local_user_admin_password))

            # "local user admin" cannot delete a user in a group containing an
            # "external user admin" role
            testlib.delete_fail(
                    self.cluster,
                    f'/settings/rbac/users/local/{user_in_group}', 403,
                    auth=(local_user_admin, local_user_admin_password))

            # "local user admin" cannot patch a user in a group containing an
            # "external user admin" role
            testlib.patch_fail(
                    self.cluster,
                    f'/settings/rbac/users/local/{user_in_group}', 403,
                    auth=(local_user_admin, local_user_admin_password))

            # "local user admin" cannot create a new "external user admin"
            data = build_payload(roles='user_admin_external',
                                 password=testlib.random_str(10),
                                 full_name=testlib.random_str(10))
            testlib.put_fail(self.cluster,
                             '/settings/rbac/users/local/wontgetcreated',
                             403, data=data,
                             auth=(local_user_admin, local_user_admin_password))

            # "local user admin" cannot create a new user in group containing
            # an "external user admin" role
            data = build_payload(roles='eventing_admin',
                                 groups=f'{external_user_admin_group}',
                                 password=testlib.random_str(10),
                                 full_name=testlib.random_str(10))
            testlib.put_fail(self.cluster,
                             '/settings/rbac/users/local/wontgetcreated',
                             403, data=data,
                             auth=(local_user_admin, local_user_admin_password))

        finally:
            # Clean up users/group created by this test
            delete_user(self.cluster, 'local', local_user_admin)
            delete_user(self.cluster, 'local', external_user_admin)
            delete_user(self.cluster, 'local', user_in_group)
            testlib.ensure_deleted(
                    self.cluster,
                    f'/settings/rbac/groups/{external_user_admin_group}')


    def cluster_admin_role_test(self):
        try:
            # Create a cluster admin
            user = 'clusterAdmin'
            name = testlib.random_str(10)
            password = testlib.random_str(10)
            put_user(self.cluster, 'local', user, password,
                     roles='cluster_admin', full_name=name,
                     validate_user_props=True)

            # The cluster admin cannot create a user. And as the permission
            # is the same for other user CRUD operations we don't have to
            # individually test them.
            data = build_payload(roles='eventing_admin',
                                 password=testlib.random_str(10),
                                 full_name=testlib.random_str(10))
            testlib.put_fail(self.cluster,
                             '/settings/rbac/users/local/wontgetcreated',
                             403, data=data, auth=(user, password))
        finally:
            # Delete the created user
            delete_user(self.cluster, 'local', user)


def build_payload(password=None, roles=None, full_name=None, groups=None,
                  locked=None, temporary_password=None):
    data = {}
    if roles is not None:
        data['roles'] = roles
    if password is not None:
        data['password'] = password
    if full_name is not None:
        data['name'] = full_name
    if groups is not None:
        data['groups'] = groups
    if locked is not None:
        data['locked'] = locked
    if temporary_password is not None:
        data['temporaryPassword'] = temporary_password

    return data


def put_user(cluster_or_node, domain, userid, password=None, roles=None,
             full_name=None, groups=None, locked=None, temporary_password=None,
             validate_user_props=False):
    data = build_payload(password, roles, full_name, groups, locked,
                         temporary_password)
    testlib.put_succ(cluster_or_node,
                     f'/settings/rbac/users/{domain}/{userid}',
                     data=data)
    if validate_user_props:
        r = testlib.get_succ(cluster_or_node,
                             f'/settings/rbac/users/{domain}/{userid}')
        r = r.json()
        testlib.assert_eq(r['id'], userid)
        testlib.assert_eq(r['domain'], domain)
        testlib.assert_eq(r['name'], full_name)
        if groups is None or groups == '':
            expected_groups = []
            group_roles = []
        else:
            expected_groups = [g.strip() for g in groups.split(',')]
            group_roles = get_roles_for_groups(cluster_or_node, groups)
            if roles is None:
                roles = group_roles
            else:
                roles = [r.strip() for r in roles.split(',')]
                # Dedup the roles if needed
                roles = list(set(roles) | set(group_roles))
            roles = ",".join(map(str, roles))

        if roles is None:
            roles_num = 0
        else:
            roles_num = len(roles.split(','))
        testlib.assert_eq(len(r['roles']), roles_num)
        testlib.assert_eq(sorted(r['groups']), sorted(expected_groups))
        assert 'password_change_date' in r, \
               f'password_change_date is missing in user props: {r}'


def delete_user(cluster_or_node, domain, userid):
    testlib.delete_succ(cluster_or_node,
                        f'/settings/rbac/users/{domain}/{userid}')


def change_user_password(cluster_or_node, user, password, new_password,
                         expected_code=200):
    return testlib.post(cluster_or_node, '/controller/changePassword',
                        data={'password': new_password}, auth=(user, password),
                        expected_code=expected_code)


def lock_user(cluster, user):
    testlib.patch_succ(cluster, '/settings/rbac/users/local/' + user,
                       data={"locked": "true"})


def unlock_user(cluster, user):
    testlib.patch_succ(cluster, '/settings/rbac/users/local/' + user,
                       data={"locked": "false"})


def lock_admin(cluster):
    node = cluster.connected_nodes[0]
    token = node.get_localtoken()
    testlib.post_succ(node, '/controller/lockAdmin',
                      auth=("@localtoken", token))


def unlock_admin(cluster):
    node = cluster.connected_nodes[0]
    token = node.get_localtoken()
    testlib.post_succ(node, '/controller/unlockAdmin',
                      auth=("@localtoken", token))


def sync_activity(cluster):
    testlib.diag_eval(
        cluster.get_node_from_hostname(cluster.get_orchestrator_node()[0]),
        "activity_aggregator ! refresh,"
        "gen_server:call(activity_aggregator, sync)")


def post_activity(cluster, json):
    r = testlib.post_succ(cluster, '/settings/security/userActivity',
                          json=json)
    testlib.assert_eq(r.json(), json)


def is_activity_in_ets(cluster):
    found_activity = False
    for node in cluster.connected_nodes:
        activity = testlib.diag_eval(
            node, "gen_server:call(activity_tracker, last_activity)").text
        if "[]" != activity:
            found_activity = True
    return found_activity


def get_activity_for_user(cluster, user):
    r = testlib.get_succ(cluster, '/settings/rbac/users/local/' + user)
    return r.json().get('last_activity_time')


# For the specified list of groups, return a unique list of roles contained
# in the groups.
def get_roles_for_groups(cluster, groups):
    roles = []
    for group in groups.split(','):
        r = testlib.get_succ(cluster, f'/settings/rbac/groups/{group}')
        r = r.json()
        newroles =  [role_item["role"] for role_item in r.get("roles", [])
                     if "role" in role_item]
        roles = list(set(roles) | set(newroles))

    return roles

def assert_authn_and_roles(cluster_or_node, user, password, expected_roles):
    r = testlib.get_succ(cluster_or_node, '/whoami', auth=(user, password))
    r = r.json()
    got_roles = [r['role'] for r in r['roles']]
    testlib.assert_eq(sorted(got_roles), sorted(expected_roles))


def assert_wrong_password(cluster_or_node, user, password):
    testlib.get_fail(cluster_or_node, '/pools/default', expected_code=401,
                     auth=(user, password))


def assert_locked(cluster, user, password):
    user_info = testlib.get_succ(cluster,
                                 '/settings/rbac/users/local/' + user).json()
    testlib.assert_eq(user_info.get('locked'), True)

    testlib.get_fail(cluster, '/pools/default', expected_code=401,
                     auth=(user, password))


def assert_not_locked(cluster, user, password):
    user_info = testlib.get_succ(cluster,
                                 '/settings/rbac/users/local/' + user).json()
    testlib.assert_eq(user_info.get('locked'), False)

    testlib.get_succ(cluster, '/pools/default',
                     auth=(user, password))


def assert_password_expired(cluster, user, password):
    user_info = testlib.get_succ(cluster,
                                 '/settings/rbac/users/local/' + user).json()
    testlib.assert_eq(user_info.get('temporary_password'), True)

    r = testlib.get_fail(cluster, '/pools/default', expected_code=403,
                         auth=(user, password))
    testlib.assert_eq(r.json(), {"message": "Password expired",
                                 "passwordExpired": True})

    testlib.get_fail(cluster, '/admin/vitals', service=Service.QUERY,
                     expected_code=401, auth=(user, password))


def assert_password_not_expired(cluster, user, password):
    user_info = testlib.get_succ(cluster,
                                 '/settings/rbac/users/local/' + user).json()
    testlib.assert_eq(user_info.get('temporary_password'), False)

    testlib.get_succ(cluster, '/pools/default', auth=(user, password))

    testlib.get_succ(cluster, '/admin/vitals', service=Service.QUERY,
                     auth=(user, password))


def start_ui_session(cluster, user, password):
    node = cluster.connected_nodes[0]  # Need consistent node for UI
    session = requests.Session()
    headers = {'Host': testlib.random_str(8), 'ns-server-ui': 'yes'}
    testlib.post_succ(node, '/uilogin', headers=headers,
                      session=session, auth=None, expected_code=200,
                      data={'user': user, 'password': password})
    return session, headers, node


def assert_sdk_pass(kv_url, sdk_auth):
    return testlib.poll_for_condition(
        lambda: test_sdk(kv_url, sdk_auth),
        sleep_time=0.1, attempts=10, msg="Auth expected to pass")


def assert_sdk_connected(sdk_cluster):
    testlib.poll_for_condition(
        lambda: test_sdk_kv(sdk_cluster),
        sleep_time=0.1, attempts=10, msg="Connection expected to work")


def assert_sdk_disconnected(sdk_cluster):
    testlib.poll_for_condition(
        lambda: not test_sdk_kv(sdk_cluster),
        sleep_time=0.1, attempts=10, msg="Connection expected to fail")


def assert_sdk_fail(kv_url, sdk_auth):
    testlib.poll_for_condition(
        lambda: not test_sdk(kv_url, sdk_auth),
        sleep_time=0.1, attempts=10, msg="Auth expected to fail")


def test_sdk(url, auth):
    try:
        return Cluster(url, ClusterOptions(auth))
    except AuthenticationException:
        return False


def test_sdk_kv(sdk_cluster: Cluster):
    try:
        # Simple request just to confirm the connection is usable, we don't need
        # document "x" to actually exist
        sdk_cluster.bucket("test").default_collection().exists("x")
        return True
    except AuthenticationException:
        return False


class RbacCommunityTests(testlib.BaseTestSet):

    def __init__(self, cluster):
        super().__init__(cluster)

    @staticmethod
    def requirements():
        return [testlib.ClusterRequirements(edition='Community',
                                            min_num_nodes=1, num_connected=1,
                                            encryption=False, afamily='ipv4',
                                            exact_services=[Service.KV])]

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        pass

    # Allowed admin user creations on community edition.
    def allowed_admin_user_creation_test(self):
        name = testlib.random_str(10)
        password = testlib.random_str(10)

        for role in ['ro_admin', 'admin', 'bucket_full_access[*]']:
            data = build_payload(roles=role, password=password,
                                 full_name=name)
            testlib.put_succ(self.cluster,
                             f'/settings/rbac/users/local/testuser',
                             data=data)
            testlib.delete_succ(self.cluster,
                                '/settings/rbac/users/local/testuser')

    # Disallowed admin user creations on community edition.
    def disallowed_admin_user_creation_test(self):
        name = testlib.random_str(10)
        password = testlib.random_str(10)

        for role in ['security_admin', 'ro_security_admin]',
                     'user_admin_local', 'user_admin_external',
                     'cluster_admin', 'eventing_admin', 'backup_admin',
                     'views_admin', 'external_stats_reader',
                     'application_telemetry_writer']:
            data = build_payload(roles=role, password=password,
                                 full_name=name)
            testlib.put_fail(self.cluster,
                             f'/settings/rbac/users/local/testuser',
                             400, data=data)
