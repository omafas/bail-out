# @author Couchbase <info@couchbase.com>
# @copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib


class UsersTestSet(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    def setup(self):
        self.user = testlib.random_str(10)
        pass

    def teardown(self):
        pass

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

    # This test verifies a user with security_admin_local cannot create
    # a user in a group that has an admin role. This would be a privilege
    # escalation.
    def prevent_role_elevation_test(self):
        user = "localusersecurityadmin"
        name = testlib.random_str(10)
        password = testlib.random_str(10)
        put_user(self.cluster, 'local', user, password=password,
                 roles='security_admin_local', full_name=name, groups='',
                 validate_user_props=True)
        assert_authn_and_roles(self.cluster, user, password,
                               ['security_admin_local'])

        # Create a secure group
        testlib.put_succ(self.cluster, f'/settings/rbac/groups/securegroup',
                         data={'roles': 'admin'})

        # Try to create a user in the secure group. This will fail as a
        # 'security_admin_local' role cannot create a user with an 'admin'
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


    # This test verifies a user with security_admin_external role cannot
    # get info for a local user.
    def witness_for_mb65113_test(self):
        user = "externalsecurityadmin"
        name = testlib.random_str(10)
        password = testlib.random_str(10)
        put_user(self.cluster, 'local', user, password=password,
                 roles='security_admin_external', full_name=name, groups='',
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


def put_user(cluster_or_node, domain, userid, password=None, roles=None,
             full_name=None, groups=None, validate_user_props=False):
    data = {}
    if roles is not None:
        data['roles'] = roles
    if password is not None:
        data['password'] = password
    if full_name is not None:
        data['name'] = full_name
    if groups is not None:
        data['groups'] = groups
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
        if roles is None:
            role_num = 0
        else:
            roles_num = len(roles.split(','))
        testlib.assert_eq(len(r['roles']), roles_num)
        if groups is None or groups == '':
            expected_groups = []
        else:
            expected_groups = [g.strip() for g in groups.split(',')]
        testlib.assert_eq(sorted(r['groups']), sorted(expected_groups))
        assert 'password_change_date' in r, \
               f'password_change_date is missing in user props: {r}'


def delete_user(cluster_or_node, domain, userid):
    testlib.delete_succ(cluster_or_node,
                        f'/settings/rbac/users/{domain}/{userid}')


def change_user_password(cluster_or_node, user, password, new_password):
    testlib.post_succ(cluster_or_node, '/controller/changePassword',
                      data={'password': new_password}, auth=(user, password))


def assert_authn_and_roles(cluster_or_node, user, password, expected_roles):
    r = testlib.get_succ(cluster_or_node, '/whoami', auth=(user, password))
    r = r.json()
    got_roles = [r['role'] for r in r['roles']]
    testlib.assert_eq(sorted(got_roles), sorted(expected_roles))


def assert_wrong_password(cluster_or_node, user, password):
    testlib.get_fail(cluster_or_node, '/pools/default', expected_code=401,
                     auth=(user, password))
