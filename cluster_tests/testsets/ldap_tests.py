# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import sys

import testlib
from ldap_test import LdapServer


class LdapTests(testlib.BaseTestSet):
    port = 10389
    admin_dn = 'cn=admin,dc=example,dc=com'
    admin_password = 'pass1'
    user = 'ldap_user'
    user1 = 'ldap.user'
    user_password = 'test_password_472956922354'
    group = 'test_group'


    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise")


    def setup(self):
        user_dn = f'cn={LdapTests.user},ou=users,dc=example,dc=com'
        user1_dn = f'cn={LdapTests.user1},ou=users,dc=example,dc=com'
        delay_for_macos = 10 if sys.platform == "darwin" else 0
        self.server = LdapServer({
          'port': LdapTests.port,
          'bind_dn': LdapTests.admin_dn,
          'password': LdapTests.admin_password,
          'base': {'objectclass': ['domain'],
                   'dn': 'dc=example,dc=com',
                   'attributes': {'dc': 'example'}},
          'entries': [
            {'objectclass': 'OrganizationalUnit',
             'dn': 'ou=users,dc=example,dc=com',
             'attributes': {'ou': 'users'}},
            {'objectclass': 'person',
             'dn': user_dn,
             'attributes': {'cn': LdapTests.user,
                            'userPassword': LdapTests.user_password}},
            {'objectclass': 'person',
             'dn': user1_dn,
             'attributes': {'cn': LdapTests.user1,
                            'userPassword': LdapTests.user_password}},
            {'objectclass': 'OrganizationalUnit',
             'dn': 'ou=groups,dc=example,dc=com',
             'attributes': {'ou': 'groups'}},
            {'objectclass': 'GroupOfNames',
             'dn': f'cn={LdapTests.group},ou=groups,dc=example,dc=com',
             'attributes': {'cn': LdapTests.group,
                            'member': [user_dn]}}
          ]}, java_delay=delay_for_macos)
        self.server.start()
        testlib.delete_config_key(self.cluster, 'ldap_settings')


    def test_teardown(self):
        testlib.ensure_deleted(
            self.cluster, f'/settings/rbac/users/external/{LdapTests.user}')
        testlib.ensure_deleted(
            self.cluster, f'/settings/rbac/users/external/{LdapTests.user1}')
        testlib.delete_config_key(self.cluster, 'ldap_settings')


    def teardown(self):
        testlib.ensure_deleted(
            self.cluster, f'/settings/rbac/groups/{LdapTests.group}')
        self.server.stop()


    def basic_set_and_get_test(self):
        actual_defaults = testlib.get_succ(self.cluster,
                                           '/settings/ldap').json()
        expected_defaults = {'authenticationEnabled': False,
                             'authorizationEnabled': False,
                             'hosts': [],
                             'port': 389,
                             'encryption': 'None',
                             'userDNMapping': 'None',
                             'bindDN': '',
                             'bindPass': '',
                             'maxParallelConnections': 100,
                             'maxCacheSize': 10000,
                             'maxGroupCacheSize': 1000,
                             'requestTimeout': 5000,
                             'nestedGroupsEnabled': False,
                             'nestedGroupsMaxDepth': 10,
                             'failOnMaxDepth': False,
                             'cacheValueLifetime': 300000,
                             'middleboxCompMode': True,
                             'serverCertValidation': True}
        assert expected_defaults == actual_defaults, \
                f"Default settings are incorrect {actual_defaults}"
        settings = {'authenticationEnabled': 'true',
                    'authorizationEnabled': 'true',
                    'hosts': 'host1,host2',
                    'port': 636,
                    'encryption': 'TLS',
                    'bindDN': 'cn=test',
                    'bindPass': 'pass',
                    'userDNMapping': '{"query":"dc=example??one?(cn=%u)"}',
                    'middleboxCompMode': 'false',
                    'groupsQuery': 'dc=example??one?(member=%D)'}
        expected = expected_defaults.copy()
        expected.update({'authenticationEnabled': True,
                         'authorizationEnabled': True,
                         'hosts': ['host1', 'host2'],
                         'port': 636,
                         'encryption': 'TLS',
                         'bindDN': 'cn=test',
                         'bindPass': '**********',
                         'userDNMapping': {"query":"dc=example??one?(cn=%u)"},
                         'middleboxCompMode': False,
                         'groupsQuery': 'dc=example??one?(member=%D)'})
        testlib.post_succ(self.cluster, '/settings/ldap', data=settings)
        actual = testlib.get_succ(self.cluster, '/settings/ldap').json()
        assert expected == actual, f"Returned settings are incorrect: {actual}"


    def json_set_and_get_test(self):
        settings = {'authenticationEnabled': True,
                    'authorizationEnabled': True,
                    'hosts': ['host1', 'host2'],
                    'port': 636,
                    'encryption': 'TLS',
                    'bindDN': 'cn=test',
                    'bindPass': 'pass',
                    'userDNMapping': {'query':'dc=example??one?(cn=%u)'},
                    'groupsQuery': 'dc=example??one?(member=%D)',
                    'bindMethod': 'Simple',
                    'maxParallelConnections': 100,
                    'maxCacheSize': 1000,
                    'maxGroupCacheSize': 1000,
                    'cacheValueLifetime': 5000,
                    'requestTimeout': 5000,
                    'nestedGroupsEnabled': False,
                    'nestedGroupsMaxDepth': 10,
                    'failOnMaxDepth': True,
                    'middleboxCompMode': True,
                    'serverCertValidation': True}
        expected = settings.copy()
        expected.update({'bindPass': '**********'})
        testlib.post_succ(self.cluster, '/settings/ldap', json=settings)
        actual = testlib.get_succ(self.cluster, '/settings/ldap').json()
        assert expected == actual, f"Returned settings are incorrect: {actual}"


    def external_user_query_test(self):
        settings = {'authenticationEnabled': 'true',
                    'authorizationEnabled': 'false',
                    'hosts': 'localhost',
                    'port': LdapTests.port,
                    'encryption': 'None',
                    'bindDN': LdapTests.admin_dn,
                    'bindPass': LdapTests.admin_password,
                    'userDNMapping':
                    '{"query":"ou=users,dc=example,dc=com??one?(cn=%u)"}'}
        external_user_test(self, LdapTests.user, settings)


    def external_user_template_test(self):
        settings = {'authenticationEnabled': 'true',
                    'authorizationEnabled': 'false',
                    'hosts': 'localhost',
                    'port': LdapTests.port,
                    'encryption': 'None',
                    'bindDN': LdapTests.admin_dn,
                    'bindPass': LdapTests.admin_password,
                    'userDNMapping':
                    '{"template":"cn=%u,ou=users,dc=example,dc=com"}'}
        external_user_test(self, LdapTests.user, settings)


    def external_user1_query_test(self):
        settings = {'authenticationEnabled': 'true',
                    'authorizationEnabled': 'false',
                    'hosts': 'localhost',
                    'port': LdapTests.port,
                    'encryption': 'None',
                    'bindDN': LdapTests.admin_dn,
                    'bindPass': LdapTests.admin_password,
                    'userDNMapping':
                    '{"query":"ou=users,dc=example,dc=com??one?(cn=%u)"}'}
        external_user_test(self, LdapTests.user1, settings)


    def external_user1_template_test(self):
        settings = {'authenticationEnabled': 'true',
                    'authorizationEnabled': 'false',
                    'hosts': 'localhost',
                    'port': LdapTests.port,
                    'encryption': 'None',
                    'bindDN': LdapTests.admin_dn,
                    'bindPass': LdapTests.admin_password,
                    'userDNMapping':
                    '{"template":"cn=%u,ou=users,dc=example,dc=com"}'}
        external_user_test(self, LdapTests.user1, settings)


    def ldap_group_test(self):
        LDAPSettings = {'authenticationEnabled': 'true',
                        'authorizationEnabled': 'true',
                        'hosts': 'localhost',
                        'port': LdapTests.port,
                        'encryption': 'None',
                        'bindDN': LdapTests.admin_dn,
                        'bindPass': LdapTests.admin_password,
                        'userDNMapping': '{"query":"ou=users,dc=example,dc=com??one?(cn=%u)"}',
                        'groupsQuery': 'ou=groups,dc=example,dc=com??one?(member=%D)'}
        testlib.post_succ(self.cluster, '/settings/ldap', data=LDAPSettings)
        testlib.ensure_deleted(self.cluster, f'/settings/rbac/users/external/{LdapTests.user}')
        testlib.ensure_deleted(self.cluster, f'/settings/rbac/groups/{LdapTests.group}')
        res = testlib.get_succ(self.cluster, '/whoami', auth=(LdapTests.user, LdapTests.user_password))
        assert [] == res.json()['roles']
        testlib.put_succ(self.cluster,
                         f'/settings/rbac/groups/{LdapTests.group}',
                         data={'ldap_group_ref':f'cn={LdapTests.group},ou=groups,dc=example,dc=com',
                               'roles': ''})
        res = testlib.get_succ(self.cluster, '/whoami',
                               auth=(LdapTests.user, LdapTests.user_password))
        assert [] == res.json()['roles']
        testlib.put_succ(self.cluster,
                         f'/settings/rbac/groups/{LdapTests.group}',
                         data={'ldap_group_ref':f'cn={LdapTests.group},ou=groups,dc=example,dc=com',
                               'roles': 'admin'})
        res = testlib.get_succ(self.cluster, '/whoami',
                               auth=(LdapTests.user, LdapTests.user_password))
        assert [{'role': 'admin'}] == res.json()['roles']


    def advanced_mapping_test(self):
        testlib.post_succ(self.cluster, '/settings/ldap',
                          json={'authenticationEnabled': False})
        mapping = [{'match': '^(\\S+)@(\\S+)\\.(\\S+)\\.com$',
                    'substitution': 'cn={0},ou={1},dc={2},dc=com'},
                   {'match': '^(\\S+)@(\\S+)\\.(\\S+)\\.org$',
                    'ldapQuery': 'ou={1},dc={2},dc=com??one?(cn={0})'}]
        LDAPSettings = {'authenticationEnabled': True,
                        'authorizationEnabled': False,
                        'hosts': ['localhost'],
                        'port': LdapTests.port,
                        'encryption': 'None',
                        'bindDN': LdapTests.admin_dn,
                        'bindPass': LdapTests.admin_password,
                        'userDNMapping': {'advanced': mapping}}
        testlib.post_succ(self.cluster, '/settings/ldap', json=LDAPSettings)
        testlib.get_succ(self.cluster, '/whoami',
                         auth=(f'{LdapTests.user}@users.example.com',
                               LdapTests.user_password))
        testlib.get_succ(self.cluster, '/whoami',
                         auth=(f'{LdapTests.user}@users.example.org',
                               LdapTests.user_password))
        testlib.get_fail(self.cluster, '/whoami', 401,
                         auth=(f'{LdapTests.user}@users.example.net',
                               LdapTests.user_password))
        testlib.get_fail(self.cluster, '/whoami', 401,
                         auth=('wrong_username@users.example.com',
                               LdapTests.user_password))


def external_user_test(self, user, ldap_settings):
        testlib.post_succ(self.cluster, '/settings/ldap',
                          data={'authenticationEnabled': 'false'})
        testlib.put_succ(self.cluster,
                         f'/settings/rbac/users/external/{user}',
                         data={'roles': 'admin'})
        testlib.get_fail(self.cluster, '/whoami', 401,
                         auth=(user, LdapTests.user_password))
        testlib.post_succ(self.cluster, '/settings/ldap', data=ldap_settings)
        res = testlib.get_succ(self.cluster,
                               '/whoami',
                               auth=(user, LdapTests.user_password))
        assert [{'role': 'admin'}] == res.json()['roles']
