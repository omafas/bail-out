# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
from testlib import assert_eq, assert_http_code, assert_in
import base64
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from multiprocessing import Process
import time
import requests
from urllib.parse import urlparse, parse_qs, urlunparse

# To remove annoying message from xmlschema:
#  INFO:xmlschema:Resource 'XMLSchema.xsd' is already loaded
import logging
logging.getLogger('xmlschema').setLevel(logging.WARNING)

from saml2 import server
from saml2 import BINDING_HTTP_REDIRECT
from saml2 import BINDING_HTTP_POST
from saml2.saml import AUTHN_PASSWORD
from saml2.saml import NAME_FORMAT_URI
from saml2.saml import NAMEID_FORMAT_PERSISTENT
from saml2.saml import NAMEID_FORMAT_TRANSIENT
from saml2.metadata import create_metadata_string
from saml2.saml import NameID
from contextlib import contextmanager
import html
import re
import datetime
import glob
import sys
from testlib.requirements import Service

debug=False
scriptdir = sys.path[0]
mock_server_port = 8119
mock_server_host = "localhost"
mock_server_url = f"http://{mock_server_host}:{mock_server_port}"
mock_metadata_endpoint = "/mock/metadata"
mock_sso_redirect_url = f"http://{mock_server_host}:{mock_server_port}/mock/auth"
mock_sso_post_url = f"http://{mock_server_host}:{mock_server_port}/mock/auth/post"
mock_slo_redirect_url = f"http://{mock_server_host}:{mock_server_port}/mock/logout"
mock_slo_post_url = f"http://{mock_server_host}:{mock_server_port}/mock/logout/post"
metadataFile = os.path.join(scriptdir, "idp_metadata.xml")
idp_subject_file_path = os.path.join(scriptdir, "idp.subject")
idp_test_username = "testuser"
idp_test_groups = [("testgroup1", "replication_admin"),
                   ("testgroup2", "external_stats_reader"),
                   ("admingroup", "admin")]
idp_test_user_attrs = {"sn": "TestUser",
                       "givenName": "Test",
                       "uid": "testuser",
                       "email": "test@example.com",
                       "displayName": "Test"}
deflate_encoding = "urn:oasis:names:tc:SAML:2.0:bindings:URL-Encoding:DEFLATE"
sp_entity_id = "sp_test_entity"
ui_headers = {'Host': 'some_addr', 'ns-server-ui': 'yes'}

bucket = "test"

class SamlTests(testlib.BaseTestSet):
    services_to_run = [Service.QUERY, Service.BACKUP, Service.CBAS]

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(min_num_nodes=2,
                                           edition="Enterprise",
                                           balanced=True,
                                           num_vbuckets=16,
                                           buckets=[{'name': bucket,
                                                     'ramQuota': 100}],
                                           include_services=
                                           SamlTests.services_to_run)

    def setup(self):
        testlib.put_succ(self.cluster,
                         f'/settings/rbac/users/external/{idp_test_username}',
                         data={'roles': 'admin'})
        for group, roles in idp_test_groups:
            testlib.put_succ(self.cluster,
                             f'/settings/rbac/groups/{group}',
                             data={'roles': roles})


    def teardown(self):
        testlib.ensure_deleted(
          self.cluster,
          f'/settings/rbac/users/external/{idp_test_username}')
        for group in idp_test_groups:
            testlib.ensure_deleted(
              self.cluster,
              f'/settings/rbac/groups/{group}')


    def unsolicited_authn_and_logout_test(self):
        with saml_configured(self.cluster.connected_nodes[0]) as IDP:
            session = requests.Session()
            _, name_id = send_unsolicited_authn(IDP, session)
            check_access(session, self.cluster.connected_nodes[0], 200)

            binding_out, destination = \
                IDP.pick_binding("single_logout_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)

            logout_id, logout_req = IDP.create_logout_request(destination,
                                                              sp_entity_id,
                                                              name_id=name_id,
                                                              sign=True)

            r = post_saml_request(destination, logout_req, session,
                                  expected_code=200)

            (redirect_url, saml_response) = \
                extract_saml_message_from_form('SAMLResponse', r.text)
            assert_eq(redirect_url, mock_slo_post_url, 'redirect_url')
            IDP.parse_logout_request_response(saml_response, binding=BINDING_HTTP_POST)


    def unsolicited_authn_with_static_metadata_multinode_test(self):
        # Testing the case when saml is configured via settings_node,
        # and user authenticates using auth_node_url (different node)
        settings_node = self.cluster.connected_nodes[0]
        auth_node = self.cluster.connected_nodes[1]
        auth_node_url = auth_node.url

        # Also test the case when idp metadata is uplodaded by admin (static)
        with saml_configured(settings_node,
                             idpMetadataOrigin='upload',
                             idpMetadataURL=None,
                             spBaseURLType='custom',
                             spCustomBaseURL=auth_node_url) as IDP:
            session = requests.Session()
            send_unsolicited_authn(IDP, session)
            check_access(session, auth_node, 200)

            # Change some settings to invalidate metadata cache
            testlib.post_succ(settings_node, '/settings/saml',
                              json={'idpSignsMetadata': False}),
            testlib.post_succ(settings_node, '/settings/saml',
                              json={'idpSignsMetadata': True}),

            # Try authentication one more time to make sure that change of
            # saml settings is not breaking anything (for example, we have
            # metadata cached now, so we want to make sure it gets updated
            # successfully)
            session = requests.Session()
            send_unsolicited_authn(IDP, session)
            check_access(session, auth_node, 200)


    def unsolicited_authn_wrong_issuer_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             idpMetadataOrigin='upload',
                             idpMetadataURL=None,
                             assertion_issuer="wrong") as IDP:
            session = requests.Session()
            r, name_id = send_unsolicited_authn(IDP, session)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session, r)
            assert_in(f'Unexpected assertion issuer ("wrong")', error_msg)
            check_access(session, self.cluster.connected_nodes[0], 401)


    def unsolicited_authn_ignore_wrong_issuer_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             idpMetadataOrigin='upload',
                             idpMetadataURL=None,
                             spVerifyIssuer=False,
                             assertion_issuer="wrong") as IDP:
            session = requests.Session()
            r, name_id = send_unsolicited_authn(IDP, session)
            check_access(session, self.cluster.connected_nodes[0], 200)


    def authn_via_post_and_single_logout_test(self):
        with saml_configured(self.cluster.connected_nodes[0]) as IDP:
            r = testlib.get_succ(self.cluster, '/saml/auth',
                                 allow_redirects=False)
            (redirect_url, saml_request) = \
                extract_saml_message_from_form('SAMLRequest', r.text)
            assert_eq(redirect_url, mock_sso_post_url, 'redirect_url')
            parsed = IDP.parse_authn_request(saml_request,
                                             BINDING_HTTP_POST)
            saml_request = parsed.message
            assert_eq(saml_request.protocol_binding, BINDING_HTTP_POST,
                      'binding')
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[saml_request.protocol_binding],
                                 entity_id=saml_request.issuer.text,
                                 request=saml_request)

            resp_args = IDP.response_args(saml_request)
            name_id = NameID(text=testlib.random_str(16))
            identity = idp_test_user_attrs.copy()
            response = IDP.create_authn_response(
                         identity, userid=idp_test_username,
                         sign_assertion=True,
                         sign_response=True,
                         name_id=name_id,
                         **resp_args)

            session = requests.Session()
            post_saml_response(destination, response, session,
                               expected_code=302)

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=200)

            # Test that SAML users can access the docs endpoint (which requires
            # making a request to memcached on their behalf).
            # Polling is required because external authentication can take up
            # to a second to get enabled in memcached after SAML is enabled, on
            # all nodes. If we wanted to make enabling SAML synchronous, we
            # would need to wait for the connection on all nodes. For now,
            # we use polling in the test instead
            testlib.poll_for_condition(
                lambda: ui_request('get', self.cluster.connected_nodes[0],
                                   f'/pools/default/buckets/{bucket}/docs',
                                   session).status_code == 200,
                sleep_time=0.5,
                timeout=60)

            r = ui_request('post', self.cluster.connected_nodes[0], '/uilogout',
                            session, expected_code=400).json()

            assert_in('redirect', r)
            assert_eq(r['redirect'], '/saml/deauth', 'redirect')

            r = ui_request('get', self.cluster.connected_nodes[0],
                           '/saml/deauth', session, expected_code=200)

            (redirect_url, saml_logout_request) = \
                extract_saml_message_from_form('SAMLRequest', r.text)
            assert_eq(redirect_url, mock_slo_post_url, 'redirect_url')
            parsed_logout_req = IDP.parse_logout_request(saml_logout_request,
                                                         BINDING_HTTP_POST)
            assert_eq(parsed_logout_req.message.name_id.text, name_id.text,
                      'name_id')

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=401)
            logout_response = IDP.create_logout_response(
                                  parsed_logout_req.message,
                                  bindings=[BINDING_HTTP_POST])
            binding_out, destination = \
                IDP.pick_binding("single_logout_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            post_saml_response(destination, response, session,
                               expected_code=302)


    def authn_via_redirect_and_regular_logout_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             idpAuthnBinding="redirect",
                             spSignRequests=False,
                             singleLogoutEnabled=False) as IDP:
            r = testlib.get_fail(self.cluster, '/saml/auth', 302,
                                 allow_redirects=False)
            assert_in('Location', r.headers)
            location = r.headers['Location']
            assert location.startswith(mock_sso_redirect_url), \
                   f'location("{location}") must start with ' \
                   f'"{mock_sso_redirect_url}"'
            parsedLocation = urlparse(location)
            params = parse_qs(parsedLocation.query)
            assert_in('SAMLEncoding', params)
            assert_eq(params['SAMLEncoding'], [deflate_encoding],
                      'SAMLEncoding')
            assert_in('SAMLRequest', params)
            parsed = IDP.parse_authn_request(params['SAMLRequest'][0],
                                             BINDING_HTTP_REDIRECT)
            saml_request = parsed.message
            # We ask to always reply in POST, because REDIRECT can't be used for
            # authn responses.
            assert_eq(saml_request.protocol_binding, BINDING_HTTP_POST,
                      'binding')
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[saml_request.protocol_binding],
                                 entity_id=saml_request.issuer.text,
                                 request=saml_request)

            resp_args = IDP.response_args(saml_request)

            identity = idp_test_user_attrs.copy()
            response = IDP.create_authn_response(
                         identity, userid=idp_test_username,
                         sign_assertion=True,
                         sign_response=True,
                         **resp_args)

            session = requests.Session()
            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=401)
            post_saml_response(destination, response, session,
                               expected_code=302)
            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=200)
            ui_request('post', self.cluster.connected_nodes[0],
                       '/uilogout', session, expected_code=200)
            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=401)


    def session_expiration_test(self):
        with saml_configured(self.cluster.connected_nodes[0]) as IDP:
            identity = idp_test_user_attrs.copy()
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            name_id = NameID(text=testlib.random_str(16))

            expiration = datetime.datetime.utcnow() - \
                         datetime.timedelta(minutes=1)
            expiration_iso = expiration.replace(microsecond=0).isoformat()

            response = IDP.create_authn_response(
                         identity,
                         None, # InResponseTo is missing cause it is
                               # an unsolicited response
                         destination,
                         sp_entity_id=sp_entity_id,
                         userid=idp_test_username,
                         name_id=name_id,
                         sign_assertion=True,
                         sign_response=True,
                         authn={'class_ref': AUTHN_PASSWORD},
                         session_not_on_or_after=expiration_iso)

            session = requests.Session()
            post_saml_response(destination, response, session,
                               expected_code=302)

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=401)


    def reuse_assertion_test(self):
        # Disable recipient verification because we want to check assertion
        # duplicate rejection on both nodes. If we don't disable it,
        # the assertion will be rejected with reason "bad_recipient", which is
        # not what we want to test here
        # Also disable envelop signature verification in order to make sure that
        # assertion signature check is enough in this case
        with saml_configured(self.cluster.connected_nodes[0],
                             spSignRequests=False,
                             spVerifyAssertionEnvelopSig=False,
                             spVerifyRecipient='false') as IDP:
            identity = idp_test_user_attrs.copy()
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            name_id = NameID(text=testlib.random_str(16))

            expiration = datetime.datetime.utcnow() + \
                         datetime.timedelta(minutes=1)
            expiration_iso = expiration.replace(microsecond=0).isoformat()

            response = IDP.create_authn_response(
                         identity,
                         None, # InResponseTo is missing cause it is
                               # an unsolicited response
                         destination,
                         sp_entity_id=sp_entity_id,
                         userid=idp_test_username,
                         name_id=name_id,
                         sign_assertion=True,
                         sign_response=True,
                         authn={'class_ref': AUTHN_PASSWORD},
                         session_not_on_or_after=expiration_iso)

            session1 = requests.Session()
            post_saml_response(destination, response, session1,
                               expected_code=302)

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session1, expected_code=200)

            # sending the same assertion again and expect it to reject it
            session2 = requests.Session()
            r = post_saml_response(destination, response, session2)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session2, r)
            assert_in("assertion replay protection", error_msg)

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session2, expected_code=401)

            # alter assertion id and retry
            # dupe check will not help in this case but signature verification
            # should catch it
            session3 = requests.Session()
            response_wrong_id = re.sub('Assertion Version="2\\.0" ID="id-',
                                       'Assertion Version="2.0" ID="id1-',
                                       response)
            assert response_wrong_id != response
            r = post_saml_response(destination, response_wrong_id, session3)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session3, r)
            assert_in("bad assertion digest", error_msg)

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session3, expected_code=401)

            # sending the same assertion again, but this time to another node
            # it still should reject it
            session4 = requests.Session()
            dest_parsed = urlparse(destination)
            node2_parsed = urlparse(self.cluster.connected_nodes[1].url)
            dest2_parsed = dest_parsed._replace(netloc=node2_parsed.netloc)
            destination2 = urlunparse(dest2_parsed)
            r = post_saml_response(destination2, response, session4)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[1], session4, r)
            assert_in("assertion replay protection", error_msg)

            ui_request('get', self.cluster.connected_nodes[1],
                       '/pools/default', session4, expected_code=401)


    def expired_assertion_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             # Moving NotOnOrAfter back to one minute
                             assertion_lifetime=-1,
                             # .. and allowing max clock skew 30 seconds
                             spClockSkewS=30) as IDP:
            session = requests.Session()
            r, name_id = send_unsolicited_authn(IDP, session)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session, r)

            assert_in('expired SAML assertion', error_msg)

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=401)


    def clock_skew_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             # Moving NotOnOrAfter back to one minute
                             assertion_lifetime=-1,
                             # .. and allowing max clock skew 80 seconds which
                             # is more than 1 minute
                             spClockSkewS=80) as IDP:
            session = requests.Session()
            send_unsolicited_authn(IDP, session)
            check_access(session, self.cluster.connected_nodes[0], 200)


    def groups_and_roles_attributes_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             groupsAttribute='groups',
                             groupsAttributeSep=', ',
                             groupsFilterRE='testgroup\\d+',
                             rolesAttribute='roles',
                             rolesAttributeSep=';',
                             rolesFilterRE='analytics_.*') as IDP:
            identity = idp_test_user_attrs.copy()
            identity["groups"] = "test1, admingroup, test2, testgroup1, "\
                                 "test3, testgroup2"
            # We don't expect analytics_admin to be used because separator is $;
            # We don't expect admin to be used because it should be filtered out
            # by the roles filter
            identity["roles"] = ["unknown;analytics_reader;admin",
                                 "test,analytics_admin;analytics_unknown",
                                 "analytics_manager[*]"]
            identity["uid"] = "testuser2" # so we don't have such user in cb
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            name_id = NameID(text=testlib.random_str(16))

            response = IDP.create_authn_response(
                         identity,
                         None, # InResponseTo is missing cause it is
                               # an unsolicited response
                         destination,
                         sp_entity_id=sp_entity_id,
                         userid=idp_test_username,
                         name_id=name_id,
                         sign_assertion=True,
                         sign_response=True)

            session = requests.Session()
            post_saml_response(destination, response, session,
                               expected_code=302)

            r = ui_request('get', self.cluster.connected_nodes[0],
                           '/whoami', session, expected_code=200)
            roles = [a["role"] for a in r.json()["roles"]]
            roles.sort()
            expected_roles = ['analytics_manager', 'analytics_reader',
                              'external_stats_reader', 'replication_admin']
            assert_eq(roles, expected_roles)

            # MB-62465: Test that SAML users can access the docs endpoint (which
            # requires making a request to memcached on their behalf).
            # Polling is required because external authentication can take up
            # to a second to get enabled in memcached after SAML is enabled.
            testlib.poll_for_condition(
                lambda: ui_request('get', self.cluster.connected_nodes[0],
                                   f'/pools/default/buckets/{bucket}/docs',
                                   session).status_code == 200,
                sleep_time=0.5,
                timeout=60)

            # MB-62604: Query uses cbauth IsAllowed to determine whether it
            # has cluster.n1ql.meta!read permission. replication_admin role
            # grants it. This will fail without the UI session info in context.
            # @cbq-engine "GET /_cbauth/checkPermission?audit=true&context=ui...
            # &domain=external&permission=cluster.n1ql.meta%21read&
            # user=testuser2"
            ui_request('get', self.cluster.connected_nodes[0],
                       '/_p/query/admin/vitals', session, expected_code=200)

            # testuser2 doesn't have permissions to do backup reads.
            ui_request('get', self.cluster.connected_nodes[0],
                       '/_p/backup/api/v1/plan',
                       session, expected_code=403)

            # MB-62604, MB-63208: Query uses cbauth GetBuckets to determine the
            # accessible buckets. replication_admin role makes all buckets
            # accessible. Without UI session info in context, no buckets will be
            # returned.
            # @cbq-engine "GET /_cbauth/getUserBuckets?audit=true&context=ui...
            # &domain=external&user=testuser2"
            r = ui_request('post', self.cluster.connected_nodes[0],
                           '/_p/query/query/service',
                           session,
                           data={'statement': 'select * from system:buckets;'},
                           expected_code=200)
            bkts = []
            for x in r.json()['results']:
                bkts.append(x['buckets']['name'])
                assert bkts == [f'{bucket}']

            # MB-62604, MB-63214: cbas doesn't use cbauth. It uses a combination
            # of /pools/default/checkPermissions, /_cbauth/checkPermission. cbas
            # parses cb-on-behalf-extras headers and populates context (similar
            # to cbauth) before calling ns_server /_cbauth/checkPermission.

            # Create analytics collection in test._default._default. This will
            # fail - we don't have cluster.analytics!manage. Analytics and full
            # admin roles have the permission (see admin_test).
            testlib.poll_for_condition(
                lambda: ui_request('post', self.cluster.connected_nodes[0],
                                   '/_p/cbas/query/service',
                                   session,
                                   data={'statement':
                                         'alter collection '
                                         f'`{bucket}`.`_default`.`_default` '
                                         'enable analytics;'}).
                status_code == 403,
                sleep_time=0.5,
                timeout=60,
                retry_on_assert=True)

            # Exercise goxdcr_rest:proxy(), which also populates cb-on-behalf
            # headers. (The previous test cases populate cb-on-behalf headers in
            # the pluggable UI endpoint (i.e. /_p/<service>).
            # @goxdcr "GET /_cbauth/checkPermission?audit=true&context=ui...
            # &domain=external&permission=cluster.xdcr.remote_clusters%21read
            # &user=testuser2"
            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default/remoteClusters',
                       session,
                       expected_code=200)

    def groups_and_roles_admin_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             groupsAttribute='groups',
                             groupsAttributeSep=', ',
                             groupsFilterRE='admin') as IDP:
            identity = idp_test_user_attrs.copy()
            identity["groups"] = "test2, admingroup, testgroup1, "\
                "test3, testgroup2"
            identity["uid"] = "adminuser" # so we don't have such user in cb
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            name_id = NameID(text=testlib.random_str(16))

            response = IDP.create_authn_response(
                identity,
                None, # InResponseTo is missing cause it is
                # an unsolicited response
                         destination,
                sp_entity_id=sp_entity_id,
                userid=idp_test_username,
                name_id=name_id,
                sign_assertion=True,
                sign_response=True)

            session = requests.Session()
            post_saml_response(destination, response, session,
                               expected_code=302)

            # All 403 test cases in groups_and_roles_attributes_test should
            # pass now as full admin.
            r = ui_request('get', self.cluster.connected_nodes[0],
                           '/whoami', session, expected_code=200)
            roles = [a["role"] for a in r.json()["roles"]]
            roles.sort()
            expected_roles = ['admin']
            assert_eq(roles, expected_roles)

            # Backup pluggable UI request
            ui_request('get', self.cluster.connected_nodes[0],
                       '/_p/backup/api/v1/plan',
                       session, expected_code=200)

            # Create analytics collection in test._default._default.
            testlib.poll_for_condition(
                lambda: ui_request('post', self.cluster.connected_nodes[0],
                                   '/_p/cbas/query/service',
                                   session,
                                   data={'statement': 'create analytics '
                                   'collection myCollection ON '
                                   f'`{bucket}`.`_default`.`_default`'}).
                status_code == 200,
                sleep_time=0.5,
                timeout=60,
                retry_on_assert=True)

            # Query analytics collections. They should exist.
            testlib.poll_for_condition(
                lambda: ui_request('post', self.cluster.connected_nodes[0],
                                   '/_p/cbas/query/service',
                                   session,
                                   data={'statement': 'select * from '
                                         'myCollection'}).
                status_code == 200,
                sleep_time=0.5,
                timeout=60,
                retry_on_assert=True)

            # Disable analytics collection in test._default._default as cleanup.
            testlib.poll_for_condition(
                lambda: ui_request('post', self.cluster.connected_nodes[0],
                                   '/_p/cbas/query/service',
                                   session,
                                   data={'statement': 'drop analytics '
                                         'collection myCollection'}).
                status_code == 200,
                sleep_time=0.5,
                timeout=60,
                retry_on_assert=True)

    # Successfull authentication, but user doesn't have access to UI
    def access_denied_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             usernameAttribute='uid',
                             groupsAttribute='groups',
                             groupsAttributeSep=', ',
                             groupsFilterRE='fakegroup.*',
                             rolesAttribute='roles',
                             rolesAttributeSep=';',
                             rolesFilterRE='analytics.*') as IDP:
            identity = idp_test_user_attrs.copy()
            identity["uid"] = "testuser2" # so we don't have such user in cb
            identity["groups"] = "test1, admingroup, test2, fakegroup1, "\
                                 "test3, fakegroup2"
            identity["roles"] = "unknown"
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)

            response = IDP.create_authn_response(
                         identity,
                         None, # InResponseTo is missing cause it is
                               # an unsolicited response
                         destination,
                         sp_entity_id=sp_entity_id,
                         userid=idp_test_username,
                         name_id=NameID(text=testlib.random_str(16)),
                         sign_assertion=True,
                         sign_response=True)

            session = requests.Session()
            r = post_saml_response(destination, response, session)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session, r)
            expected = 'Access denied for user "testuser2": ' \
                       'Insufficient Permissions. ' \
                       'Extracted groups: fakegroup1, fakegroup2. ' \
                       'Extracted roles: <empty>'
            assert_eq(error_msg, expected)


    def metadata_with_invalid_signature_test(self):
        try:
            # trusted fingerprints will not match mockidp2* certs
            with saml_configured(self.cluster.connected_nodes[0],
                                 metadata_certs_prefix="mockidp2_"):
                assert False, "ns_server should reject metadata as it's " \
                              "signed by untrusted cert"
        except AssertionError as e:
            assert_in(
                "metadata signature verification failed: cert_not_accepted",
                str(e))


    def assertion_with_invalid_signature_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             spVerifyAssertionSig=True,
                             spVerifyAssertionEnvelopSig=False,
                             metadata_certs_prefix="mockidp_",
                             certs_prefix="mockidp2_") as IDP:
            identity = idp_test_user_attrs.copy()
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            name_id = NameID(text=testlib.random_str(16))

            response = IDP.create_authn_response(
                         identity,
                         None,
                         destination,
                         sp_entity_id=sp_entity_id,
                         userid=idp_test_username,
                         name_id=name_id,
                         sign_assertion=True,
                         sign_response=False)

            session = requests.Session()
            r = post_saml_response(destination, response, session)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session, r)
            assert_in("certificate is not trusted", error_msg)

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=401)


    def authn_response_with_invalid_signature_test(self):
        with saml_configured(self.cluster.connected_nodes[0],
                             spVerifyAssertionSig=False,
                             spVerifyAssertionEnvelopSig=True,
                             metadata_certs_prefix="mockidp_",
                             certs_prefix="mockidp2_") as IDP:
            identity = idp_test_user_attrs.copy()
            binding_out, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            name_id = NameID(text=testlib.random_str(16))

            response = IDP.create_authn_response(
                         identity,
                         None,
                         destination,
                         sp_entity_id=sp_entity_id,
                         userid=idp_test_username,
                         name_id=name_id,
                         sign_assertion=False,
                         sign_response=True)

            session = requests.Session()
            r = post_saml_response(destination, response, session)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session, r)
            assert_in("certificate is not trusted", error_msg)

            ui_request('get', self.cluster.connected_nodes[0],
                       '/pools/default', session, expected_code=401)


    def reject_large_saml_response_test(self):
        with saml_configured(self.cluster.connected_nodes[0]) as IDP:
            session = requests.Session()
            _, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            # Send a saml response that's one byte larger than the default
            # configured max value (256 KiB).
            large_response = '\x01' * (256 * 1024 + 1)

            r = post_saml_response(destination, large_response, session)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session, r)
            assert_in("SAML response larger than max configured size",
                      error_msg)


    def reject_non_default_max_saml_response_size_test(self):
        max_size = 2 * 256 * 1024 # 512KiB
        with saml_configured(self.cluster.connected_nodes[0],
                             spSAMLResponseMaxSize=max_size) as IDP:
            session = requests.Session()
            _, destination = \
                IDP.pick_binding("assertion_consumer_service",
                                 bindings=[BINDING_HTTP_POST],
                                 entity_id=sp_entity_id)
            # Send a saml response that's one byte larger than the
            # configured max value (512 KiB).
            large_response = '\x01' * (max_size + 1)

            r = post_saml_response(destination, large_response, session)
            error_msg = catch_error_after_redirect(
                self.cluster.connected_nodes[0], session, r)
            assert_in("SAML response larger than max configured size",
                      error_msg)


    def reject_saml_response_size_max_setting_test(self):
        min_size = 256 * 1024
        max_size = 1024 * 1024

        settings = {'spSAMLResponseMaxSize': min_size - 1}
        testlib.post_fail(self.cluster,
                          "/settings/saml",
                          expected_code=400,
                          json=settings)

        settings = {'spSAMLResponseMaxSize': max_size + 1}
        testlib.post_fail(self.cluster,
                          "/settings/saml",
                          expected_code=400,
                          json=settings)


@contextmanager
def saml_configured(node, assertion_issuer=None, **kwargs):
    mock_server_process = None
    metadata_origin = kwargs['idpMetadataOrigin'] \
                      if 'idpMetadataOrigin' in kwargs \
                      else 'http'
    try:
        metadata = generate_mock_metadata(node, **kwargs)
        if metadata_origin != 'upload':
            with open(metadataFile, 'wb') as f:
                f.write(metadata.encode("utf-8"))
            mock_server_process = Process(target=start_mock_server)
            mock_server_process.start()
            wait_mock_server(f'http://{mock_server_host}:{mock_server_port}/ping', 150)
        else:
            kwargs['idpMetadata'] = metadata
        set_sso_options(node, **kwargs)
        new_kwargs = kwargs.copy()
        if assertion_issuer is not None:
            new_kwargs['idp_entity_id'] = assertion_issuer
        IDP = server.Server(idp_config(node, **new_kwargs))
        yield IDP
    finally:
        if mock_server_process is not None:
            mock_server_process.terminate()
        for idp_subject_file in glob.glob(idp_subject_file_path + "*"):
            os.remove(idp_subject_file)
        if os.path.exists(metadataFile):
            os.remove(metadataFile)
        testlib.delete_succ(node, '/settings/saml')


def generate_mock_metadata(node, metadata_certs_prefix=None, **kwargs):
    if metadata_certs_prefix is not None:
        kwargs['certs_prefix'] = metadata_certs_prefix
    cfg = idp_config(node, **kwargs)
    cfg['metadata'] = {} ## making sure it will not try connecting to ns_server
                         ## when server below is being created, because saml
                         ## configuration in ns_server is not created yet
    IDP = server.Server(cfg)
    valid_for = 1 # hours
    return create_metadata_string(None, config=IDP.config, valid=valid_for,
                                  sign=True)


def start_mock_server():
    mockServer = HTTPServer((mock_server_host, mock_server_port),
                            MockIDPMetadataHandler)
    mockServer.serve_forever()


def wait_mock_server(url, retry):
    while retry > 0:
      try:
          return requests.get(url)
      except requests.exceptions.ConnectionError:
          time.sleep(0.2)
          retry -= 1
    raise RuntimeError('Mock server wait failed')


class MockIDPMetadataHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == mock_metadata_endpoint:
            self.send_response(200)
            self.send_header("Content-type", "application/samlmetadata+xml")
            self.end_headers()
            with open(metadataFile, 'rb') as f:
                md = f.read()
                self.wfile.write(md)
        elif self.path == "/ping":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'pong')
        else:
            raise RuntimeError('unhandled endpoint')

    def log_message(self, format, *args):
        if debug:
            super().log_message(format, *args)
        return


def set_sso_options(node, **kwargs):
    cert_path = os.path.join(testlib.get_resources_dir(), "saml",
                             "mocksp_cert.pem")
    with open(cert_path, 'r') as f:
        cert_pem = f.read()

    key_path = os.path.join(testlib.get_resources_dir(), "saml",
                            "mocksp_key.pem")
    with open(key_path, 'r') as f:
        key_pem = f.read()

    idpcert_fp_path = os.path.join(testlib.get_resources_dir(), "saml",
                                   "mockidp_cert_fingerprints.pem")
    with open(idpcert_fp_path, 'r') as f:
        trusted_fps = f.read()
    metadataURL = f'http://{mock_server_host}:{mock_server_port}{mock_metadata_endpoint}'

    settings = {'enabled': 'true',
                'idpMetadataOrigin': "http",
                'idpMetadataURL': metadataURL,
                'idpMetadata': "",
                'idpSignsMetadata': True,
                'idpMetadataRefreshIntervalS': 1,
                'idpMetadataConnectAddressFamily': 'inet',
                'idpAuthnBinding': 'post',
                'idpLogoutBinding': 'post',
                'usernameAttribute': 'uid',
                'spVerifyRecipient': 'consumeURL',
                'spAssertionDupeCheck': 'global',
                'spEntityId': sp_entity_id,
                'spBaseURLType': 'node',
                'spBaseURLScheme': 'http',
                'spCustomBaseURL': 'http://127.0.0.1',
                'spOrgName': 'Test Org',
                'spOrgDisplayName': 'Test Display Name',
                'spOrgURL': 'example.com',
                'spContactName': 'test contact',
                'spContactEmail': 'test@example.com',
                'spVerifyAssertionSig': True,
                'spVerifyAssertionEnvelopSig': True,
                'spCertificate': cert_pem,
                'spKey': key_pem,
                'spSignRequests': True,
                'spSignMetadata': True,
                'spTrustedFingerprints': trusted_fps,
                'spTrustedFingerprintsUsage': 'metadataInitialOnly',
                'groupsAttribute': '',
                'groupsAttributeSep': '',
                'groupsFilterRE': '',
                'rolesAttribute': '',
                'rolesAttributeSep': '',
                'rolesFilterRE': '',
                'singleLogoutEnabled': True,
                'spClockSkewS': 0,
                'spVerifyIssuer': True,
                'spSAMLResponseMaxSize': 256 * 1024}


    for k in kwargs:
        if k in settings:
            if kwargs[k] is not None:
                settings[k] = kwargs[k]
            else:
                del settings[k]

    testlib.post_succ(node, '/settings/saml', json=settings)


def idp_config(node, spSignRequests=True, assertion_lifetime=15,
               certs_prefix="mockidp_", idp_entity_id=None, **kwargs):
    sp_base_url = node.url
    if idp_entity_id is None:
        idp_entity_id = f"{mock_server_url}{mock_metadata_endpoint}"
    key_path = os.path.join(testlib.get_resources_dir(), "saml",
                            f"{certs_prefix}key.pem")
    cert_path = os.path.join(testlib.get_resources_dir(), "saml",
                            f"{certs_prefix}cert.pem")
    log_level = "DEBUG" if debug else "ERROR"
    return {"entityid": idp_entity_id,
            "description": "My IDP",
            "valid_for": 1,
            "service": {
                "idp": {
                    "name": "Mock IdP",
                    "endpoints": {
                        "single_sign_on_service": [
                            (mock_sso_redirect_url, BINDING_HTTP_REDIRECT),
                            (mock_sso_post_url, BINDING_HTTP_POST)
                        ],
                        "single_logout_service": [
                            (mock_slo_redirect_url, BINDING_HTTP_REDIRECT),
                            (mock_slo_post_url, BINDING_HTTP_POST)
                        ]
                    },
                    "policy": {
                        "default": {
                            "lifetime": {"minutes": assertion_lifetime},
                            "attribute_restrictions": None,
                            "name_form": NAME_FORMAT_URI
                        },
                    },
                    "subject_data": idp_subject_file_path,
                    "name_id_format": [NAMEID_FORMAT_TRANSIENT,
                                       NAMEID_FORMAT_PERSISTENT],
                    "want_authn_requests_signed": spSignRequests
                },
            },
            "debug": 0,
            "key_file": key_path,
            "cert_file": cert_path,
            "metadata": {
                "remote": [{"url": f"{sp_base_url}/saml/metadata"}]
            },
            "organization": {
                "display_name": "Test Org",
                "name": "Test NAME",
                "url": "http://www.example.com",
            },
            "contact_person": [
                {
                    "contact_type": "support",
                    "given_name": "Support",
                    "email_address": "support@example.com"
                },
            ],
            "logging": {
                "version": 1,
                "formatters": {
                    "simple": {
                        "format": "[%(asctime)s] [%(levelname)s] [%(name)s.%(funcName)s] %(message)s",
                    },
                },
                "handlers": {
                    "stderr": {
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stderr",
                        "level": log_level,
                        "formatter": "simple",
                    },
                },
                "loggers": {
                    "saml2": {
                        "level": log_level
                    },
                },
                "root": {
                    "level": log_level,
                    "handlers": [
                        "stderr",
                    ],
                },
            },
        }


def extract_saml_message_from_form(msg_type, form_data):
    action_re = re.compile('action="(.+)"')
    redirect_url = html.unescape(action_re.search(form_data).group(1))
    response_re = re.compile(f'name="{msg_type}"\s+value="(.+)"')
    saml_msg = html.unescape(response_re.search(form_data).group(1))
    return (redirect_url, saml_msg)


# In some cases we redirect user back to UI and show an error
# This function retrives that error and makes sure that the user is not
# logged in to UI
def catch_error_after_redirect(node, session, response):
    assert_http_code(302, response)

    print(f'Redirected to: {response.headers["Location"]}')
    redirect_path = response.headers['Location']

    # check that user doesn't have any roles
    # it is ui but cookie is not set
    ui_request('get', node, '/whoami', session, expected_code=401)

    # checking that we redirect to a valid page
    ui_request('get', node, redirect_path, session, expected_code=200)

    # extracting msg id from the redirect url
    parsedLocation = urlparse(redirect_path)
    fragment = parsedLocation.fragment
    params = parse_qs(urlparse(fragment).query)
    assert_in('samlErrorMsgId', params)
    error_id = params['samlErrorMsgId']

    # extracting error msg from server
    r = ui_request('get', node, '/saml/error', session, expected_code=200,
                   params={'id': error_id})
    error_msg = r.json()['error']
    print(f'Received error: {error_msg}')
    return error_msg


def generate_unsolicited_authn_response(IDP):
    identity = idp_test_user_attrs.copy()
    binding_out, destination = \
        IDP.pick_binding("assertion_consumer_service",
                         bindings=[BINDING_HTTP_POST],
                         entity_id=sp_entity_id)
    name_id = NameID(text=testlib.random_str(16))

    expiration = datetime.datetime.utcnow() + \
                 datetime.timedelta(minutes=1)
    expiration_iso = expiration.replace(microsecond=0).isoformat()

    response = IDP.create_authn_response(
                 identity,
                 None, # InResponseTo is missing cause it is
                       # an unsolicited response
                 destination,
                 sp_entity_id=sp_entity_id,
                 userid=idp_test_username,
                 name_id=name_id,
                 sign_assertion=True,
                 sign_response=True,
                 authn={'class_ref': AUTHN_PASSWORD},
                 session_not_on_or_after=expiration_iso)

    return destination, response, name_id


def send_unsolicited_authn(IDP, session):
    destination, response, name_id = \
        generate_unsolicited_authn_response(IDP)

    print(f"Sending authn response to {destination}...")
    r = post_saml_response(destination, response, session, expected_code=302)
    return r, name_id


def check_access(session, node, expected_code):
    testlib.get_fail(node, '/pools/default', headers=ui_headers,
                     session=session, auth=None, expected_code=expected_code)


def post_saml_request(destination, req, session, expected_code=None):
    req_encoded = base64.b64encode(f"{req}".encode("utf-8"))
    return testlib.http_request('post', destination,
                                data={'SAMLRequest': req_encoded},
                                headers=ui_headers,
                                allow_redirects=False,
                                session=session,
                                expected_code=expected_code)


def post_saml_response(destination, response, session, expected_code=None):
    if type(response) == str:
        response = base64.b64encode(f"{response}".encode("utf-8"))
    return testlib.http_request('post', destination,
                                 data={'SAMLResponse': response},
                                 headers=ui_headers,
                                 allow_redirects=False,
                                 session=session,
                                 expected_code=expected_code)


def ui_request(method, node, path, session, expected_code=None, **kwargs):
    return testlib.request(method,
                           node,
                           path,
                           expected_code=expected_code,
                           headers=ui_headers,
                           allow_redirects=False,
                           session=session,
                           auth=None, **kwargs)
