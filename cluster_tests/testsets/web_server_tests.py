# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
import http.client
import contextlib
import base64

REDIRECT_RESPONSE = "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\
<html><head><title>301 Moved Permanently</title></head><body>\
<h1>Moved Permanently</h1>\
<p>The document has moved <a href=\"/ui/index.html\">here</a>.\
</p></body></html>"

class WebServerTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    def setup(self):
        pass

    def teardown(self):
        pass

    # Sending wrong path in http request: "GET \ HTTP1.1"
    def invalid_uri_in_request_test(self):
        node = self.cluster.connected_nodes[0]
        with low_level_http_get(node, '\\') as response:
            testlib.assert_eq(response.status, 400)
            testlib.assert_eq(response.reason, 'Bad Request')

    # We should ignore multiple slashes
    def double_slash_test(self):
        testlib.get_succ(self.cluster, '////pools///////default')
        # Using _metakv because this specific endpoint assumes that Path
        # is not starting with /, and crashes otherwise
        testlib.get_fail(self.cluster, '////_metakv/unknown', expected_code=404)

    # We support curly braces and some other questionable characters in
    # query parameters for backward compatibility.
    # Note that we don't urlencode parameters on purpose here.
    def curly_braces_test(self):
        # Note that the requests lib encodes parameters automatically so we
        # can't use it here
        node = self.cluster.connected_nodes[0]
        (user, password) = self.cluster.auth
        basic_auth = base64.b64encode(f'{user}:{password}'.encode()).decode()
        basic_auth = 'Basic ' + basic_auth
        request = '/_prometheus/federate?match={__name__="up"}'
        with low_level_http_get(node, request, auth=basic_auth) as response:
            testlib.assert_eq(response.status, 200)

    # Want to first use vulnerability, then enable flag, and test that it
    # dissallows header injection.
    def malicious_hostname_redirect_test(self):
        response = testlib.get_fail(self.cluster, "/", 301,
                                    headers={"Host": "example"},
                                    allow_redirects=False)
        testlib.assert_eq(response.headers["Location"],
                          "http://example/ui/index.html")
        testlib.post_succ(self.cluster, "/internalSettings",
                          data={"useRelativeWebRedirects": "true"})
        response = testlib.get_fail(self.cluster, "/", 301,
                                    headers={"Host": "example"},
                                    allow_redirects=False)
        testlib.assert_eq(response.text.strip(), REDIRECT_RESPONSE)
        testlib.assert_eq(response.headers["Location"], "/ui/index.html")

        # But even with the flag set, if we use sane headers, it will always
        # succeed.
        testlib.get_succ(self.cluster, "/")

        # disable it again, to bring us back to default state
        testlib.post_succ(self.cluster, "/internalSettings",
                          data={"useRelativeWebRedirects": "false"})

    # make sure we aren't vulnerable to web cache poisoning attacks
    def ensure_correct_cache_control_redirect_test(self):
        response = testlib.get_fail(self.cluster, "/", 301,
                                    headers={"Host": "example"},
                                    allow_redirects=False)
        testlib.assert_eq(response.headers["cache-control"],
                          "no-cache,no-store,must-revalidate")
        testlib.assert_eq(response.headers["expires"],
                          "Thu, 01 Jan 1970 00:00:00 GMT")
        testlib.assert_eq(response.headers["pragma"], "no-cache")


@contextlib.contextmanager
def low_level_http_get(node, path, auth=None):
    conn = http.client.HTTPConnection(node.host, node.port)
    try:
        host = testlib.maybe_add_brackets(node.host)
        headers = {'Host': f'{host}:{node.port}'}
        if auth is not None:
            headers['Authorization'] = auth
        conn.request('GET', path, headers=headers)
        yield conn.getresponse()
    finally:
        conn.close()
