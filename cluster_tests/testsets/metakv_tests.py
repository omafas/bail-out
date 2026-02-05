# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
import base64
import json

TESTKEY = "/test_key"
STREAM_TEST_KEY = "/stream_test/"

class MetakvTests(testlib.BaseTestSet):
    def __init__(self, cluster):
        super().__init__(cluster)

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        testlib.metakv_delete_succ(self.cluster, TESTKEY)
        testlib.metakv_delete_succ(self.cluster, STREAM_TEST_KEY)

    def metakv_basic_test(self):
        # Test basic flow without revision
        testlib.metakv_put_succ(self.cluster, TESTKEY,
                                data={"value": "value"})
        r = testlib.metakv_get_succ(self.cluster, TESTKEY)
        body = r.json()

        # GET returns base64-encoded value and rev
        got_value = base64.b64decode(body["value"], validate=True).decode()
        assert got_value == "value"

        testlib.metakv_delete_succ(self.cluster, TESTKEY)

    def metakv_cas_test(self):
        testlib.metakv_put_succ(self.cluster, TESTKEY,
                                data={"value": "value"})

        # Test CAS behavior - successful update with matching revision
        response = testlib.metakv_get_succ(self.cluster, TESTKEY)
        rev_b64 = response.json()["rev"]
        rev_bytes = base64.b64decode(rev_b64, validate=True)
        testlib.metakv_put_succ(
            self.cluster, TESTKEY, data={"value": "value1", "rev": rev_bytes}
        )
        # Verify value updated
        r = testlib.metakv_get_succ(self.cluster, TESTKEY)
        assert (
            base64.b64decode(r.json()["value"], validate=True).decode()
            == "value1"
        )
        assert r.json()["rev"] != rev_b64, "rev should have changed"

        # Test CAS behavior - failed update with stale revision
        stale_rev_bytes = rev_bytes
        testlib.metakv_put_fail(
            self.cluster,
            TESTKEY,
            409,
            data={"value": "should_fail", "rev": stale_rev_bytes},
        )

        # Test successful update with current revision
        response = testlib.metakv_get_succ(self.cluster, TESTKEY)
        current_rev_b64 = response.json()["rev"]
        current_rev_bytes = base64.b64decode(current_rev_b64, validate=True)
        testlib.metakv_put_succ(
            self.cluster, TESTKEY, data={"value": "value2",
                                         "rev": current_rev_bytes}
        )

        # Verify value updated
        r = testlib.metakv_get_succ(self.cluster, TESTKEY)
        assert (
            base64.b64decode(r.json()["value"], validate=True).decode()
            == "value2"
        )

        # Test delete with revision
        response = testlib.metakv_get_succ(self.cluster, TESTKEY)
        final_rev_b64 = response.json()["rev"]
        final_rev_bytes = base64.b64decode(final_rev_b64, validate=True)
        testlib.metakv_delete_succ(
            self.cluster, TESTKEY, params={"rev": final_rev_bytes}
        )

        # Test delete with stale revision (should fail with 409)
        testlib.metakv_put_succ(self.cluster, TESTKEY, data={"value": "value3"})
        response = testlib.metakv_get_succ(self.cluster, TESTKEY)
        current_rev_b64 = response.json()["rev"]
        current_rev_bytes = base64.b64decode(current_rev_b64, validate=True)

        # Try to delete with stale revision
        testlib.metakv_delete_fail(
            self.cluster, TESTKEY, 409, params={"rev": final_rev_bytes}
        )

        # Delete with current revision should succeed
        testlib.metakv_delete_succ(
            self.cluster, TESTKEY, params={"rev": current_rev_bytes}
        )

    def metakv_streaming_test(self):
        entries = [
            {"path": STREAM_TEST_KEY + "a", "value": "foo", "sensitive": False},
            {"path": STREAM_TEST_KEY + "b", "value": "bar", "sensitive": True},
        ]

        for e in entries:
            data = {"value": e["value"]}
            if e["sensitive"]:
                data["sensitive"] = "true"
            testlib.metakv_put_succ(self.cluster, e["path"], data=data)

        # Non-continuous streaming over directory
        r = testlib.get_succ(
            self.cluster, f"/_metakv{STREAM_TEST_KEY}", stream=True
        )

        expected_keys = {e["path"] for e in entries}
        expected_values = {e["path"]: e["value"] for e in entries}
        expected_sensitive = {e["path"]: e["sensitive"] for e in entries}

        seen_values = {}
        seen_revs_b64 = {}
        seen_sensitive = {}

        decoder = json.JSONDecoder()
        buf = ""
        for chunk in r.iter_content(chunk_size=None):
            buf += chunk.decode("utf-8")
            buf = buf.lstrip()
            while buf:
                try:
                    obj, end = decoder.raw_decode(buf)
                except json.JSONDecodeError:
                    break  # wait for more bytes
                buf = buf[end:].lstrip()

                path = obj.get("path")
                value_b64 = obj.get("value")
                rev_b64 = obj.get("rev")
                sensitive = obj.get("sensitive")

                assert path in expected_keys, f"unexpected streamed key: {path}"

                assert value_b64 is not None and isinstance(value_b64, str)
                seen_values[path] = base64.b64decode(
                    value_b64, validate=True
                ).decode()

                assert rev_b64 is not None and isinstance(rev_b64, str)
                seen_revs_b64[path] = rev_b64

                seen_sensitive[path] = sensitive

        assert set(seen_values.keys()) == expected_keys
        assert set(seen_revs_b64.keys()) == expected_keys
        assert set(seen_sensitive.keys()) == expected_keys

        for e in entries:
            p = e["path"]
            assert seen_values[p] == expected_values[p]
            assert seen_sensitive[p] is expected_sensitive[p]

        # Compare streamed revs to GET revs (after base64-decoding)
        for key, rev_b64 in seen_revs_b64.items():
            g = testlib.metakv_get_succ(self.cluster, key)
            rev_get_b64 = g.json()["rev"]
            assert base64.b64decode(rev_b64, validate=True) == base64.b64decode(
                rev_get_b64, validate=True
            )
