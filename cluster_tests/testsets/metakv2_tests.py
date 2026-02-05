# @author Couchbase <info@couchbase.com>
# @copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
import json

METAKV2_ENDPOINT = "/_metakv2"
CONTROLLER_ENDPOINT = METAKV2_ENDPOINT + "/_controller"

def decode_json(resp):
    return testlib.json_response(resp,
                                 testlib.format_error(resp, "Invalid json"))

def assert_json_key(resp, key, json):
    return testlib.assert_json_key(key, json, testlib.format_res_info(resp))

def assert_not_changed(resp, expected_path = None):
    testlib.assert_http_code(200, resp)
    json = decode_json(resp)
    assert_json_value(resp, "message", "Not Changed", json)
    if expected_path is not None:
        assert_json_key(resp, "revision", json)
        assert_json_value(resp, "path", expected_path, json)

def assert_json_error(resp, expected):
    json = decode_json(resp)
    error = assert_json_key(resp, "error", json)
    testlib.assert_eq(error, expected, "error", resp)

def assert_json_value(resp, key, expected, json):
    v = assert_json_key(resp, key, json)
    testlib.assert_eq(v, expected, key, resp)

def assert_json_value_in(resp, key, expected, json):
    v = assert_json_key(resp, key, json)
    testlib.assert_in(v, expected, resp)

def extract_val_rev(resp, json):
    return (assert_json_key(resp, "value", json),
            assert_json_key(resp, "revision", json))

def fetch_leaves(resp, content, leaves):
    for key, value in content.items():
        r = assert_json_key(resp, "revision", value)
        if key.endswith("/"):
            if "value" in value.keys():
                fetch_leaves(resp, value["value"], leaves)
            else:
                leaves[key] = "dir"
        else:
            v = assert_json_key(resp, "value", value)
            leaves[key] = v

def extract_snapshot(resp):
    (val, rev) = assert_val_rev(resp)
    return {k: extract_val_rev(resp, v)[0] for (k, v) in val.items()}

def assert_created(resp):
    testlib.assert_http_code(201, resp)
    json = decode_json(resp)
    assert_json_key(resp, "revision", json)
    assert_json_value(resp, "message", "Created", json)

def assert_updated(resp):
    testlib.assert_http_code(200, resp)
    json = decode_json(resp)
    assert_json_key(resp, "revision", json)
    assert_json_value(resp, "message", "Updated", json)

def assert_deleted(resp):
    testlib.assert_http_code(200, resp)
    json = decode_json(resp)
    assert_json_key(resp, "revision", json)
    assert_json_value(resp, "message", "Deleted", json)

def assert_not_found(resp):
    testlib.assert_http_code(404, resp)
    json = decode_json(resp)
    assert_json_value(resp, "message", "Not Found", json)

def assert_not_empty(resp):
    testlib.assert_http_code(400, resp)
    json = decode_json(resp)
    assert_json_value(resp, "message", "Not Empty", json)

def assert_conflict(resp, key):
    testlib.assert_http_code(409, resp)
    json = decode_json(resp)
    assert_json_key(resp, "revision", json)
    assert_json_value(resp, "message", "Conflict", json)
    assert_json_value(resp, "path", key, json)

def assert_exists(resp):
    testlib.assert_http_code(200, resp)
    json = decode_json(resp)
    assert_json_key(resp, "revision", json)
    assert_json_value(resp, "message", "Exists", json)

def assert_top_level_leaf(resp, keys):
    testlib.assert_http_code(400, resp)
    json = decode_json(resp)
    assert_json_value(resp, "message",
                      "Creating top level leaves is not allowed", json)
    assert_json_value_in(resp, "path", keys, json)

def assert_val_rev(resp):
    testlib.assert_http_code(200, resp)
    json = decode_json(resp)
    return extract_val_rev(resp, json)

def assert_dir(resp, dir):
    (root, rev) = assert_val_rev(resp)
    rootdir = assert_json_key(resp, dir, root)
    (v, r) = extract_val_rev(resp, rootdir)
    leaves = {}
    fetch_leaves(resp, v, leaves)
    return leaves

def assert_value(resp, expected):
    (v, r) = assert_val_rev(resp)
    testlib.assert_eq(v, expected, "value", resp)
    return r

class Metakv2Tests(testlib.BaseTestSet):
    def __init__(self, cluster):
        super().__init__(cluster)

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes=1, memsize=1024,
                                           buckets=[])

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        testlib.ensure_deleted(self.cluster, METAKV2_ENDPOINT + "/root/",
                               params={'recursive': 'true'})
        resp = self.metakv2_get("/root/")
        assert_not_found(resp)

    def metakv2_get(self, key, recursive=False, depth=None):
        params = {}
        if recursive:
            params['recursive']='true'
        if depth is not None:
            params['depth']=depth

        return testlib.request('GET', self.cluster, METAKV2_ENDPOINT + key,
                               params=params)

    def metakv2_put(self, key, create=False, rev=None, recursive=False,
                    value=""):
        params = {}
        if create:
            params['create']='true'
        if recursive:
            params['recursive']='true'
        if rev is not None:
            params['rev']=rev

        return testlib.request('PUT', self.cluster, METAKV2_ENDPOINT + key,
                               data=value, params=params)

    def metakv2_get_snapshot(self, keys):
        input = "[" + ",".join(map(lambda x: "\"" + x + "\"", keys)) + "]"
        return testlib.post_succ(self.cluster,
                                 CONTROLLER_ENDPOINT + "/getSnapshot",
                                 data=input)

    def metakv2_set_multiple(self, kvrs, recursive=False):
        params = {}
        if recursive:
            params['recursive']='true'
        input = json.dumps(kvrs)
        return testlib.request('POST', self.cluster,
                               CONTROLLER_ENDPOINT + "/setMultiple",
                               data=input, params=params)

    def metakv2_delete(self, key, recursive=False):
        params = {}
        if recursive:
            params['recursive']='true'

        return testlib.request('DELETE', self.cluster,
                               METAKV2_ENDPOINT + key, params=params)

    def assert_dir_content(self, dir, expected, recursive=True, depth=None):
        resp = self.metakv2_get(dir, recursive=recursive, depth=depth)
        content = assert_dir(resp, dir)
        testlib.assert_eq(content, expected, "content", resp)

    def get_put_leaf_test(self):
        key = "/root/subdir/key1"
        resp = self.metakv2_get(key)
        assert_not_found(resp)

        resp = self.metakv2_put(key, value="v1")
        assert_not_found(resp)

        resp = self.metakv2_put(key, value="v1", create=True)
        assert_not_found(resp)

        resp = self.metakv2_put(key, value="v1", create=True, recursive=True)
        assert_created(resp)

        resp = self.metakv2_get(key)
        assert_value(resp, "v1")

        resp = self.metakv2_put(key, value="v2", create=True, recursive=True)
        assert_conflict(resp, key)

        resp = self.metakv2_put(key, value="v2", recursive=True)
        assert_updated(resp)

        resp = self.metakv2_put(key, value="v3")
        assert_updated(resp)

        resp = self.metakv2_put(key, value="v3", create=True)
        assert_conflict(resp, key)

        resp = self.metakv2_put(key, value="v3")
        assert_not_changed(resp, key)

        self.assert_dir_content("/root/", {key: 'v3'})

        resp = self.metakv2_get(key)
        rev = assert_value(resp, "v3")

        resp = self.metakv2_put(key, value="v4", rev = "fake")
        testlib.assert_http_code(400, resp)

        resp = self.metakv2_put(key, value="v4", rev = "fake:67")
        assert_conflict(resp, key)

        resp = self.metakv2_put(key, value="v4", rev = rev)
        assert_updated(resp)
        self.assert_dir_content("/root/", {key: 'v4'})

        resp = self.metakv2_put("/top", value="v4")
        assert_top_level_leaf(resp, ["/top"])

    def get_put_dir_test(self):
        subdir = "/root/subdir/"
        dir = "/root/subdir/dir/"
        resp = self.metakv2_get(subdir)
        assert_not_found(resp)

        resp = self.metakv2_put(dir)
        assert_not_found(resp)

        resp = self.metakv2_put(subdir, recursive=True)
        assert_created(resp)

        resp = self.metakv2_put(dir)
        assert_created(resp)

        resp = self.metakv2_put(dir)
        assert_exists(resp)

        self.assert_dir_content(subdir, {})
        self.assert_dir_content(dir, {})

    def get_directory_test(self):
        resp = self.metakv2_put("/root/subdir/key1", value="v1",
                                create=True, recursive=True)
        assert_created(resp)

        resp = self.metakv2_put("/root/subdir/subdir1/key2", value="v2",
                                create=True, recursive=True)
        assert_created(resp)

        resp = self.metakv2_put("/root/subdir/key3", value="v3",
                                create=True, recursive=True)
        assert_created(resp)

        resp = self.metakv2_put("/root/subdir1/key4", value="v4",
                                create=True, recursive=True)
        assert_created(resp)

        resp = self.metakv2_get("/root/subdir2/")
        assert_not_found(resp)

        resp = self.metakv2_get("/root/subdir2/", recursive=True)
        assert_not_found(resp)

        self.assert_dir_content("/root/subdir/",
                                {'/root/subdir/key1': 'v1',
                                 '/root/subdir/key3': 'v3'},
                                recursive = False)

        self.assert_dir_content("/root/subdir/",
                                {'/root/subdir/key1': 'v1',
                                 '/root/subdir/key3': 'v3',
                                 '/root/subdir/subdir1/key2': 'v2'})

        self.assert_dir_content("/root/subdir/",
                                {'/root/subdir/key1': 'v1',
                                 '/root/subdir/key3': 'v3',
                                 '/root/subdir/subdir1/': 'dir'}, depth = 1)

        self.assert_dir_content("/root/",
                                {'/root/subdir/': 'dir',
                                 '/root/subdir1/': 'dir'}, depth = 1)

        self.assert_dir_content("/root/",
                                {'/root/subdir/key1': 'v1',
                                 '/root/subdir/key3': 'v3',
                                 '/root/subdir1/key4': 'v4',
                                 '/root/subdir/subdir1/': 'dir'}, depth = 2)

        self.assert_dir_content("/root/",
                                {'/root/subdir/key1': 'v1',
                                 '/root/subdir/key3': 'v3',
                                 '/root/subdir1/key4': 'v4',
                                 '/root/subdir/subdir1/key2': 'v2'}, depth = 3)

        self.assert_dir_content("/root/", {}, recursive = False)

    def get_snapshot_test(self):
        resp = self.metakv2_put("/root/subdir/key1", value="v1",
                                create=True, recursive=True)
        assert_created(resp)

        resp = self.metakv2_put("/root/subdir/subdir1/key2", value="v2",
                                create=True, recursive=True)
        assert_created(resp)

        resp = self.metakv2_get_snapshot(["/root/subdir/key1",
                                          "/root/subdir/subdir1/key2",
                                          "/root/subdir/subdir1",
                                          "/root/subdir/subdir1/key3"])
        snapshot = extract_snapshot(resp)
        testlib.assert_eq(snapshot,
                          {'/root/subdir/key1': 'v1',
                           '/root/subdir/subdir1/key2': 'v2'}, "snapshot", resp)

    def set_multiple_test(self):
        k1 = "/root/subdir/key1"
        k2 = "/root/subdir/subdir1/key2"
        k3 = "/root/subdir1/subdir2/key3"
        k4 = "/root/subdir/subdir1/key4"

        resp = self.metakv2_set_multiple({k1: {"value": "v1"},
                                          k2: {"value": "v2"},
                                          k3: {"value": "v3"}})
        assert_not_found(resp)

        resp = self.metakv2_set_multiple({k1: {"value": "v1", "create": True},
                                          k2: {"value": "v2", "create": True},
                                          k3: {"value": "v3", "create": True}})
        assert_not_found(resp)

        # corrupted revision
        resp = self.metakv2_set_multiple(
            {k1: {"value": "v1", "create": True},
             k2: {"value": "v2", "create": True},
             k3: {"value": "v3", "revision": "rev"}},
            recursive=True)
        testlib.assert_http_code(400, resp)
        json = decode_json(resp)
        errors = assert_json_key(resp, "errors", json)
        testlib.assert_eq(errors, [{}, {}, {'revision':
                                            'Corrupted revision string'}],
                          "errors", resp)

        resp = self.metakv2_set_multiple({k1: {"value": "v1", "create": True},
                                          k2: {"value": "v2", "create": True},
                                          k3: {"value": "v3", "create": True}},
                                         recursive=True)
        assert_updated(resp)

        # already exists
        resp = self.metakv2_set_multiple({k1: {"value": "v10"},
                                          k2: {"value": "v20"},
                                          k3: {"value": "v30", "create": True}},
                                         recursive=True)
        assert_conflict(resp, k3)

        self.assert_dir_content("/root/", {k1: "v1", k2: "v2", k3: "v3"})

        resp = self.metakv2_set_multiple({k1: {"value": "v10"},
                                          k2: {"value": "v20"}}, recursive=True)
        assert_updated(resp)

        self.assert_dir_content("/root/", {k1: "v10", k2: "v20", k3: "v3"})

        resp = self.metakv2_set_multiple(
            {k3: {"value": "v30"}, k4: {"value": "v40"}})
        assert_updated(resp)

        self.assert_dir_content("/root/", {k1: "v10", k2: "v20", k3: "v30",
                                           k4: "v40"})

        resp = self.metakv2_get(k1)
        rev = assert_value(resp, "v10")

        resp = self.metakv2_set_multiple(
            {k1: {"value": "v1", "revision": "fake:70"}})
        assert_conflict(resp, k1)

        resp = self.metakv2_set_multiple({k1: {"value": "v1", "revision": rev},
                                          k2: {"value": "v2"}})
        assert_updated(resp)
        self.assert_dir_content("/root/", {k1: "v1", k2: "v2", k3: "v30",
                                           k4: "v40"})

        resp = self.metakv2_set_multiple({"/top": {"value": "v1"}})
        assert_top_level_leaf(resp, ["/top"])

        resp = self.metakv2_set_multiple({"/top1": {"value": "v1"},
                                          "/top2": {"value": "v2"}})
        assert_top_level_leaf(resp, ["/top1", "/top2"])

    def delete_test(self):
        k1 = "/root/subdir/key1"
        k2 = "/root/subdir/subdir1/key2"
        k3 = "/root/subdir1/subdir2/key3"

        resp = self.metakv2_set_multiple({k1: {"value": "v1", "create": True},
                                          k2: {"value": "v2", "create": True},
                                          k3: {"value": "v3", "create": True}},
                                         recursive=True)
        assert_updated(resp)

        self.assert_dir_content("/root/", {k1: "v1", k2: "v2", k3: "v3"})

        resp = self.metakv2_delete("/root/subdir/")
        assert_not_empty(resp)

        resp = self.metakv2_delete("/root/subdir/key2")
        assert_not_found(resp)

        resp = self.metakv2_delete("/root/subdir/key1")
        assert_deleted(resp)

        self.assert_dir_content("/root/", {k2: "v2", k3: "v3"})

        resp = self.metakv2_delete("/root/subdir/subdir1", recursive=True)
        assert_not_found(resp)

        resp = self.metakv2_delete("/root/subdir/subdir1/", recursive=True)
        assert_deleted(resp)

        self.assert_dir_content("/root/", {k3: "v3"})

        resp = self.metakv2_delete("/root/subdir/")
        assert_deleted(resp)

        self.assert_dir_content("/root/", {k3: "v3"})

        resp = self.metakv2_set_multiple({k1: {"value": "v1", "create": True},
                                          k2: {"value": "v2", "create": True}},
                                         recursive=True)
        assert_updated(resp)

        self.assert_dir_content("/root/", {k1: "v1", k2: "v2", k3: "v3"})

        resp = self.metakv2_delete("/root/subdir/", recursive=True)
        assert_deleted(resp)
        self.assert_dir_content("/root/", {k3: "v3"})
