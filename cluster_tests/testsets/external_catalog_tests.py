# @author Couchbase <info@couchbase.com>
# @copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in
# that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.
import testlib

BASE_PATH = "/pools/default/externalCatalogs"


def catalog_path(name):
    return f"{BASE_PATH}/{name}"


def get_catalogs_from_response(response_json):
    return {k: v for k, v in response_json.items()
            if k != "uid"}


class ExternalCatalogTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(
            buckets=[],
            min_num_nodes=1,
            min_num_connected=1,
            balanced=True,
            include_services=[testlib.Service.QUERY])

    def setup(self):
        testlib.set_config_key(self.cluster,
                               'ignore_external_catalog_validation_errors',
                               True)
        testlib.diag_eval(self.cluster,
                          'ns_config:set('\
                          'forced_external_catalog_validation_results, '\
                          '#{<<"param1">> => <<"value1">>,'\
                          '  <<"param2">> => <<"value2">>})')

    def teardown(self):
        pass

    def test_teardown(self):
        response = testlib.get_succ(self.cluster, BASE_PATH).json()
        catalogs = get_catalogs_from_response(response)
        for catalog in catalogs:
            testlib.ensure_deleted(
                self.cluster, catalog_path(catalog))

    def get_empty_list_test(self):
        r = testlib.get_succ(self.cluster, BASE_PATH)
        body = r.json()
        testlib.assert_in("uid", body)
        catalogs = get_catalogs_from_response(body)
        testlib.assert_eq({}, catalogs)

    def create_catalog_test(self):
        r = testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "test-catalog"})
        body = r.json()
        testlib.assert_in("rev", body)

        r = testlib.get_succ(
            self.cluster, catalog_path("test-catalog"))
        body = r.json()
        testlib.assert_in("rev", body)

    def create_duplicate_catalog_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "dup-catalog"})
        testlib.post_fail(
            self.cluster, BASE_PATH, expected_code=409,
            data={"name": "dup-catalog"})

    def create_catalog_missing_name_test(self):
        testlib.post_fail(
            self.cluster, BASE_PATH, expected_code=400,
            data={})

    def create_catalog_invalid_name_test(self):
        testlib.post_fail(
            self.cluster, BASE_PATH, expected_code=400,
            data={"name": "bad name!"})

    def get_nonexistent_catalog_test(self):
        testlib.get_fail(
            self.cluster,
            catalog_path("nonexistent"),
            expected_code=404)

    def list_multiple_catalogs_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "catalog-a"})
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "catalog-b"})

        r = testlib.get_succ(self.cluster, BASE_PATH)
        catalogs = get_catalogs_from_response(r.json())
        testlib.assert_in("catalog-a", catalogs)
        testlib.assert_in("catalog-b", catalogs)

    def modify_catalog_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "modify-catalog"})

        testlib.put_succ(
            self.cluster, catalog_path("modify-catalog"),
            data={"param1": "value1"}),

        r = testlib.get_succ(
            self.cluster, catalog_path("modify-catalog"))
        testlib.assert_eq("value1", r.json()["param1"])

    def modify_nonexistent_catalog_test(self):
        testlib.put_fail(
            self.cluster,
            catalog_path("nonexistent"),
            expected_code=404,
            data={})

    def patch_catalog_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "patch-catalog"})

        r = testlib.patch_succ(
            self.cluster,
            catalog_path("patch-catalog"),
            data={"param1": "value1"})
        body = r.json()
        testlib.assert_eq("value1", body["param1"])

        r = testlib.get_succ(
            self.cluster,
            catalog_path("patch-catalog"))
        testlib.assert_eq("value1", r.json()["param1"])

    def patch_catalog_preserves_existing_params_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "patch-preserve",
                  "param1": "value1"})

        testlib.patch_succ(
            self.cluster,
            catalog_path("patch-preserve"),
            data={"param2": "value2"})

        r = testlib.get_succ(
            self.cluster,
            catalog_path("patch-preserve"))
        body = r.json()
        testlib.assert_eq("value1", body["param1"])
        testlib.assert_eq("value2", body["param2"])

    def patch_nonexistent_catalog_test(self):
        testlib.patch_fail(
            self.cluster,
            catalog_path("nonexistent"),
            expected_code=404,
            data={"param": "value"})

    def patch_catalog_name_prohibited_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "no-rename"})

        testlib.patch_fail(
            self.cluster,
            catalog_path("no-rename"),
            expected_code=400,
            data={"name": "new-name"})

    def delete_catalog_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "to-delete"})

        testlib.delete_succ(
            self.cluster, catalog_path("to-delete"))

        testlib.get_fail(
            self.cluster,
            catalog_path("to-delete"),
            expected_code=404)

        r = testlib.get_succ(self.cluster, BASE_PATH)
        catalogs = get_catalogs_from_response(r.json())
        testlib.assert_not_in("to-delete", catalogs)

    def delete_nonexistent_catalog_test(self):
        testlib.delete_fail(
            self.cluster,
            catalog_path("nonexistent"),
            expected_code=404)

    def get_catalog_returns_rev_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "get-rev"})
        r = testlib.get_succ(
            self.cluster, catalog_path("get-rev"))
        testlib.assert_in("rev", r.json())

    def list_catalogs_returns_rev_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "list-rev"})
        r = testlib.get_succ(self.cluster, BASE_PATH)
        catalogs = get_catalogs_from_response(r.json())
        for cat in catalogs.values():
            testlib.assert_in("rev", cat)

    def uid_increments_test(self):
        r = testlib.get_succ(self.cluster, BASE_PATH)
        uid0 = r.json()["uid"]

        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "uid-cat-a"})
        r = testlib.get_succ(self.cluster, BASE_PATH)
        uid1 = r.json()["uid"]
        testlib.assert_eq(uid0 + 1, uid1)

        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "uid-cat-b"})
        r = testlib.get_succ(self.cluster, BASE_PATH)
        uid2 = r.json()["uid"]
        testlib.assert_eq(uid0 + 2, uid2)

        testlib.put_succ(
            self.cluster,
            catalog_path("uid-cat-a"),
            data={"param1": "value1"})
        r = testlib.get_succ(self.cluster, BASE_PATH)
        uid3 = r.json()["uid"]
        testlib.assert_eq(uid0 + 3, uid3)

        testlib.delete_succ(
            self.cluster,
            catalog_path("uid-cat-a"))
        r = testlib.get_succ(self.cluster, BASE_PATH)
        uid4 = r.json()["uid"]
        testlib.assert_eq(uid0 + 4, uid4)

    def rev_matches_uid_test(self):
        r = testlib.get_succ(self.cluster, BASE_PATH)
        uid0 = r.json()["uid"]

        r = testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "rev-uid"})
        testlib.assert_eq(uid0 + 1, r.json()["rev"])

        r = testlib.put_succ(
            self.cluster,
            catalog_path("rev-uid"),
            data={"param1": "value1"})
        testlib.assert_eq(uid0 + 2, r.json()["rev"])
