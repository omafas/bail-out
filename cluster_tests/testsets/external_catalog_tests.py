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
                          'forced_external_catalog_validation_results,'\
                          ' #{<<"param">> => <<"value">>})')

    def teardown(self):
        pass

    def test_teardown(self):
        catalogs = testlib.get_succ(self.cluster, BASE_PATH).json()
        for catalog in catalogs:
            testlib.ensure_deleted(
                self.cluster, catalog_path(catalog))

    def get_empty_list_test(self):
        r = testlib.get_succ(self.cluster, BASE_PATH)
        testlib.assert_eq({}, r.json())

    def create_catalog_test(self):
        r = testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "test-catalog"})
        body = r.json()
        testlib.assert_eq({}, body)

        r = testlib.get_succ(
            self.cluster, catalog_path("test-catalog"))
        body = r.json()
        testlib.assert_eq({}, body)

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
        catalogs = r.json()
        testlib.assert_in("catalog-a", catalogs)
        testlib.assert_in("catalog-b", catalogs)

    def modify_catalog_test(self):
        testlib.post_succ(
            self.cluster, BASE_PATH,
            data={"name": "modify-catalog"})

        testlib.put_succ(
            self.cluster, catalog_path("modify-catalog"),
            data={"param": "value"}),

        r = testlib.get_succ(
            self.cluster, catalog_path("modify-catalog"))
        testlib.assert_eq("value", r.json()["param"])

    def modify_nonexistent_catalog_test(self):
        testlib.put_fail(
            self.cluster,
            catalog_path("nonexistent"),
            expected_code=404,
            data={})

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
        names = [c["name"] for c in r.json()]
        testlib.assert_not_in("to-delete", names)

    def delete_nonexistent_catalog_test(self):
        testlib.delete_fail(
            self.cluster,
            catalog_path("nonexistent"),
            expected_code=404)
