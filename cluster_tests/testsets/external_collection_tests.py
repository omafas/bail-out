# @author Couchbase <info@couchbase.com>
# @copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in
# that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.
import testlib

BUCKETS_ENDPOINT = "/pools/default/buckets"


def bucket_path(bucket_name):
    return f"{BUCKETS_ENDPOINT}/{bucket_name}"


def manifest_path(bucket_name):
    return f"{bucket_path(bucket_name)}/scopes"


def collections_path(bucket_name, scope_name):
    return (f"{bucket_path(bucket_name)}"
            f"/scopes/{scope_name}/collections")


def collection_path(bucket_name, scope_name,
                    collection_name):
    return (f"{collections_path(bucket_name, scope_name)}"
            f"/{collection_name}")


def get_scope_from_manifest(cluster, bucket_name, scope_name, external=False):
    manifest = get_manifest(cluster, bucket_name,
                            external=external)
    for scope in manifest.get("scopes", []):
        if scope["name"] == scope_name:
            return scope
    return None


def get_manifest(cluster, bucket_name,
                 external=False):
    path = manifest_path(bucket_name)
    if external:
        path += "?external=1"
    r = testlib.get_succ(cluster, path)
    return r.json()


def get_manifest_uid(cluster, bucket_name,
                     external=False):
    return int(
        get_manifest(cluster, bucket_name,
                     external=external)["uid"], 16)


def get_collection_from_manifest(cluster, bucket_name,
                                 scope_name,
                                 collection_name,
                                 external=False):
    scope = get_scope_from_manifest(
        cluster, bucket_name, scope_name,
        external=external)
    if scope is None:
        return None
    collections = scope.get("collections", [])
    for col in collections:
        if col['name'] == collection_name:
            return col
    return None


class ExternalCollectionTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(
            buckets=[],
            min_num_nodes=1,
            min_num_connected=1,
            include_services=[testlib.Service.QUERY])

    def setup(self):
        testlib.diag_eval(self.cluster,
                          'ns_config:set(' \
                          'forced_external_collection_validation_results, ' \
                          '#{<<"param1">> => <<"value1">>,' \
                          '  <<"param2">> => <<"value2">>})')
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        testlib.delete_all_buckets(self.cluster)

    def create_bucket(self, bucket_name):
        testlib.post_succ(self.cluster, BUCKETS_ENDPOINT,
                          expected_code=202,
                          data={"name": bucket_name,
                                "bucketType": "membase",
                                "storageBackend": "couchstore",
                                "ramQuotaMB": 256})

    def create_scope(self, bucket_name, scope_name):
        scope_url = f"{bucket_path(bucket_name)}/scopes/"
        testlib.post_succ(self.cluster, scope_url,
                          data={"name": scope_name})

    def create_external_collection_test(self):
        bucket_name = "external-bucket"
        scope_name = "external-scope"
        collection_name = "external-collection"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        uid_before = get_manifest_uid(
            self.cluster, bucket_name, external=True)

        # Create external collection with external=1 query parameter
        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": collection_name})

        # Verify the collection was created uid changed
        testlib.assert_not_eq(
            uid_before,
            get_manifest_uid(self.cluster, bucket_name,
                             external=True))
        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name, external=True)
        testlib.assert_eq(collection_name, col["name"])

    def create_regular_collection_test(self):
        bucket_name = "regular-bucket"
        scope_name = "regular-scope"
        collection_name = "regular-collection"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        uid_before = get_manifest_uid(
            self.cluster, bucket_name)

        # Create regular collection without external parameter
        testlib.post_succ(
            self.cluster,
            collections_path(bucket_name, scope_name),
            data={"name": collection_name})

        # Verify the collection was created and uid bumped by 1
        testlib.assert_eq(
            uid_before + 1,
            get_manifest_uid(self.cluster, bucket_name))
        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name)
        testlib.assert_eq(collection_name, col["name"])

    def create_external_collection_with_props_test(self):
        bucket_name = "external-bucket-props"
        scope_name = "external-scope-props"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        uid_before = get_manifest_uid(
            self.cluster, bucket_name, external=True)

        # Create external collection with maxTTL property
        collection_name = "external-collection-props"
        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": collection_name})

        testlib.assert_not_eq(
            uid_before,
            get_manifest_uid(self.cluster, bucket_name,
                             external=True))
        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name, external=True)
        testlib.assert_eq(collection_name, col["name"])

    def create_external_collection_in_default_scope_test(self):
        bucket_name = "default-scope-bucket"
        collection_name = "external-collection-default"

        self.create_bucket(bucket_name)

        uid_before = get_manifest_uid(
            self.cluster, bucket_name, external=True)

        # Create external collection in _default scope
        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, '_default')}"
            "?external=1",
            data={"name": collection_name})

        # Verify the collection was created and uid changed
        testlib.assert_not_eq(
            uid_before,
            get_manifest_uid(self.cluster, bucket_name,
                             external=True))
        col = get_collection_from_manifest(
            self.cluster, bucket_name, "_default",
            collection_name, external=True)
        testlib.assert_eq(collection_name, col["name"])

    def create_duplicate_external_collection_test(self):
        bucket_name = "dup-bucket"
        scope_name = "dup-scope"
        collection_name = "dup-collection"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        # First creation should succeed
        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": collection_name})

        uid_before = get_manifest_uid(
            self.cluster, bucket_name, external=True)

        # Duplicate creation should fail
        testlib.post_fail(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            expected_code=400,
            data={"name": collection_name})

        # Manifest uid should not change on failure
        uid_after = get_manifest_uid(
            self.cluster, bucket_name, external=True)
        testlib.assert_eq(uid_before, uid_after)

    def create_external_collection_missing_name_test(self):
        bucket_name = "missing-name-bucket"
        scope_name = "missing-name-scope"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        uid_before = get_manifest_uid(
            self.cluster, bucket_name, external=True)

        # Create collection without name should fail
        testlib.post_fail(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            expected_code=400,
            data={})

        # Manifest uid should not change on failure
        uid_after = get_manifest_uid(
            self.cluster, bucket_name, external=True)
        testlib.assert_eq(uid_before, uid_after)

    def create_external_collection_invalid_name_test(self):
        bucket_name = "invalid-name-bucket"
        scope_name = "invalid-name-scope"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        uid_before = get_manifest_uid(
            self.cluster, bucket_name, external=True)

        # Create collection with invalid name should fail
        testlib.post_fail(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            expected_code=400,
            data={"name": ".bad!name"})

        # Manifest uid should not change on failure
        uid_after = get_manifest_uid(
            self.cluster, bucket_name, external=True)
        testlib.assert_eq(uid_before, uid_after)

    def list_external_collections_test(self):
        bucket_name = "list-bucket"
        scope_name = "list-scope"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        testlib.assert_eq(0, get_manifest_uid(
            self.cluster, bucket_name, external=True))

        # Create multiple external collections
        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": "collection-a"})

        # We skip some uids used by normal collections
        uid_mid = get_manifest_uid(
            self.cluster, bucket_name, external=True)
        testlib.assert_eq(3, uid_mid)

        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": "collection-b"})

        testlib.assert_eq(
            uid_mid + 1,
            get_manifest_uid(self.cluster, bucket_name,
                             external=True))

        # List collections via external manifest
        scope = get_scope_from_manifest(
            self.cluster, bucket_name, scope_name,
            external=True)
        names = [c["name"]
                 for c in scope.get("collections", [])]
        testlib.assert_in("collection-a", names)
        testlib.assert_in("collection-b", names)

    def modify_external_collection_test(self):
        bucket_name = "modify-bucket"
        scope_name = "modify-scope"
        collection_name = "modify-collection"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": collection_name})

        uid_before = get_manifest_uid(
            self.cluster, bucket_name, external=True)

        # Modify the external collection via PATCH
        testlib.patch_succ(
            self.cluster,
            f"{collection_path(bucket_name, scope_name, collection_name)}"
            "?external=1",
            data={})

        # External manifest uid should not be bumped, no change
        testlib.assert_eq(
            uid_before,
            get_manifest_uid(self.cluster, bucket_name,
                             external=True))

        # Collection should still exist
        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name, external=True)
        testlib.assert_eq(collection_name,
                          col["name"])

    def scope_not_found_test(self):
        bucket_name = "scope-not-found-bucket"

        self.create_bucket(bucket_name)

        # Create collection in non-existent scope should fail
        testlib.post_fail(
            self.cluster,
            f"{collections_path(bucket_name, 'nonexistent')}?external=1",
            expected_code=404,
            data={"name": "test-collection"})

    def bucket_not_found_test(self):
        # Create collection in non-existent bucket should fail
        testlib.post_fail(
            self.cluster,
            f"{collections_path('nonexistent', 'scope')}?external=1",
            expected_code=404,
            data={"name": "test-collection"})

    def patch_external_collection_test(self):
        bucket_name = "patch-bucket"
        scope_name = "patch-scope"
        collection_name = "patch-collection"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)
        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": collection_name})

        testlib.patch_succ(
            self.cluster,
            f"{collection_path(bucket_name, scope_name, collection_name)}"
            "?external=1")

        print(get_manifest(self.cluster, bucket_name, external=True))

        col = get_collection_from_manifest(
            self.cluster, bucket_name,
            scope_name, collection_name, external=True)
        testlib.assert_not_eq(None, col)

    def patch_nonexistent_external_collection_test(self):
        bucket_name = "patch-404-bucket"
        scope_name = "patch-404-scope"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        testlib.patch_fail(
            self.cluster,
            f"{collection_path(bucket_name, scope_name, 'nonexistent')}"
            "?external=1",
            expected_code=404)

    def delete_external_collection_test(self):
        bucket_name = "delete-ext-bucket"
        scope_name = "delete-ext-scope"
        collection_name = "delete-ext-col"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": collection_name})

        # Verify the collection exists in the
        # external manifest.
        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name, external=True)
        testlib.assert_not_eq(None, col)

        # Cannot delete the collection without the external flag.
        testlib.delete_fail(
            self.cluster,
            collection_path(
                bucket_name, scope_name,
                collection_name), 404)

        # Deletion without external=1 should do nothing
        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name, external=True)
        testlib.assert_not_eq(None, col)

        # But deletion with external=1 should work and bump the manifest uid
        testlib.delete_succ(
            self.cluster,
            f"{collection_path(bucket_name, scope_name, collection_name)}"
             "?external=1")

        # Verify the collection is gone from the external manifest.
        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name, external=True)
        testlib.assert_eq(None, col)

    def delete_external_collection_with_param_test(self):
        bucket_name = "delete-ext-param-bucket"
        scope_name = "delete-ext-param-scope"
        collection_name = "delete-ext-param-col"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        testlib.post_succ(
            self.cluster,
            f"{collections_path(bucket_name, scope_name)}"
            "?external=1",
            data={"name": collection_name})

        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name, external=True)
        testlib.assert_not_eq(None, col)

        # Delete with ?external=1 should also work.
        testlib.delete_succ(
            self.cluster,
            f"{collection_path(bucket_name, scope_name, collection_name)}"
            "?external=1")

        col = get_collection_from_manifest(
            self.cluster, bucket_name, scope_name,
            collection_name, external=True)
        testlib.assert_eq(None, col)

    def delete_nonexistent_external_collection_test(self):
        bucket_name = "delete-404-bucket"
        scope_name = "delete-404-scope"

        self.create_bucket(bucket_name)
        self.create_scope(bucket_name, scope_name)

        testlib.delete_fail(
            self.cluster,
            collection_path(
                bucket_name, scope_name,
                "nonexistent"),
            expected_code=404)
