# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
from pprint import pprint

import testlib

BUCKET_NAME = "test"


class ResourceManagementAPITests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        # - Provisioned edition required for guard rails to be configurable
        return testlib.ClusterRequirements(edition="Provisioned")

    def setup(self):
        # Set the promQL queries to default values to ensure that they are
        # triggered consistently
        set_promql_queries(self.cluster)

    def teardown(self):
        pass

    def test_teardown(self):
        # Reset guard rail config
        testlib.diag_eval(self.cluster,
                          "[{resource_management, Cfg}] = "
                          "  menelaus_web_guardrails:default_config(),"
                          "ns_config:set(resource_management, Cfg).")

    def get_guard_rails_test(self):
        resident_ratio_config = testlib.get_succ(
            self.cluster, "/settings/resourceManagement/bucket/residentRatio")
        get("enabled", resident_ratio_config)
        get("couchstoreMinimum", resident_ratio_config)
        get("magmaMinimum", resident_ratio_config)

        data_size_config = testlib.get_succ(
            self.cluster, "/settings/resourceManagement/bucket/dataSizePerNode")
        get("enabled", data_size_config)
        get("couchstoreMaximum", data_size_config)
        get("magmaMaximum", data_size_config)

        cores_per_bucket_config = testlib.get_succ(
            self.cluster, "/settings/resourceManagement/coresPerBucket")
        get("enabled", cores_per_bucket_config)
        get("minimum", cores_per_bucket_config)

        collections_per_quota_config = testlib.get_succ(
            self.cluster,
            "/settings/resourceManagement/bucket/collectionsPerQuota")
        get("enabled", collections_per_quota_config)
        get("maximum", collections_per_quota_config)

        index_create_rr_config = testlib.get_succ(
            self.cluster, "/settings/resourceManagement/index/indexCreationRR")
        get("enabled", index_create_rr_config)
        get("minimum", index_create_rr_config)

        topology_change_rr_config = testlib.get_succ(
            self.cluster, "/settings/resourceManagement/index/topologyChangeRR")
        get("enabled", topology_change_rr_config)
        get("minimum", topology_change_rr_config)

        disk_usage_config = testlib.get_succ(
            self.cluster, "/settings/resourceManagement/diskUsage")
        get("enabled", disk_usage_config)
        get("maximum", disk_usage_config)

    def set_guard_rails_json_test(self):
        # Set guard rails with json

        r = testlib.post_succ(self.cluster, "/settings/resourceManagement",
                              json={
                                  "bucket": {
                                      "residentRatio": {
                                          "enabled": True,
                                          "couchstoreMinimum": 5,
                                          "magmaMinimum": 0.5
                                      },
                                      "dataSizePerNode": {
                                          "enabled": True,
                                          "couchstoreMaximum": 32,
                                          "magmaMaximum": 64
                                      },
                                      "collectionsPerQuota": {
                                          "enabled": True,
                                          "maximum": 2
                                      }
                                  },
                                  "index": {
                                      "indexCreationRR": {
                                          "enabled": True,
                                          "minimum": 6
                                      },
                                      "topologyChangeRR": {
                                          "enabled": True,
                                          "minimum": 5
                                      }
                                  },
                                  "coresPerBucket": {
                                      "enabled": True,
                                      "minimum": 0.2
                                  },
                                  "diskUsage": {
                                      "enabled": True,
                                      "maximum": 90
                                  }
                              })

        bucket_config = get("bucket", r)

        resident_ratio_config = bucket_config.get("residentRatio")
        assert resident_ratio_config.get("enabled") is True
        assert resident_ratio_config.get("couchstoreMinimum") == 5
        assert resident_ratio_config.get("magmaMinimum") == 0.5

        data_size_config = bucket_config.get("dataSizePerNode")
        assert data_size_config.get("enabled") is True
        assert data_size_config.get("couchstoreMaximum") == 32
        assert data_size_config.get("magmaMaximum") == 64

        collections_config = bucket_config.get("collectionsPerQuota")
        assert collections_config.get("enabled") is True
        assert collections_config.get("maximum") == 2

        index_config = get("index", r)

        index_resident_ratio_config = index_config.get("indexCreationRR")
        assert index_resident_ratio_config.get("enabled") is True
        assert index_resident_ratio_config.get("minimum") == 6

        topology_change_config = index_config.get("topologyChangeRR")
        assert topology_change_config.get("enabled") is True
        assert topology_change_config.get("minimum") == 5

        data_disk_usage_config = get("diskUsage", r)
        assert data_disk_usage_config.get("enabled") is True
        assert data_disk_usage_config.get("maximum") == 90

        cores_per_bucket_config = get("coresPerBucket", r)
        assert cores_per_bucket_config.get("enabled") is True
        assert cores_per_bucket_config.get("minimum") == 0.2

    def set_guard_rails_form_test(self):
        # Set guard rails with form-encoding

        r = testlib.post_succ(
            self.cluster, "/settings/resourceManagement",
            data={
                "bucket.residentRatio.enabled": "false",
                "bucket.residentRatio.couchstoreMinimum": 6,
                "bucket.residentRatio.magmaMinimum": 0.6,
                "bucket.dataSizePerNode.enabled": "false",
                "bucket.dataSizePerNode.couchstoreMaximum": 33,
                "bucket.dataSizePerNode.magmaMaximum": 65,
                "bucket.collectionsPerQuota.enabled": "false",
                "bucket.collectionsPerQuota.maximum": 3,
                "index.indexCreationRR.enabled": "false",
                "index.indexCreationRR.minimum": 7,
                "index.topologyChangeRR.enabled": "false",
                "index.topologyChangeRR.minimum": 6,
                "coresPerBucket.enabled": "false",
                "coresPerBucket.minimum": 0.3,
                "diskUsage.enabled": "false",
                "diskUsage.maximum": 91
            })

        bucket_config = get("bucket", r)

        resident_ratio_config = bucket_config.get("residentRatio")
        assert resident_ratio_config.get("enabled") is False
        assert resident_ratio_config.get("couchstoreMinimum") == 6
        assert resident_ratio_config.get("magmaMinimum") == 0.6

        data_size_config = bucket_config.get("dataSizePerNode")
        assert data_size_config.get("enabled") is False
        assert data_size_config.get("couchstoreMaximum") == 33
        assert data_size_config.get("magmaMaximum") == 65

        collections_config = bucket_config.get("collectionsPerQuota")
        assert collections_config.get("enabled") is False
        assert collections_config.get("maximum") == 3

        index_config = get("index", r)

        index_creation_rr_config = index_config.get("indexCreationRR")
        assert index_creation_rr_config.get("enabled") is False
        assert index_creation_rr_config.get("minimum") == 7

        topology_change_rr_config = index_config.get("topologyChangeRR")
        assert topology_change_rr_config.get("enabled") is False
        assert topology_change_rr_config.get("minimum") == 6

        assert get("coresPerBucket", r).get("enabled") is False
        assert get("coresPerBucket", r).get("minimum") == 0.3

        assert get("diskUsage", r).get("enabled") is False
        assert get("diskUsage", r).get("maximum") == 91

    def set_guard_rails_path_test(self):
        # Set residentRatio guard rail using path

        r = testlib.post_succ(
            self.cluster, "/settings/resourceManagement/bucket/residentRatio",
            data={
                "enabled": "true",
                "couchstoreMinimum": 7,
                "magmaMinimum": 0.7
            })
        assert get("enabled", r) is True
        assert get("couchstoreMinimum", r) == 7
        assert get("magmaMinimum", r) == 0.7

        r = testlib.post_succ(
            self.cluster, "/settings/resourceManagement/bucket/dataSizePerNode",
            json={
                "enabled": True,
                "couchstoreMaximum": 34,
                "magmaMaximum": 66
            })
        assert get("enabled", r) is True
        assert get("couchstoreMaximum", r) == 34
        assert get("magmaMaximum", r) == 66

        r = testlib.post_succ(
            self.cluster, "/settings/resourceManagement/coresPerBucket",
            data={
                "enabled": "true",
                "minimum": 0.4
            })

        assert get("enabled", r) is True
        assert get("minimum", r) == 0.4

        r = testlib.post_succ(
            self.cluster, "/settings/resourceManagement/diskUsage",
            data={
                "enabled": "true",
                "maximum": 92
            })

        assert get("enabled", r) is True
        assert get("maximum", r) == 92

        r = testlib.post_succ(
            self.cluster, "/settings/resourceManagement/index/indexCreationRR",
            json={
                "enabled": True,
                "minimum": 8
            })
        assert get("enabled", r) is True
        assert get("minimum", r) == 8

        r = testlib.post_succ(
            self.cluster, "/settings/resourceManagement/index/topologyChangeRR",
            json={
                "enabled": True,
                "minimum": 7
            })
        assert get("enabled", r) is True
        assert get("minimum", r) == 7


class GuardRailRestrictionTests(testlib.BaseTestSet):
    num_connected = 2

    def __init__(self, cluster):
        super().__init__(cluster)
        self.original_max_supported = None
        self.original_promql = None

    @staticmethod
    def requirements():
        # - Provisioned edition required for guard rails to be configurable
        # - 3 nodes so that we can test swap rebalances
        # - 1024MB quota for magma bucket
        return testlib.ClusterRequirements(
            edition="Provisioned", min_num_nodes=3,
            num_connected=GuardRailRestrictionTests.num_connected,
            min_memsize=1024)

    def setup(self):
        testlib.delete_all_buckets(self.cluster)

        # Get original settings, so that they can be set back on teardown
        original_settings = testlib.get_succ(self.cluster, "/internalSettings")\
            .json()
        self.original_max_supported = original_settings \
            .get("maxBucketCount", 30)
        self.original_promql = original_settings .get("resourcePromQLOverride")

    def teardown(self):
        testlib.delete_all_buckets(self.cluster)

        # Set back modified internal settings to their original values
        testlib.post_succ(self.cluster, "/internalSettings",
                          data={"maxBucketCount": self.original_max_supported,
                                **{f"resourcePromQLOverride.{key}": value
                                   for key, value in
                                   self.original_promql.items()}})

        response = testlib.get_succ(self.cluster, "/internalSettings").json()
        assert response.get("maxBucketCount") == self.original_max_supported
        assert response.get("resourcePromQLOverride") == self.original_promql, \
            f"failed to reset resourcePromQLOverride to " \
            f"{self.original_promql}, got from /internalSettings: {response}"

    def test_teardown(self):
        testlib.delete_all_buckets(self.cluster)
        # Reset the promQL queries to default values to ensure that they are
        # triggered consistently
        set_promql_queries(self.cluster)
        # Disable bucket guard rails to avoid one being triggered when we are
        # looking for another to be triggered
        disable_bucket_guard_rails(self.cluster)

    def num_buckets_test(self):
        pools = testlib.get_succ(self.cluster, "/pools/default").json()
        cpu_count = pools["nodes"][0]["cpuCount"]

        # Set minimum cores per bucket to N times the cpu count, permitting
        # exactly N buckets (where N = max_buckets_dynamic)
        for max_buckets_dynamic in range(1, 4):
            testlib.post_succ(self.cluster,
                              "/settings/resourceManagement/coresPerBucket",
                              json={
                                  "enabled": True,
                                  "minimum": cpu_count / max_buckets_dynamic
                              })

            # Set the hard limit just above the dynamic limit
            max_buckets_supported = max_buckets_dynamic + 1
            testlib.post_succ(self.cluster, "/internalSettings",
                              data={"maxBucketCount": max_buckets_supported})

            # Create the permitted buckets
            for i in range(max_buckets_dynamic):
                self.cluster.create_bucket({
                    "name": f"test_{i}",
                    "ramQuota": 100
                })

            # Test that an additional bucket can't be created and gives the
            # expected error message, mentioning the per-core limit
            r = self.cluster.create_bucket(
                {
                    "name": "test_too_many",
                    "ramQuota": 100
                }, expected_code=400)
            exp_error = f"Cannot create more than {max_buckets_dynamic} " \
                        f"buckets due to insufficient cpu cores. Either " \
                        f"increase the resource minimum or the number of " \
                        f"cores on all kv nodes."
            assert r.json()['_'] == exp_error, \
                f"{r.json()['_']} != {exp_error}"

            # Disable the per-core limit
            testlib.post_succ(self.cluster,
                              "/settings/resourceManagement/coresPerBucket",
                              json={
                                  "enabled": False
                              })

            # One more bucket allowed
            self.cluster.create_bucket({
                "name": f"test_{cpu_count}",
                "ramQuota": 100
            })

            # Test that no more buckets are allowed, and we now get the old
            # error message
            r = self.cluster.create_bucket(
                {
                    "name": "test_too_many_again",
                    "ramQuota": 100
                }, expected_code=400)
            exp_error = f"Cannot create more than {max_buckets_supported} " \
                        f"buckets"
            assert r.json()["_"] == exp_error, \
                f"{r.json()['_']} != {exp_error}"

            # Delete the buckets in preparation for the next test case
            testlib.delete_all_buckets(self.cluster)

    def rebalance_rr_test(self):
        # Create the bucket synchronously, so that the bucket is ready for the
        # rebalance checks to be performed correctly
        self.cluster.create_bucket({
            "name": BUCKET_NAME,
            "ramQuota": 1024,
            "storageBackend": "couchstore"
        }, sync=True)
        quota_in_bytes = 1024 * 1024 * 1024  # 1,073,741,824 bytes

        # Ensure that the guard rail is enabled with a minimum of 10%
        testlib.post_succ(
            self.cluster, "/settings/resourceManagement/bucket/residentRatio",
            json={
                "enabled": True,
                "couchstoreMinimum": 10,
                "magmaMinimum": 1,
            })

        # Trigger the guard rail by injecting a new promQL query to set the
        # per-node data size just greater than 5X the quota, s.t.
        # quota / node size < 20%
        # This means that removing a node should mean that the remaining node
        # receives the whole data size, i.e.
        # quota / node size < 10%
        set_promql_queries(self.cluster,
                           data_size_bytes=1+5*quota_in_bytes)
        self.rebalance_with_cleanup(
            added_nodes=[],
            ejected_nodes=[self.cluster.connected_nodes[1]],
            initial_code=400,
            initial_expected_error='{"rr_will_be_too_low":"The following '
                                   'buckets are expected to breach the '
                                   'resident ratio minimum: test."}')

        spare = self.cluster.spare_node()

        # Swap rebalance should be permitted when the RR% is already below
        # the limit (quota / node size = quota / (1 + 10 * quota) < 10%)
        set_promql_queries(self.cluster,
                           data_size_bytes=1+10*quota_in_bytes)
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[self.cluster.connected_nodes[1]])

        # Check that we don't trigger the guard rail by setting the
        # per-node data size equal to 5X the quota, s.t.
        # removing a node should mean that the remaining node receives the whole
        # data size, i.e. quota/node size = 10% so the rebalance is allowed
        set_promql_queries(self.cluster,
                           data_size_bytes=5*quota_in_bytes)
        self.rebalance_with_cleanup(
            added_nodes=[],
            ejected_nodes=[self.cluster.connected_nodes[1]])

        # If a bucket violates the guardrail during storage migration, we do not
        # permit the swap rebalance
        self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "magma"
        })
        set_promql_queries(self.cluster,
                           data_size_bytes=1+10*quota_in_bytes)
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[self.cluster.connected_nodes[1]],
            initial_code=400,
            initial_expected_error='{"rr_will_be_too_low":"The following '
                                   'buckets are expected to breach the '
                                   'resident ratio minimum: test."}')

    def rebalance_data_size_test(self):
        # Create the bucket synchronously, so that the bucket is ready for the
        # rebalance checks to be performed correctly
        self.cluster.create_bucket({
            "name": BUCKET_NAME,
            "ramQuota": 1024,
            "storageBackend": "couchstore"
        }, sync=True)

        # Ensure that the guard rail is enabled with a maximum of 1GB
        testlib.post_succ(
            self.cluster, "/settings/resourceManagement/bucket/dataSizePerNode",
            json={
                "enabled": True,
                "couchstoreMaximum": 0.001,
                "magmaMaximum": 0.01
            })

        # Trigger the guard rail by injecting a new promQL query to set the
        # per-node data size to the max per node, s.t. the data size per node
        # after the rebalance will be just at the maximum
        set_promql_queries(self.cluster, data_size_bytes=500_000_000)
        self.rebalance_with_cleanup(
            added_nodes=[],
            ejected_nodes=[self.cluster.connected_nodes[1]],
            initial_code=400,
            initial_expected_error='{"data_size_will_be_too_high":"The '
                                   'following buckets are expected to breach '
                                   'the maximum data size per node: test."}')

        spare = self.cluster.spare_node()

        # Swap rebalance should be permitted when the per node data size is
        # already at the limit
        set_promql_queries(self.cluster, data_size_bytes=1_000_000_000)
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[self.cluster.connected_nodes[1]])

        # Rebalance should be permitted when the data size per node after the
        # rebalance will be just below the maximum
        set_promql_queries(self.cluster, data_size_bytes=499_999_999)
        self.rebalance_with_cleanup(
            added_nodes=[],
            ejected_nodes=[self.cluster.connected_nodes[1]])

        # If a bucket violates the guardrail during storage migration, we do not
        # permit the swap rebalance
        self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "magma"
        })
        set_promql_queries(self.cluster, data_size_bytes=1_000_000_000)
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[self.cluster.connected_nodes[1]],
            initial_code=400,
            initial_expected_error='{"data_size_will_be_too_high":"The '
                                   'following buckets are expected to breach '
                                   'the maximum data size per node: test."}')

    def rebalance_cores_per_bucket_test(self):
        pools = testlib.get_succ(self.cluster, "/pools/default").json()
        cpu_count = pools["nodes"][0]["cpuCount"]

        # Set minimum cores per bucket to cpu count, permitting exactly 1 bucket
        testlib.post_succ(self.cluster,
                          "/settings/resourceManagement/coresPerBucket",
                          json={
                              "enabled": True,
                              "minimum": cpu_count
                          })

        # We won't simulate different nodes having different core counts, so we
        # just care about whether there are 1 or 2 buckets and whether a node is
        # being added
        self.cluster.create_bucket({
            "name": BUCKET_NAME,
            "ramQuota": 100
        })

        # If there is 1 bucket, then we should allow all rebalances, i.e. all
        # rebalances where any nodes being added have sufficient cores
        self.rebalance_with_cleanup(
            added_nodes=[],
            ejected_nodes=[self.cluster.connected_nodes[1]])
        spare = self.cluster.spare_node()
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[])
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[self.cluster.connected_nodes[1]])

        # Temporarily allow 2 buckets
        testlib.post_succ(self.cluster,
                          "/settings/resourceManagement/coresPerBucket",
                          json={
                              "enabled": True,
                              "minimum": cpu_count / 2
                          })
        # Create a second bucket
        self.cluster.create_bucket({
            "name": BUCKET_NAME+"2",
            "ramQuota": 100
        })
        # Set the cores per bucket back to only allowing 1 bucket
        testlib.post_succ(self.cluster,
                          "/settings/resourceManagement/coresPerBucket",
                          json={
                              "enabled": True,
                              "minimum": cpu_count
                          })

        # If there are 2 buckets, then we should only allow rebalances where no
        # nodes are being added i.e. only rebalances where we are not adding a
        # node with insufficient cores
        self.rebalance_with_cleanup(
            added_nodes=[],
            ejected_nodes=[self.cluster.connected_nodes[1]])

        # Otherwise, we reject the rebalance
        error_msg = '{"not_enough_cores_for_num_buckets":"The following ' \
                    r'node\(s\) being added have insufficient cpu cores for ' \
                    r'the number of buckets already in the cluster: ' \
                    f'{spare.otp_node()}. "}}'
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[],
            initial_code=400,
            initial_expected_error=error_msg)
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[self.cluster.connected_nodes[1]],
            initial_code=400,
            initial_expected_error=error_msg)

    def rebalance_disk_usage_test(self):
        # Set maximum disk usage to 100, so that the disk usage must be below
        # the limit
        set_disk_guardrail_maximum(self.cluster, 100)

        # If the disk usage is below the limit, all rebalances should be
        # permitted
        self.rebalance_with_cleanup(
            added_nodes=[],
            ejected_nodes=[self.cluster.connected_nodes[1]])
        spare = self.cluster.spare_node()
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[])
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[self.cluster.connected_nodes[1]])

        # Set the maximum to 0, so that the disk usage must be above the limit
        set_disk_guardrail_maximum(self.cluster, 0)

        # If the disk usage is above the limit, only rebalances that don't
        # reduce the number of data nodes are permitted
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[])
        self.rebalance_with_cleanup(
            added_nodes=[spare],
            ejected_nodes=[self.cluster.connected_nodes[1]])

        # If the disk usage is above the limit and the rebalance will reduce the
        # number of data nodes, then we don't permit the rebalance
        self.rebalance_with_cleanup(
            added_nodes=[],
            ejected_nodes=[self.cluster.connected_nodes[1]],
            initial_code=400,
            initial_expected_error='{"disk_usage_too_high')

    def rebalance_with_cleanup(self, added_nodes, ejected_nodes, **kwargs):
        # Call a function which might start a rebalance which we want to cancel
        # and revert, once the function returns (or raises an exception)
        try:
            for node in added_nodes:
                self.cluster.add_node(node)
            self.cluster.rebalance(ejected_nodes=ejected_nodes,
                                   # We assume that the rebalance is still in
                                   # progress to simplify cleanup, so we must
                                   # not wait for it to complete
                                   wait=False,
                                   **kwargs)
        finally:
            testlib.post_succ(self.cluster, "/controller/stopRebalance")

            # Reset the promql queries so that rebalances are permitted
            set_promql_queries(self.cluster)
            original_settings = get_guard_rail_settings(self.cluster)
            disable_bucket_guard_rails(self.cluster)

            # Connected_nodes must be fixed if ejected_nodes weren't ejected
            self.cluster.connected_nodes = list(set(
                self.cluster.connected_nodes + ejected_nodes))

            # Rebalance ejected nodes back in and extra nodes out
            self.cluster.rebalance(ejected_nodes=added_nodes)

            testlib.post_succ(self.cluster, "/settings/resourceManagement",
                              json=original_settings)
            testlib.assert_eq(len(self.cluster.connected_nodes),
                              GuardRailRestrictionTests.num_connected,
                              "connected nodes")

    def storage_migration_test(self):
        # Test migration from couchstore to magma
        # Create the bucket synchronously, so that the bucket is ready for the
        # migration checks to be performed correctly
        self.cluster.create_bucket({
            "name": BUCKET_NAME,
            "ramQuota": 1024,
            "storageBackend": "couchstore"
        }, sync=True)

        # Ensure that the appropriate guard rails have expected limits
        testlib.post_succ(
            self.cluster, "/settings/resourceManagement/bucket/",
            json={
                "residentRatio": {
                    "enabled": True,
                    "couchstoreMinimum": 10,
                    "magmaMinimum": 1
                },
                "dataSizePerNode": {
                    "enabled": True,
                    "couchstoreMaximum": 1.6,
                    "magmaMaximum": 16
                }
            })

        # Test that we can't perform bucket migration from couchstore to magma
        # when the bucket doesn't satisfy the couchstore limits
        set_promql_queries(self.cluster, data_size_tb=10, resident_ratio=5)
        resp = self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "magma"
        }, expected_code=400).json()
        assert resp.get("errors", {}).get("storageBackend") == \
               "Storage mode migration is not allowed when data size per node" \
               " is above the configured maximum: 1.6TB", \
               f"Unexpected errors: {resp}"

        # Test that we can't perform bucket migration from couchstore to magma
        # when the bucket doesn't satisfy the couchstore data size limit
        set_promql_queries(self.cluster, data_size_tb=10, resident_ratio=15)
        resp = self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "magma"
        }, expected_code=400).json()
        assert resp.get("errors", {}).get("storageBackend") == \
               "Storage mode migration is not allowed when data size per node" \
               " is above the configured maximum: 1.6TB", \
               f"Unexpected errors: {resp}"

        # Test that we can't perform bucket migration from couchstore to magma
        # when the bucket doesn't satisfy the couchstore resident ratio limit
        set_promql_queries(self.cluster, data_size_tb=1, resident_ratio=5)
        resp = self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "magma"
        }, expected_code=400).json()
        assert resp.get("errors", {}).get("storageBackend") == \
               "Storage mode migration is not allowed when resident ratio is " \
               "below the configured minimum: 10%", f"Unexpected errors: {resp}"

        # Test that we can perform bucket migration from couchstore to magma
        # when the bucket satisfies the couchstore limits
        set_promql_queries(self.cluster, data_size_tb=1, resident_ratio=15)

        self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "magma"
        })

        # Test migration back from magma to couchstore

        # Test that we can't perform bucket migration from magma to couchstore
        # when the bucket doesn't satisfy the couchstore limits
        set_promql_queries(self.cluster, data_size_tb=10, resident_ratio=5)
        resp = self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "couchstore"
        }, expected_code=400).json()
        assert resp.get("errors", {}).get("storageBackend") == \
               "Storage mode migration is not allowed when data size per node" \
               " is above the configured maximum: 1.6TB", \
               f"Unexpected errors: {resp}"

        # Test that we can't perform bucket migration from magma to couchstore
        # when the bucket doesn't satisfy the couchstore data size limit
        set_promql_queries(self.cluster, data_size_tb=10, resident_ratio=15)
        resp = self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "couchstore"
        }, expected_code=400).json()
        assert resp.get("errors", {}).get("storageBackend") == \
               "Storage mode migration is not allowed when data size per node" \
               " is above the configured maximum: 1.6TB", \
               f"Unexpected errors: {resp}"

        # Test that we can't perform bucket migration from magma to couchstore
        # when the bucket doesn't satisfy the couchstore resident ratio limit
        set_promql_queries(self.cluster, data_size_tb=1, resident_ratio=5)
        resp = self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "couchstore"
        }, expected_code=400).json()
        assert resp.get("errors", {}).get("storageBackend") == \
               "Storage mode migration is not allowed when resident ratio is " \
               "below the configured minimum: 10%", f"Unexpected errors: {resp}"

        # Test that we can perform bucket migration from couchstore to magma
        # when the bucket satisfies the couchstore limits
        set_promql_queries(self.cluster, data_size_tb=1, resident_ratio=15)

        self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "storageBackend": "couchstore"
        })

    def max_collections_test(self):
        bucket_quota = 1000
        self.cluster.create_bucket({
            "name": BUCKET_NAME,
            "ramQuota": bucket_quota,
        })
        max_collections = 4
        testlib.post_succ(self.cluster, "/settings/resourceManagement/bucket"
                                        "/collectionsPerQuota",
                          json={
                              "enabled": True,
                              "maximum": max_collections/bucket_quota
                          })
        for i in range(max_collections):
            testlib.post_succ(self.cluster,
                              f"/pools/default/buckets/{BUCKET_NAME}"
                              f"/scopes/_default/collections",
                              data={"name": f"test_collection_{i}"})

        # Can't create more than the max collections per quota
        r = testlib.post_fail(self.cluster,
                              f"/pools/default/buckets/{BUCKET_NAME}"
                              f"/scopes/_default/collections",
                              data={
                                  "name": f"test_collection_{max_collections}"
                              },
                              expected_code=429)
        testlib.assert_eq(r.text, f'{{"errors":{{'
                          f'"_":"Maximum number of collections '
                          f'({max_collections}) for this bucket has been '
                          f'reached"}}}}')

        # Can't reduce the bucket quota once the limit has been reached
        r = self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "ramQuota": 999
        }, expected_code=400)
        testlib.assert_in("RAM quota cannot be less than 1000.0 MiB, to "
                          "support 4 collections", r.text)

        # Can still make other bucket updates when the limit has been reached
        self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "ramQuota": bucket_quota
        })

        testlib.post_succ(self.cluster, "/settings/resourceManagement/bucket"
                                        "/collectionsPerQuota",
                          json={
                              "enabled": True,
                              "maximum": 2 * max_collections / bucket_quota
                          })
        # Can't reduce the bucket quota below the min quota per collections
        r = self.cluster.update_bucket({
                "name": BUCKET_NAME,
                "ramQuota": 499
            }, expected_code=400)
        testlib.assert_in("RAM quota cannot be less than 500.0 MiB, to support "
                          "4 collections", r.text)

    def num_replicas_test(self):
        # Enable the guardrail
        testlib.post_succ(self.cluster,
                          "/settings/resourceManagement/diskUsage",
                          json={
                              "enabled": True,
                              "maximum": 50
                          })
        self.cluster.create_bucket({
            "name": BUCKET_NAME,
            "ramQuota": 100,
            "replicaNumber": 1
        }, sync=True)

        # Trigger the disk usage guardrail by setting the maximum to 0
        set_disk_guardrail_maximum(self.cluster, 0)

        # Test that the numReplicas cannot be increased
        r = self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "replicaNumber": 2
        }, expected_code=400)
        testlib.assert_in("The following data node(s) have insufficient disk "
                          "space to safely increase the number of replicas: ",
                          r.text)
        # Test that the numReplicas can be set to the same value
        self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "replicaNumber": 1
        })

        # Test that the numReplicas can be decreased
        self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "replicaNumber": 0
        })

        # Set disk usage maximum to 100, so the guardrail is no longer triggered
        set_disk_guardrail_maximum(self.cluster, 100)

        # Test that the numReplicas can now be increased
        self.cluster.update_bucket({
            "name": BUCKET_NAME,
            "replicaNumber": 2
        })


class DataIngressTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        # - Provisioned edition required for guard rails to be configurable
        # - 2 nodes so that we can test that all nodes reject write once the
        #   guard rail has been hit. Note, assert_cant_write will be less likely
        #   to test both nodes if num_nodes increases, so it should be modified
        #   at the same time.
        return testlib.ClusterRequirements(edition="Provisioned",
                                           min_num_nodes=2, num_connected=2)

    def setup(self):
        testlib.delete_all_buckets(self.cluster)
        self.cluster.create_bucket({
            "name": BUCKET_NAME,
            "ramQuota": 100
        })

    def teardown(self):
        testlib.delete_all_buckets(self.cluster)

    def test_teardown(self):
        # Reset the promQL queries to default values to ensure that they are
        # triggered consistently
        set_promql_queries(self.cluster)
        # Ensure that if the node removal was incorrectly permitted, we can run
        # further tests, by adding the node back in
        if len(self.cluster.connected_nodes) < 2:
            node = self.cluster.spare_node()
            self.cluster.add_node(node, do_rebalance=True)

    def rr_growth_test(self):
        # Disable other guard rails to ensure we don't get any unexpected
        # guard rails triggered (only seen with disk usage, but disabling all
        # to be extra sure)
        disable_bucket_guard_rails(self.cluster)

        # Ensure that the guard rail is enabled with a minimum of 10%
        testlib.post_succ(
            self.cluster, "/settings/resourceManagement/bucket/residentRatio",
            json={
                "enabled": True,
                "couchstoreMinimum": 10,
            })

        testlib.poll_for_condition(self.cluster.can_write(BUCKET_NAME,
                                                          "test_doc"),
                                   sleep_time=0.5, attempts=120)

        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME, "ok")

        # Make sure that we can successfully write to the bucket
        testlib.poll_for_condition(self.cluster.can_write(BUCKET_NAME,
                                                          "test_doc"),
                                   sleep_time=1, attempts=120,
                                   msg="write to bucket 'test'")

        # Trigger the guard rail by setting the resident ratio below the minimum
        set_promql_queries(self.cluster, resident_ratio=9)

        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME,
                                      "resident_ratio")

        # Expect write to fail
        assert_cant_write(self.cluster, BUCKET_NAME,
                          "Ingress disabled due to ratio between per-node "
                          "quota and data size exceeding configured limit")

        # Reset the promQL to verify that the status returns to ok
        set_promql_queries(self.cluster, resident_ratio=100)

        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME, "ok")

        # Writes should succeed again
        testlib.post_succ(
            self.cluster, "/pools/default/buckets/test/docs/test_doc",
            data="")

    def data_size_growth_test(self):
        # Disable other guard rails to ensure we don't get any unexpected
        # guard rails triggered (only seen with disk usage, but disabling all
        # to be extra sure)
        disable_bucket_guard_rails(self.cluster)

        # Ensure that the guard rail is enabled
        testlib.post_succ(
            self.cluster, "/settings/resourceManagement/bucket/dataSizePerNode",
            json={
                "enabled": True,
                "couchstoreMaximum": 1,
            })

        testlib.poll_for_condition(is_warmed_up(self.cluster, BUCKET_NAME),
                                   sleep_time=0.5, attempts=120)

        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME, "ok")

        # Make sure that we can successfully write to the cluster
        testlib.poll_for_condition(self.cluster.can_write(BUCKET_NAME,
                                                          "test_doc"),
                                   sleep_time=0.5, attempts=120)

        # Set the data size above the maximum
        set_promql_queries(self.cluster, data_size_tb=2)

        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME, "data_size")

        # Expect write to fail
        assert_cant_write(self.cluster, BUCKET_NAME,
                          "Ingress disabled due to data size exceeding "
                          "configured limit")

        # Set the data size below the maximum, to verify that the status goes
        # back to ok
        set_promql_queries(self.cluster, data_size_tb=0.5)

        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME, "ok")

        # Writes should succeed again
        testlib.post_succ(
            self.cluster, "/pools/default/buckets/test/docs/test_doc",
            data="")

    def disk_usage_growth_test(self):
        # Disable other guard rails to ensure we don't get any unexpected
        # guard rails triggered (only seen with disk usage, but disabling all
        # to be extra sure)
        disable_bucket_guard_rails(self.cluster)

        # Ensure that the guard rail is enabled, and set the limit high to avoid
        # false positives
        set_disk_guardrail_maximum(self.cluster, 100)

        testlib.poll_for_condition(is_warmed_up(self.cluster, BUCKET_NAME),
                                   sleep_time=0.5, attempts=120)

        # Wait for a stat to be populated, as the check will be ignored until we
        # get that stat from prometheus
        wait_for_stat(self.cluster, "sys_disk_usage_ratio", n=2)
        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME, "ok")

        # Make sure that we can successfully write to the cluster
        testlib.poll_for_condition(self.cluster.can_write(BUCKET_NAME,
                                                          "test_doc"),
                                   sleep_time=0.5, attempts=120)

        # Set the maximum to 0, so that the disk usage must be above the limit
        set_disk_guardrail_maximum(self.cluster, 0)

        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME, "disk_usage")

        # Expect write to fail
        assert_cant_write(self.cluster, BUCKET_NAME,
                          "Ingress disabled due to disk usage exceeding "
                          "configured limit")

        # Set maximum to 100, so that the disk usage must be below the limit
        set_disk_guardrail_maximum(self.cluster, 100)

        refresh_guard_rails(self.cluster)
        assert_bucket_resource_status(self.cluster, BUCKET_NAME, "ok")

        # Writes should succeed again
        testlib.post_succ(
            self.cluster, "/pools/default/buckets/test/docs/test_doc",
            data="")


def get_guard_rail_settings(cluster):
    return testlib.get_succ(cluster,
                            "/settings/resourceManagement/").json()


def disable_bucket_guard_rails(cluster):
    testlib.post_succ(
        cluster, "/settings/resourceManagement/",
        json={
            "bucket": {
                "residentRatio": {
                    "enabled": False
                },
                "dataSizePerNode": {
                    "enabled": False
                }
            },
            "diskUsage": {
                "enabled": False
            },
        })


# Set promQL queries such that they give fixed values, rather than depending on
# the cluster state. The default values are such that no guard rails will fire
def set_promql_queries(cluster, data_size_tb=.0, data_size_bytes=0,
                       disk_usage=0, resident_ratio=100, bucket=None):
    if bucket is None:
        bucket = BUCKET_NAME
    # Create a fake metric with a bucket label using up with a specific instance
    # and job to avoid duplicates
    bucket_metric_base = f'label_replace(up{{instance="ns_server",' \
                         f'job="general"}}, "bucket", "{bucket}", "", "")'

    testlib.post_succ(cluster, "/internalSettings",
                      data={"resourcePromQLOverride.dataSizePerNodeTB":
                            f"{data_size_tb} * {bucket_metric_base}",
                            "resourcePromQLOverride.dataSizePerNodeBytes":
                            f"{data_size_bytes} * {bucket_metric_base}",
                            "resourcePromQLOverride.dataResidentRatio":
                            f"{resident_ratio} * {bucket_metric_base}"})


def set_disk_guardrail_maximum(cluster, value):
    testlib.post_succ(cluster,
                      "/settings/resourceManagement/diskUsage",
                      json={
                          "enabled": True,
                          "maximum": value
                      })


def assert_bucket_resource_status(cluster, bucket, expected_status):
    status = get_bucket_status(cluster, bucket)
    assert status == expected_status, \
        f"Bucket '{bucket}' status was '{status}'. Expected '{expected_status}'"


def is_warmed_up(cluster, bucket):
    def f():
        r = testlib.get_succ(cluster, f"/pools/default/buckets/{bucket}")
        return all(node.get("status") == "healthy"
                   for node in r.json().get("nodes", []))
    return f


def assert_cant_write(cluster, bucket, exp_error):
    # Test 10 random keys, in order to likely test writing to both nodes
    for i in range(10):
        j = testlib.random_str(10)
        r = testlib.post_fail(
            cluster, f"/pools/default/buckets/{bucket}/docs/{j}",
            data="", expected_code=400)
        reason = r.json().get("reason")
        assert exp_error == reason, \
            f"Got unexpected error reason on write: '{reason}'. " \
            f"Expected '{exp_error}'"


def wait_for_stat(cluster, stat, n=1):

    def got_stats():
        params = stats_range_common_params()
        data = range_api_get(cluster, stat,
                             params=params)
        return len(data) >= n

    testlib.poll_for_condition(got_stats, 1, attempts=60,
                               msg=f"Wait for stat '{stat}' to be on {n} nodes")


def stats_range_common_params():
    return {'start': - 10,
            'step': 1}


def range_api_get(cluster, stat, params=None):
    if params is None:
        params = {}
    r = testlib.get_succ(
          cluster,
          f'/pools/default/stats/range/{stat}',
          params=params)
    r = r.json()
    print('stats res: ')
    pprint(r)
    return r['data']


def refresh_guard_rails(cluster):
    # Force each node's guardrail monitor to check for status changes
    for node in cluster.connected_nodes:
        testlib.diag_eval(node, "guardrail_monitor ! check,"
                                "gen_server:call(guardrail_monitor, sync).")

    node = cluster.wait_for_orchestrator()
    # Wait for the enforcer to handle the config updates
    testlib.diag_eval(node, "ns_config:sync(), "
                            "gen_server:call(guardrail_enforcer, sync).")


def get_bucket_status(cluster, bucket_name):
    node = cluster.wait_for_orchestrator()
    # Get the bucket status according to the orchestrator
    r = testlib.diag_eval(node,
                          f"guardrail_enforcer:get_status({{bucket, "
                          f"\"{bucket_name}\"}}).")
    return r.text


def get(key, response):
    url = response.url
    j = testlib.json_response(response, f"Response to {url} is not json")
    return testlib.assert_json_key(key, j, url)
