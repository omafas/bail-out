# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import testlib
import time


def get_bucket(cluster, bucket_name):
    return testlib.json_response(
        testlib.get_succ(cluster, f"/pools/default/buckets/{bucket_name}"),
        "non-json response for " + f"/pools/default/buckets/{bucket_name}")


def get_scopes(cluster, bucket):
    return testlib.json_response(
        testlib.get_succ(cluster,
                         f"/pools/default/buckets/{bucket}/scopes"),
        "non-json response for " +
        f"/pools/default/buckets/{bucket}/scopes")


def update_collection(cluster, bucket, scope, collection, data):
    return testlib.patch_succ(
        cluster,
        f"/pools/default/buckets/{bucket}/scopes/{scope}/"
        f"collections/{collection}",
        data=data)


def create_bucket(
    cluster, bucket, storage_mode, ram_quota_mb, eviction_policy=None
):
    data = {'name': f'{bucket}',
            'storageBackend': f'{storage_mode}',
            'ramQuotaMB': f'{ram_quota_mb}'}
    if eviction_policy:
        data["evictionPolicy"] = eviction_policy
    cluster.create_bucket(data)


def create_scope(cluster, bucket, scope):
    return testlib.post_succ(
        cluster, f"/pools/default/buckets/{bucket}/scopes",
        data={'name': f"{scope}"})


def assert_create_collection_fail(cluster, bucket, scope, data):
    return testlib.post_fail(
        cluster, f"/pools/default/buckets/{bucket}/scopes/{scope}/collections",
        expected_code=400, data=data)


def assert_create_collection_succ(cluster, bucket, scope, data):
    return testlib.post_succ(
        cluster, f"/pools/default/buckets/{bucket}/scopes/{scope}/collections",
        data=data)


def assert_modify_collection_fail(cluster, bucket, scope, data):
    collection = data['name']
    return testlib.patch_fail(
        cluster,
        f"/pools/default/buckets/{bucket}/scopes/{scope}/"
        f"collections/{collection}",
        expected_code=400, data=data)


def disable_history_on_all_collections(cluster, bucket_name):
    res = get_scopes(cluster, bucket_name)
    # Extract all collections and set history to false.
    for scope in res['scopes']:
        for collection in scope['collections']:
            update_collection(cluster, bucket_name,
                              scope['name'], collection['name'],
                              {'history': "false"})


def disable_history_on_bucket(cluster, bucket_name):
    cluster.update_bucket(
        {'name': f'{bucket_name}',
         'historyRetentionCollectionDefault': "false"})


def create_and_update_bucket(
    cluster,
    bucket_name,
    old_storage_mode,
    new_storage_mode,
    ram_quota_mb,
    old_eviction_policy=None,
    new_eviction_policy=None,
):
    create_bucket(
        cluster,
        bucket_name,
        old_storage_mode,
        ram_quota_mb,
        old_eviction_policy,
    )

    # magma -> couchstore migration won't proceed until history retention isn't
    # set to false.
    if old_storage_mode == "magma":
        disable_history_on_bucket(cluster, bucket_name)
        disable_history_on_all_collections(cluster, bucket_name)

    update_data = {
        "name": f"{bucket_name}",
        "storageBackend": f"{new_storage_mode}",
    }
    if new_eviction_policy:
        update_data["evictionPolicy"] = new_eviction_policy
        update_data["noRestart"] = "true"

    cluster.update_bucket(update_data)


def get_per_node_storage_mode(cluster, bucket_name):
    res = get_bucket(cluster, bucket_name)
    return {n['hostname']: n['storageBackend'] for n in res['nodes']
            if n.get('storageBackend') != None}


def get_per_node_eviction_policy(cluster, bucket_name):
    res = get_bucket(cluster, bucket_name)
    return {
        n["hostname"]: n["evictionPolicy"]
        for n in res["nodes"]
        if n.get("evictionPolicy") != None
    }

def assert_per_node_storage_mode_keys_added(cluster, bucket_name,
                                            expected_storage_mode):
    per_node_storage_mode = get_per_node_storage_mode(
        cluster, bucket_name)
    storage_modes = per_node_storage_mode.values()
    # Assert per-node storage_mode was added to all the nodes
    # and it's the expected_storage_mode.
    assert len(storage_modes) == len(cluster.connected_nodes) and \
        all([storage_mode == expected_storage_mode
             for storage_mode in storage_modes])


def assert_per_node_eviction_policy_keys_added(
    cluster, bucket_name, expected_eviction_policy
):
    per_node_eviction_policy = get_per_node_eviction_policy(
        cluster, bucket_name
    )
    eviction_policies = per_node_eviction_policy.values()

    assert len(eviction_policies) == len(cluster.connected_nodes) and all(
        [
            eviction_policy == expected_eviction_policy
            for eviction_policy in eviction_policies
        ]
    )


def assert_per_node_storage_mode_not_present(cluster, node, bucket_name):
    per_node_storage_mode = get_per_node_storage_mode(
        cluster, bucket_name)
    assert None == per_node_storage_mode.get(node.hostname())


def assert_per_node_eviction_policy_not_present(cluster, node, bucket_name):
    per_node_eviction_policy = get_per_node_eviction_policy(
        cluster, bucket_name
    )
    assert None == per_node_eviction_policy.get(node.hostname())


def assert_ejected_node_override_props_deleted(
        cluster, ejected_otp_node, bucket_name):

    # Node ejection is asynchronous at the end of a rebalance - i.e the
    # new chronicle_master might not have removed the node from nodes_wanted
    # and cleaned-up the override props yet.
    #
    # Wait for the ejected node to disappear from /nodeStatus and then check
    # if the override props have been deleted.

    def is_node_ejected():
        otp_nodes = testlib.get_otp_nodes(cluster).values()
        return all(ejected_otp_node != n for n in otp_nodes)

    testlib.poll_for_condition(
        is_node_ejected, sleep_time=0.5, attempts=30, timeout=60,
        msg=f"ejecting node {ejected_otp_node}")

    code = f"""
        {{ok, BucketConfig}} = ns_bucket:get_bucket("{bucket_name}"),
        OverrideSubKeys = [storage_mode, eviction_policy, autocompaction],
        lists:member(true,
            [true || {{{{node, '{ejected_otp_node}', SK}}, _V}}
                    <- BucketConfig, lists:member(SK, OverrideSubKeys)])
        """
    r = testlib.diag_eval(cluster, code).content.decode('ascii')
    assert r == "false", \
        f"per-node override props clean-up failed for " \
        f"ejected node: {ejected_otp_node}"


def assert_per_node_storage_mode_in_memcached(
    node, bucket_name, expected_storage_mode
):
    storage_mode = (
        testlib.diag_eval(
            node,
            f'ns_memcached:get_config_stats("{bucket_name}", <<"ep_backend">>).',
        )
        .content.decode("ascii")
        .strip('<<"')
        .strip('">>')
    )

    if expected_storage_mode == "couchstore":
        expected_storage_mode = "couchdb"
    assert storage_mode == expected_storage_mode


def assert_per_node_eviction_policy_in_memcached(
    node, bucket_name, expected_eviction_policy
):
    eviction_policy = (
        testlib.diag_eval(
            node,
            f'ns_memcached:get_config_stats("{bucket_name}", <<"ep_item_eviction_policy">>).',
        )
        .content.decode("ascii")
        .strip('<<"')
        .strip('">>')
    )

    # Map bucket config eviction policy to memcached format
    eviction_policy_map = {
        "valueOnly": "value_only",
        "fullEviction": "full_eviction",
        "noEviction": "no_eviction",
    }

    expected_eviction_policy = eviction_policy_map.get(
        expected_eviction_policy, expected_eviction_policy
    )
    assert eviction_policy == expected_eviction_policy


def _find_available_new_node(cluster, old_nodes):
    """Find an available new node for swap rebalance"""
    for candidate_node in cluster._nodes:
        if candidate_node not in old_nodes:
            return candidate_node
    return None


def _validate_swap_rebalance_result(
    cluster,
    new_node,
    ejected_node,
    bucket_name,
    old_otp_nodes,
    expected_storage_mode,
    expected_eviction_policy,
):
    """Common validation logic for both single and all-node swaps"""
    if expected_storage_mode:
        assert_per_node_storage_mode_in_memcached(
            new_node, bucket_name, expected_storage_mode
        )
        assert_per_node_storage_mode_not_present(cluster, new_node, bucket_name)

    if expected_eviction_policy:
        assert_per_node_eviction_policy_in_memcached(
            new_node, bucket_name, expected_eviction_policy
        )
        assert_per_node_eviction_policy_not_present(
            cluster, new_node, bucket_name
        )

    assert_ejected_node_override_props_deleted(
        cluster, old_otp_nodes[ejected_node.hostname()], bucket_name
    )


def perform_single_node_swap_rebalance(
    cluster,
    bucket_name,
    node_index=0,  # Default to first node, but configurable
    expected_storage_mode=None,
    expected_eviction_policy=None,
):
    """
    Perform swap rebalance on a single node to apply storage mode and/or eviction policy changes.

    Args:
        cluster: The cluster object
        bucket_name: Name of the bucket to migrate
        node_index: Index of the node to replace (default: 0)
        expected_storage_mode: Expected storage mode after rebalance (optional)
        expected_eviction_policy: Expected eviction policy after rebalance (optional)
    """
    old_nodes = cluster.connected_nodes.copy()
    old_otp_nodes = testlib.get_otp_nodes(cluster)

    # Validate that we have enough nodes
    if not old_nodes or node_index >= len(old_nodes):
        raise ValueError(
            f"Invalid node_index {node_index}. Connected nodes: {len(old_nodes)}"
        )

    # Select the target node to replace
    target_node = old_nodes[node_index]

    # Find a new node to add
    new_node = _find_available_new_node(cluster, old_nodes)
    if new_node is None:
        raise ValueError("No available new node for swap rebalance")

    # Add new node and remove old node
    cluster.add_node(new_node, verbose=True)
    cluster.rebalance(ejected_nodes=[target_node], wait=True, verbose=True)

    # Common validation logic
    _validate_swap_rebalance_result(
        cluster,
        new_node,
        target_node,
        bucket_name,
        old_otp_nodes,
        expected_storage_mode,
        expected_eviction_policy,
    )


def perform_node_swap_rebalance(
    cluster,
    bucket_name,
    expected_storage_mode=None,
    expected_eviction_policy=None,
):
    """
    Perform node swap rebalance to apply storage mode and/or eviction policy changes.
    This function replaces ALL nodes in the cluster.

    Args:
        cluster: The cluster object
        bucket_name: Name of the bucket to migrate
        expected_storage_mode: Expected storage mode after rebalance (optional)
        expected_eviction_policy: Expected eviction policy after rebalance (optional)
    """
    nodes_in_cluster = len(cluster.connected_nodes)
    old_nodes = cluster.connected_nodes.copy()  # Capture original nodes once
    old_otp_nodes = testlib.get_otp_nodes(cluster)

    count = 0
    for new_node in cluster._nodes:
        if new_node in old_nodes:
            continue
        # Check if we already replaced all the nodes:
        if count >= nodes_in_cluster:
            break

        cluster.add_node(new_node, verbose=True)
        # Use the original old_nodes list (not current connected_nodes)
        node_to_eject = old_nodes[count]
        cluster.rebalance(
            ejected_nodes=[node_to_eject], wait=True, verbose=True
        )

        # Use shared validation logic
        _validate_swap_rebalance_result(
            cluster,
            new_node,
            node_to_eject,
            bucket_name,
            old_otp_nodes,
            expected_storage_mode,
            expected_eviction_policy,
        )
        count += 1


def perform_single_node_failover_full_recovery(
    cluster,
    bucket_name,
    node_index=0,  # Default to first node, but configurable
    expected_storage_mode=None,
    expected_eviction_policy=None,
):
    """
    Perform failover and full recovery on a single node to apply storage mode and/or eviction policy changes.

    Args:
        cluster: The cluster object
        bucket_name: Name of the bucket to migrate
        node_index: Index of the node to failover/recover (default: 0)
        expected_storage_mode: Expected storage mode after recovery (optional)
        expected_eviction_policy: Expected eviction policy after recovery (optional)
    """
    nodes = cluster.connected_nodes

    # Validate that we have enough nodes
    if not nodes or node_index >= len(nodes):
        raise ValueError(
            f"Invalid node_index {node_index}. Connected nodes: {len(nodes)}"
        )

    # Select the target node for failover/recovery
    target_node = nodes[node_index]

    cluster.failover_node(target_node, graceful=False)
    cluster.recover_node(target_node, recovery_type="full", do_rebalance=True)

    # Verify node has the updated configuration
    if expected_storage_mode:
        assert_per_node_storage_mode_in_memcached(
            target_node, bucket_name, expected_storage_mode
        )
        assert_per_node_storage_mode_not_present(
            cluster, target_node, bucket_name
        )

    if expected_eviction_policy:
        assert_per_node_eviction_policy_in_memcached(
            target_node, bucket_name, expected_eviction_policy
        )
        assert_per_node_eviction_policy_not_present(
            cluster, target_node, bucket_name
        )


def perform_failover_full_recovery(
    cluster,
    bucket_name,
    expected_storage_mode=None,
    expected_eviction_policy=None,
):
    """
    Perform failover and recovery rebalance to apply storage mode and/or eviction policy changes.
    This function performs failover/recovery on ALL nodes in the cluster.

    Args:
        cluster: The cluster object
        bucket_name: Name of the bucket to migrate
        expected_storage_mode: Expected storage mode after rebalance (optional)
        expected_eviction_policy: Expected eviction policy after rebalance (optional)
    """
    nodes_count = len(cluster.connected_nodes)
    for node_index in range(nodes_count):
        perform_single_node_failover_full_recovery(
            cluster,
            bucket_name,
            node_index=node_index,
            expected_storage_mode=expected_storage_mode,
            expected_eviction_policy=expected_eviction_policy,
        )


def migrate_storage_mode(
    cluster,
    old_storage_mode,
    new_storage_mode,
    id,
    old_eviction_policy=None,
    new_eviction_policy=None,
):
    bucket = f"bucket-{id}"
    scope = f"scope-{id}"
    collection = f"collection-{id}"
    create_and_update_bucket(
        cluster,
        bucket,
        old_storage_mode,
        new_storage_mode,
        1024,
        old_eviction_policy,
        new_eviction_policy,
    )
    assert_per_node_storage_mode_keys_added(
        cluster, bucket, old_storage_mode)
    if old_eviction_policy:
        assert_per_node_eviction_policy_keys_added(
            cluster, bucket, old_eviction_policy
        )

    create_scope(cluster, bucket, scope)
    # try creating a collection with history: true for a bucket marked to
    # be migrated to magma and it should fail.
    if new_storage_mode == "magma":
        assert_create_collection_fail(
            cluster, bucket, scope,
            {'name': collection,
             'history': 'true'})

        assert_create_collection_succ(
            cluster, bucket, scope,
            {'name': collection,
             'history': 'false'})

        # assert history can not be set to true.
        assert_modify_collection_fail(
            cluster, bucket, scope,
            {'name': collection,
             'history': 'true'})

        # modify some other prop of the collection and it should pass.
        update_collection(cluster, bucket=bucket, scope=scope,
                          collection=collection, data={'maxTTL': 15})

    cluster.delete_bucket(bucket)


class BucketMigrationTest(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise",
                                           min_num_nodes=4, min_memsize=2*1024,
                                           num_connected=2)

    def setup(self):
        testlib.delete_all_buckets(self.cluster)
        testlib.diag_eval(
            self.cluster,
            "ns_config:set(allow_online_eviction_policy_change, true).",
        )

    def teardown(self):
        testlib.diag_eval(
            self.cluster,
            "ns_config:delete(allow_online_eviction_policy_change).",
        )

    def test_teardown(self):
        testlib.delete_all_buckets(self.cluster)

    def migrate_storage_mode_test(self):
        migrate_storage_mode(
            self.cluster, old_storage_mode="couchstore",
            new_storage_mode="magma", id=1)
        migrate_storage_mode(
            self.cluster, old_storage_mode="magma",
            new_storage_mode="couchstore", id=2)

    def migrate_storage_mode_and_eviction_policy_test(self):
        migrate_storage_mode(
            self.cluster,
            old_storage_mode="couchstore",
            new_storage_mode="magma",
            id=5,
            old_eviction_policy="valueOnly",
            new_eviction_policy="fullEviction",
        )
        migrate_storage_mode(
            self.cluster,
            old_storage_mode="magma",
            new_storage_mode="couchstore",
            id=6,
            old_eviction_policy="fullEviction",
            new_eviction_policy="valueOnly",
        )

    def migrate_storage_mode_then_eviction_policy_via_rebalance_test(
        self,
    ):
        """Test storage mode migration followed by eviction policy change with "
        "--no-restart"""
        bucket_name = "bucket-no-restart-test"

        create_bucket(
            self.cluster, bucket_name, "couchstore", 1024, "valueOnly"
        )

        update_data = {
            "name": bucket_name,
            "storageBackend": "magma",
        }

        self.cluster.update_bucket(update_data)

        assert_per_node_storage_mode_keys_added(
            self.cluster, bucket_name, "couchstore"
        )

        # Verify no eviction policy overrides are present
        per_node_eviction_policy = get_per_node_eviction_policy(
            self.cluster, bucket_name
        )
        assert (
            per_node_eviction_policy == {}
        ), f"Expected no eviction policy overrides, got: {per_node_eviction_policy}"

        # Try to update eviction policy without noRestart - should fail
        update_data_with_eviction_no_flag = {
            "name": bucket_name,
            "evictionPolicy": "fullEviction",
        }

        self.cluster.update_bucket(
            update_data_with_eviction_no_flag, expected_code=400
        )

        update_data_with_eviction = {
            "name": bucket_name,
            "evictionPolicy": "fullEviction",
            "noRestart": "true",
        }

        self.cluster.update_bucket(update_data_with_eviction)

        assert_per_node_storage_mode_keys_added(
            self.cluster, bucket_name, "couchstore"
        )
        assert_per_node_eviction_policy_keys_added(
            self.cluster, bucket_name, "valueOnly"
        )

        perform_single_node_swap_rebalance(
            self.cluster,
            bucket_name,
            node_index=0,
            expected_storage_mode="magma",
            expected_eviction_policy="fullEviction",
        )

        self.cluster.delete_bucket(bucket_name)

    def migrate_eviction_policy_then_storage_mode_via_rebalance_test(self):
        """Test eviction policy change with --no-restart followed by storage mode change"""
        bucket_name = "bucket-eviction-then-storage-test"

        # Step 1: Create bucket with magma and fullEviction eviction policy
        create_bucket(
            self.cluster, bucket_name, "couchstore", 1024, "valueOnly"
        )

        # Step 2: Change eviction policy with --no-restart (should add per-node overrides)
        update_data_eviction = {
            "name": bucket_name,
            "evictionPolicy": "fullEviction",
            "noRestart": "true",
        }

        self.cluster.update_bucket(update_data_eviction)

        # Step 3: Verify eviction policy overrides are added
        assert_per_node_eviction_policy_keys_added(
            self.cluster, bucket_name, "valueOnly"
        )

        # Step 4: Verify no storage mode overrides are present yet
        per_node_storage_mode = get_per_node_storage_mode(
            self.cluster, bucket_name
        )
        assert (
            per_node_storage_mode == {}
        ), f"Expected no storage mode overrides, got: {per_node_storage_mode}"

        # Step 5: Change storage mode (should add storage mode overrides)
        update_data_storage = {
            "name": bucket_name,
            "storageBackend": "magma",
        }

        self.cluster.update_bucket(update_data_storage)

        # Step 6: Verify both eviction policy and storage mode overrides are present
        assert_per_node_eviction_policy_keys_added(
            self.cluster, bucket_name, "valueOnly"
        )
        assert_per_node_storage_mode_keys_added(
            self.cluster, bucket_name, "couchstore"
        )

        # Step 7: Perform swap rebalance to apply both changes
        perform_single_node_swap_rebalance(
            self.cluster,
            bucket_name,
            node_index=1,
            expected_storage_mode="magma",
            expected_eviction_policy="fullEviction",
        )

        self.cluster.delete_bucket(bucket_name)

    def disallow_storage_mode_migration_when_history_set_test(self):
        bucket = "bucket-1"
        data = {'name': bucket,
                'storageBackend': "magma",
                'ramQuotaMB': 1024}

        self.cluster.create_bucket(data)

        storage_mode_update = {"name": bucket, "storageBackend": "couchstore"}

        self.cluster.update_bucket(storage_mode_update, expected_code=400)

        disable_history_on_bucket(self.cluster, bucket)

        self.cluster.update_bucket(storage_mode_update, expected_code=400)

        disable_history_on_all_collections(self.cluster, bucket)

        self.cluster.update_bucket(storage_mode_update)

        self.cluster.delete_bucket(bucket)

    def migrate_storage_mode_via_rebalance_test(self):
        create_and_update_bucket(self.cluster, "bucket-1", "couchstore",
                                 "magma", 1024)
        assert_per_node_storage_mode_keys_added(self.cluster, "bucket-1",
                                                "couchstore")

        perform_single_node_swap_rebalance(
            self.cluster,
            "bucket-1",
            node_index=0,
            expected_storage_mode="magma",
        )

    def migrate_storage_mode_and_eviction_policy_via_rebalance_test(
        self,
    ):
        """Test migration of both storage mode and eviction policy
        across all nodes in the cluster to verify complete override cleanup."""
        create_and_update_bucket(
            self.cluster,
            "bucket-2",
            "couchstore",
            "magma",
            1024,
            "valueOnly",
            "fullEviction",
        )
        assert_per_node_storage_mode_keys_added(
            self.cluster, "bucket-2", "couchstore"
        )
        assert_per_node_eviction_policy_keys_added(
            self.cluster, "bucket-2", "valueOnly"
        )

        # Use full cluster replacement to verify both storage mode and eviction policy
        # changes are applied across all nodes and all overrides are cleaned up
        perform_node_swap_rebalance(
            self.cluster,
            "bucket-2",
            expected_storage_mode="magma",
            expected_eviction_policy="fullEviction",
        )

    def migrate_storage_mode_via_full_recovery_test(self):
        create_and_update_bucket(
            self.cluster, "bucket-2", "couchstore", "magma", 1024)
        assert_per_node_storage_mode_keys_added(
            self.cluster, "bucket-2", "couchstore"
        )

        perform_single_node_failover_full_recovery(
            self.cluster,
            "bucket-2",
            node_index=0,
            expected_storage_mode="magma",
        )

    def migrate_storage_mode_and_eviction_policy_via_full_recovery_test(self):
        create_and_update_bucket(
            self.cluster,
            "bucket-3",
            "couchstore",
            "magma",
            1024,
            "valueOnly",
            "fullEviction",
        )
        assert_per_node_storage_mode_keys_added(
            self.cluster, "bucket-3", "couchstore"
        )
        assert_per_node_eviction_policy_keys_added(
            self.cluster, "bucket-3", "valueOnly"
        )

        perform_failover_full_recovery(
            self.cluster,
            "bucket-3",
            expected_storage_mode="magma",
            expected_eviction_policy="fullEviction",
        )

    def perform_delta_recovery_mid_storage_and_eviction_policy_migration_test(
        self,
    ):
        bucket_name = "bucket-4"
        create_and_update_bucket(
            self.cluster,
            bucket_name=bucket_name,
            old_storage_mode="couchstore",
            new_storage_mode="magma",
            ram_quota_mb=1024,
            old_eviction_policy="valueOnly",
            new_eviction_policy="fullEviction",
        )

        def is_bucket_online_on_all_nodes():
            r = get_bucket(self.cluster, bucket_name)
            return all([node["status"] == "healthy" for node in r["nodes"]])

        testlib.poll_for_condition(
            is_bucket_online_on_all_nodes,
            sleep_time=0.5,
            attempts=20,
            timeout=60,
            msg="poll bucket is online on all nodes",
        )

        # Failover a node and delta-recover it - the bucket should still have
        # per-node storage_mode/eviction_policy override props. Storage mode
        # and eviction policy in memcached on the recovered node should be the
        # old values.

        failover_node = self.cluster.connected_nodes[0]
        self.cluster.failover_node(failover_node, graceful=False)
        self.cluster.recover_node(
            failover_node, recovery_type="delta", do_rebalance=True
        )

        assert_per_node_storage_mode_keys_added(
            self.cluster, bucket_name, "couchstore"
        )
        # Eviction policy overrides should remain during delta recovery
        # All nodes should have eviction policy overrides during migration
        assert_per_node_eviction_policy_keys_added(
            self.cluster, bucket_name, "valueOnly"
        )
        assert_per_node_storage_mode_in_memcached(
            failover_node, bucket_name, "couchstore"
        )
        # Eviction policy should be the old value (override preserved) during delta recovery
        assert_per_node_eviction_policy_in_memcached(
            failover_node, bucket_name, "valueOnly"
        )
