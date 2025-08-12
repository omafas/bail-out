# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This test currently tests:
# 1.) The json structure of all the *_settings_managers is validated to be
#     valid json.
# 2.) GET requests to all the endpoints and validation of type and default
# 3.) METAKV entries for extra "shardAffinity" setting and it's unusual upgrade
#     path. It will use a metakv entry to determine the default value post
#     upgrade. We can't fully test this until we have mixed-version
#     cluster_tests. TODO: mixed-version upgrade testing for shard-affinity
# 4.) TODO: PUT/POST tests for the various *_settings_managers

import json
import testlib
import base64

from testlib.util import Service

SECRET_KEY = "/indexing/settings/config/features/ShardAffinity"

# TODO: create a class for this?
def create_inner(default, type_only=False):
    return {
        "validator": lambda x: type(x) is type(default),
        "default": default,
        "type_only": type_only
    }

# TODO: there is currently no eventing endpoint to test since this is only set
# during cluster-init and never modified.
# EVENTING_SETTINGS = {
#     "ramQuota": create_inner(256)
# }

ANALYTICS_SETTINGS = {
    "numReplicas": create_inner(0),
}

INDEX_SETTINGS = {
    "redistributeIndexes": create_inner(False),
    "numReplica": create_inner(0),
    "enableShardAffinity": create_inner(False, type_only=True),
    "enablePageBloomFilter": create_inner(False, type_only=True),
    "indexerThreads": create_inner(0),
    "memorySnapshotInterval": create_inner(200),
    "stableSnapshotInterval": create_inner(5000),
    "maxRollbackPoints": create_inner(2),
    "logLevel": create_inner("info"),
    "storageMode": create_inner("plasma")
}

def shard_affinty_blob(true_false):
    return f"{{\"indexer.default.enable_shard_affinity\": {true_false}}}"

SHARD_AFFINITY_OBJ_KEY = "indexer.default.enable_shard_affinity"

QUERY_SETTINGS = {
    "queryTmpSpaceDir": create_inner("", type_only=True),
    "queryTmpSpaceSize": create_inner(5120),
    "queryPipelineBatch": create_inner(16),
    "queryPipelineCap": create_inner(512),
    "queryScanCap": create_inner(512),
    "queryTimeout": create_inner(0),
    "queryPreparedLimit": create_inner(16384),
    "queryCompletedLimit": create_inner(4000),
    "queryCompletedThreshold": create_inner(1000),
    "queryLogLevel": create_inner("info"),
    "queryMaxParallelism": create_inner(1),
    "queryN1QLFeatCtrl": create_inner(76, type_only=True),
    "queryTxTimeout": create_inner("0ms"),
    "queryMemoryQuota": create_inner(0),
    "queryUseCBO": create_inner(True),
    "queryCleanupClientAttempts": create_inner(True),
    "queryCleanupLostAttempts": create_inner(True),
    "queryCleanupWindow": create_inner("60s"),
    "queryNumAtrs": create_inner(1024),
    "queryNodeQuota": create_inner(0),
    "queryUseReplica": create_inner("unset"),
    "queryNodeQuotaValPercent": create_inner(67),
    "queryNumCpus": create_inner(0),
    "queryCompletedMaxPlanSize": create_inner(262144),
    "queryCurlWhitelist": create_inner({"all_access": False, "allowed_urls": [],
                                        "disallowed_urls": []})
}

def _get_test_generator(cluster, endpoint, defaults):
    result = testlib.get_succ(cluster, endpoint)
    json = result.json()
    # make sure we have the same set of keys
    assert sorted(json.keys()) == sorted(defaults.keys())

    # go through all key/value pairs and validate them based on type and/or
    # equality
    for (k, v) in defaults.items():
        assert k in json.keys()
        if not v["type_only"]:
            assert json[k] == v["default"]
        assert v["validator"](json[k])

# most of this is just to confirm we have a valid json blob in the metakv entry
def _json_blob_test_gen(cluster, url):
    value = testlib.metakv_get_succ(cluster, url).json()["value"]
    dictionary = json.loads(base64.b64decode(value))
    assert dictionary is not {} # there are no empty settings, that we know of

def get_bool(value):
    if value == b"True":
        return True
    elif value == b"False":
        return False
    elif value == True:
        return True
    elif value == False:
        return False
    else:
        raise Exception("Invalid boolean string conversion")

class SettingsManagersTests(testlib.BaseTestSet):

    def __init__(self, cluster) -> None:
        super().__init__(cluster)

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes=1, min_memsize=1024,
                                           services=[Service.KV,
                                                     Service.INDEX,
                                                     Service.EVENTING,
                                                     Service.CBAS,
                                                     Service.QUERY])

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_teardown(self):
        pass

    def index_settings_manager_test(self):
        _get_test_generator(self.cluster, "/settings/indexes", INDEX_SETTINGS)

    def index_settings_manager_json_test(self):
        _json_blob_test_gen(self.cluster, "/indexing/settings/config")

    def query_settings_manager_test(self):
        _get_test_generator(self.cluster, "/settings/querySettings",
                            QUERY_SETTINGS)

    def query_settings_manager_json_test(self):
        _json_blob_test_gen(self.cluster, "/query/settings/config")

    def eventing_settings_manager_json_test(self):
        _json_blob_test_gen(self.cluster, "/eventing/settings/config")

    def analytics_settings_manager_test(self):
        _get_test_generator(self.cluster, "/settings/analytics",
                            ANALYTICS_SETTINGS)

    def analytics_settings_manager_json_test(self):
        _json_blob_test_gen(self.cluster, "/analytics/settings/config")

    # Custom test for a "magic" metakv key that will be introduced. Doesn't do
    # a ton at the moment but can hopefully be expanded later.
    def enable_shard_affinity_metakv_test(self):
        testlib.metakv_put_succ(self.cluster, SECRET_KEY,
                                data = {'value': shard_affinty_blob("true")})
        result = testlib.metakv_get_succ(self.cluster, SECRET_KEY)
        data = base64.b64decode(result.json()["value"])
        obj = json.loads(data)
        assert get_bool(obj[SHARD_AFFINITY_OBJ_KEY])
        testlib.metakv_delete_succ(self.cluster, SECRET_KEY)
        shouldntbethere = testlib.metakv_get(self.cluster, SECRET_KEY)
        assert shouldntbethere.json() == []
        assert shouldntbethere.status_code == 404
