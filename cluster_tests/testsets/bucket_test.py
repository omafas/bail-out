# @author Couchbase <info@couchbase.com>
# @copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import string
import sys
import random
import time
import collections
import itertools
import traceback

import requests
import testlib

"""
How to add a new test for a new parameter, param:
- Add good values to self.test_params['param'] in
    BucketTestSetBase.init_test_params(...) as a list of values, including None
    if the parameter is optional
- Add bad values to self.bad_params['param'] in
    BucketTestSetBase.init_bad_params(...) as a list of values
- Provide expected errors that will occur with the bad values in
    BucketTestSetBase.get_errors(...)
- Limits can be optionally set in BucketTestSetBase.init_limits(...) if useful
    for the above 3 additions
- Possibly provide a clause in was_bucket_change_made / 
    was_auto_compaction_change_made to parse the values from the response to a bucket
    GET request, in order to verify that the new parameter value is present
- Add a test function to the appropriate test class (BasicBucketTestSet,
    OnPremBucketTestSet, MultiNodeBucketTestSet, or ServerlessBucketTestSet).
    This should contain a call of self.test_param("param", **main_params) with
    main_params defining which tests are to be performed
Note:
- To test related parameters together, a combined parameter can be used, where
    the name is the related parameters separated by ',' and the test_params /
    bad_params values are given as a tuple specified for the combined parameter.
    For an example, see allowedTimePeriod or pitr
- Both bucket level and global auto-compaction parameters should be handled here
    with the corresponding tests in auto_compaction_test.py - the reason for
    this is to ensure that combinations of auto-compaction and other parameters
    in the same request can be tested. Bucket level auto-compaction parameters
    should be added to AUTO_COMPACTION_PARAMETERS
"""

BUCKETS_ENDPOINT = "/pools/default/buckets"
BUCKET_ENDPOINT_DEFAULT = BUCKETS_ENDPOINT + "/default"

SET_AUTO_COMPACTION_ENDPOINT = "/controller/setAutoCompaction"
GET_AUTO_COMPACTION_ENDPOINT = "/settings/autoCompaction"

PITR_PARAMS = "pitrGranularity,pitrMaxHistoryAge"
ALLOWED_TIME_PERIOD_PARAMS = "allowedTimePeriod[fromHour]," \
                             "allowedTimePeriod[fromMinute]," \
                             "allowedTimePeriod[toHour]," \
                             "allowedTimePeriod[toMinute]," \
                             "allowedTimePeriod[abortOutside]"

AUTO_COMPACTION_PARAMETERS = ["parallelDBAndViewCompaction",
                              "databaseFragmentationThreshold[percentage]",
                              "databaseFragmentationThreshold[size]",
                              "viewFragmentationThreshold[percentage]",
                              "viewFragmentationThreshold[size]",
                              "purgeInterval",
                              "allowedTimePeriod[fromHour]",
                              "allowedTimePeriod[fromMinute]",
                              "allowedTimePeriod[toHour]",
                              "allowedTimePeriod[toMinute]",
                              "allowedTimePeriod[abortOutside]",
                              "magmaFragmentationPercentage"]

def is_flush_enabled(info):
    return "flush" in info['controllers']


def get_ram_quota(info):
    return int(info['quota']['rawRAM'] / 1048576)


def get_bucket_type(bucketType):
    if bucketType == "membase":
        return "couchbase"
    return bucketType


def get_storage_backend(info):
    if info.get('storageBackend') in ["undefined", None]:
        return "couchstore"
    else:
        return info['storageBackend']


def to_value(value):
    if type(value) == bool:
        return ["false", "true"][value]
    else:
        return value


def was_bucket_change_made(new_info, changes):
    change_was_made = True
    if "ramQuota" in changes and "ramQuotaMB" in changes:
        changes.pop("ramQuotaMB")
    for key, change_val in changes.items():
        if key == "name":
            cur_val = new_info.get(key)
        elif key == "bucketType":
            cur_val = get_bucket_type(new_info.get(key))
            change_val = get_bucket_type(change_val)
        elif key in ["ramQuota", "ramQuotaMB"]:
            cur_val = get_ram_quota(new_info)
        elif key == "storageBackend":
            cur_val = get_storage_backend(new_info)
        elif key == "replicaIndex":
            cur_val = new_info.get(key)
        elif key == "flushEnabled":
            cur_val = int(is_flush_enabled(new_info))
        elif key == "autoCompactionDefined":
            continue
        elif to_value(new_info.get(key)) != change_val:
            cur_val = to_value(new_info.get(key))
        else:
            continue

        if cur_val != change_val:
            print(f"Change to {key} not made from {cur_val} to {change_val}")
            change_was_made = False
    return change_was_made


def was_auto_compaction_change_made(new_info, changes):
    change_was_made = True
    auto_compaction_defined = "autoCompactionSettings" in new_info and \
                              new_info['autoCompactionSettings'] != False

    for key, change_val in changes.items():
        if key not in AUTO_COMPACTION_PARAMETERS:
            continue
        if key == "purgeInterval":
            cur_val = float(new_info.get(key, 0))
        elif auto_compaction_defined:
            cur_val = new_info['autoCompactionSettings']
            key_parts = key.replace(']', '').split('[')
            for part in key_parts:
                cur_val = cur_val.get(part, None)
                if cur_val == None:
                    break
            cur_val = to_value(cur_val)
        elif new_info.get(key) != change_val:
            cur_val = new_info.get(key)
        else:
            continue

        if cur_val != change_val:
            print(f"AutoComp change to {key} not made from {cur_val} to "
                  f"{change_val}")
            change_was_made = False
    return change_was_made


def was_change_made(new_info, changes):
    bucket_changes = {k: v for k, v in changes.items()
                      if k not in AUTO_COMPACTION_PARAMETERS}
    auto_compaction_changes = {k: v for k, v in changes.items()
                               if k in AUTO_COMPACTION_PARAMETERS}
    return was_bucket_change_made(new_info, bucket_changes) and \
           was_auto_compaction_change_made(new_info, auto_compaction_changes)

# TODO: split out generic behaviour for use with other endpoints and to aid with
#  de-tangling auto compaction from buckets
class BucketTestSetBase(testlib.BaseTestSet):

    def __init__(self, cluster):
        super().__init__(cluster)
        self.memsize = None
        self.next_bucket_id = 0

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    # Send a request with no validation of response
    def request(self, method, endpoint, **kwargs):
        return testlib.request(method, self.cluster, endpoint, **kwargs)

    # Send a request and validate the response is as expected, as well as
    # checking that any expected bucket changes were made
    def test_request(self, method, endpoint, data=None, json_data=None,
                     headers=None, code=None, errors=None, log=False,
                     params=None, just_validate=False,
                     expected_response=None):
        r = self.request(method, endpoint, data=data, json=json_data,
                         headers=headers, params=params)

        if params is None:
            params = []

        # Determine the expected error code and whether we need to validate that
        # the request was performed
        if method == 'POST':
            if code is None:
                if errors:
                    code = 400
                elif endpoint == BUCKETS_ENDPOINT:
                    if just_validate:
                        code = 200
                    else:
                        code = 202
                elif BUCKETS_ENDPOINT in endpoint:
                    if just_validate:
                        code = 202
                    else:
                        code = 200
                else:
                    code = 200
            validate = True
        else:
            code = 200
            errors = {}
            validate = False

        if expected_response is not None:
            # We will compare the text directly, for the case where the response
            # may not be json
            errors = expected_response
            actual_errors = r.text
        else:
            try:
                actual_errors = r.json().get('errors', {})
            except Exception:
                actual_errors = {}

        if r.status_code != code or (errors and actual_errors != errors):
            log = True

        expected_good = errors is None or len(errors) == 0

        # Determine whether the response is as expected
        if code == 400:
            if expected_good:
                log = True
        else:
            if not expected_good:
                log = True

        # If a change should have been made, check it was correctly made
        if not actual_errors and validate and not just_validate:
            validated = self.validate_settings(endpoint, data)
            if not validated:
                log = True
        else:
            validated = True

        if log:
            if endpoint == BUCKETS_ENDPOINT:
                name = data['name']
            elif BUCKETS_ENDPOINT in endpoint:
                name = endpoint.split('/')[-1]
            print(f"--------- Request test ---------\n"
                  f"Method: {method}\n"
                  f"Endpoint: {endpoint}\n"
                  f"Params: {params}\n"
                  f"Expected code: {code}\n"
                  f"Expected errors: {errors}\n"
                  f"Actual code: {r.status_code}\n"
                  f"Actual response: {r.text}\n"
                  f"Actual errors: {actual_errors}\n"
                  f"Test data: {data}")
        if r.text == '{"_":"Bucket with given name still exists"}':
            return r

        assert r.status_code == code,\
            "Unexpected status code"
        if errors:
            if actual_errors:
                assert actual_errors == errors, \
                    "Different errors were found to those expected"
            else:
                assert actual_errors == errors, \
                    "Expected errors missing"
        assert validated, \
            "Request apparently succeeded but does not appear to have been " \
            "carried out"

        return r

    def test_get(self, endpoint, **kwargs):
        return self.test_request('GET', endpoint, **kwargs)

    def test_post(self, endpoint, **kwargs):
        return self.test_request('POST', endpoint, **kwargs)

    def test_delete(self, endpoint, **kwargs):
        return self.test_request('DELETE', endpoint, **kwargs)

    def validate_settings(self, endpoint, data):
        if endpoint == BUCKETS_ENDPOINT:
            name = data['name']

            response = self.request('GET', endpoint + "/" + name)
            try:
                info = response.json()
                return was_change_made(info, data)
            except Exception as e:
                print(e)
                traceback.print_exc()
                raise Exception("Failed to get bucket details after create")
        elif BUCKETS_ENDPOINT in endpoint:
            response = self.request('GET', endpoint)
            try:
                info = response.json()
                return was_change_made(info, data)
            except Exception as e:
                print(e)
                traceback.print_exc()
                raise Exception("Failed to get bucket details after update")
        elif endpoint == SET_AUTO_COMPACTION_ENDPOINT:
            response = self.request('GET', GET_AUTO_COMPACTION_ENDPOINT)
            try:
                info = response.json()
                return was_auto_compaction_change_made(info, data)
            except Exception as e:
                print(e)
                traceback.print_exc()
                raise Exception("Failed to get auto compaction settings")
        else:
            print("Invalid endpoint for validate_settings", endpoint)
            return False

    def name(self):
        if hasattr(self, '__name__'):
            return self.__name__
        else:
            return type(self).__name__

    def setup(self):
        self.num_nodes = len(self.cluster.connected_nodes)
        self.memsize = self.cluster.memory_quota()
        self.is_enterprise = self.cluster.is_enterprise
        self.is_76 = self.cluster.is_76
        self.is_serverless = self.cluster.is_serverless
        self.is_dev_preview = self.cluster.is_dev_preview
        self.good_symbols = string.ascii_letters + string.digits + "._-%"

        # Deleting existing buckets to make space
        self.test_teardown()

    # TODO: Handle limits differently for greater ease of adding tests
    def init_limits(self, bucket_type, storage_backend, is_creation,
                    param=None):
        self.limits = collections.defaultdict(collections.defaultdict)

        # Ram Quota
        # ----------------------------------------------------------------------
        self.limits['ramQuota']['max'] = self.memsize
        if storage_backend == "magma":
            self.limits['ramQuota']['min'] = 1024
        elif bucket_type == "memcached":
            self.limits['ramQuota']['min'] = 64
        else:
            self.limits['ramQuota']['min'] = 100

        # Bucket rank
        # ----------------------------------------------------------------------
        self.limits['rank']['min'] = 0
        self.limits['rank']['max'] = 1000

        # Threads Number
        # ----------------------------------------------------------------------
        self.limits['threadsNumber']['min'] = 2
        self.limits['threadsNumber']['max'] = 8

        # Replica Number
        # ----------------------------------------------------------------------
        self.limits['replicaNumber']['min'] = 0
        if param == "durabilityMinLevel":
            self.limits['replicaNumber']['max'] = 2
        else:
            self.limits['replicaNumber']['max'] = 3

        # Max TTL
        # ----------------------------------------------------------------------
        self.limits['maxTTL']['min'] = 0
        self.limits['maxTTL']['max'] = 2147483647

        # Replica Index
        # ----------------------------------------------------------------------
        self.limits['replicaIndex']['min'] = 0
        self.limits['replicaIndex']['max'] = 1

        # Flush Enabled
        # ----------------------------------------------------------------------
        self.limits['flushEnabled']['min'] = 0
        self.limits['flushEnabled']['max'] = 1

        # Magma Max Shards
        # ----------------------------------------------------------------------
        self.limits['magmaMaxShards']['min'] = 1
        self.limits['magmaMaxShards']['max'] = 128

        # Pitr Granularity
        # ----------------------------------------------------------------------
        self.limits['pitrGranularity']['min'] = 1
        self.limits['pitrGranularity']['max'] = 18000

        # Pitr Max History Age
        # ----------------------------------------------------------------------
        self.limits['pitrMaxHistoryAge']['min'] = 1
        self.limits['pitrMaxHistoryAge']['max'] = 172800

        # Drift Ahead Threshold ms
        # ----------------------------------------------------------------------
        self.limits['driftAheadThresholdMs']['min'] = 100

        # Drift Behind Threshold ms
        # ----------------------------------------------------------------------
        self.limits['driftBehindThresholdMs']['min'] = 100

        # Storage Quota Percentage
        # ----------------------------------------------------------------------
        self.limits['storageQuotaPercentage']['min'] = 1
        self.limits['storageQuotaPercentage']['max'] = 85

        # Width
        # ----------------------------------------------------------------------
        self.limits['width']['min'] = 1

        # Weight
        # ----------------------------------------------------------------------
        self.limits['weight']['min'] = 0

        # Num VBuckets
        # ----------------------------------------------------------------------
        self.limits['numVBuckets']['min'] = 16
        self.limits['numVBuckets']['max'] = 1024

        # Database Fragmentation Threshold [percentage]
        # ----------------------------------------------------------------------
        self.limits['databaseFragmentationThreshold[percentage]']['min'] = 2
        self.limits['databaseFragmentationThreshold[percentage]']['max'] = 100

        # Database Fragmentation Threshold [size]
        # ----------------------------------------------------------------------
        self.limits['databaseFragmentationThreshold[size]']['min'] = 1

        # View Fragmentation Threshold [percentage]
        # ----------------------------------------------------------------------
        self.limits['viewFragmentationThreshold[percentage]']['min'] = 2
        self.limits['viewFragmentationThreshold[percentage]']['max'] = 100

        # View Fragmentation Threshold [size]
        # ----------------------------------------------------------------------
        self.limits['viewFragmentationThreshold[size]']['min'] = 1

        # Purge Interval
        # ----------------------------------------------------------------------
        if bucket_type == "ephemeral":
            self.limits['purgeInterval']['min'] = 0.0007
        elif self.is_76:
            self.limits['purgeInterval']['min'] = 0.01
        else:
            self.limits['purgeInterval']['min'] = 0.04

        self.limits['purgeInterval']['max'] = 60

        # Allowed Time Period [fromHour]
        # ----------------------------------------------------------------------

        self.limits['allowedTimePeriod[fromHour]']['min'] = 0
        self.limits['allowedTimePeriod[fromHour]']['max'] = 23

        # Allowed Time Period [fromMinute]
        # ----------------------------------------------------------------------

        self.limits['allowedTimePeriod[fromMinute]']['min'] = 0
        self.limits['allowedTimePeriod[fromMinute]']['max'] = 59

        # Allowed Time Period [toHour]
        # ----------------------------------------------------------------------

        self.limits['allowedTimePeriod[toHour]']['min'] = 0
        self.limits['allowedTimePeriod[toHour]']['max'] = 23

        # Allowed Time Period [toMinute]
        # ----------------------------------------------------------------------

        self.limits['allowedTimePeriod[toMinute]']['min'] = 0
        self.limits['allowedTimePeriod[toMinute]']['max'] = 59

        # Magma Fragmentation Percentage
        # ----------------------------------------------------------------------
        self.limits['magmaFragmentationPercentage']['min'] = 10
        self.limits['magmaFragmentationPercentage']['max'] = 100

    def add_limits(self, field):
        self.test_params[field].append(self.limits[field]['min'])
        if 'max' in self.limits[field]:
            self.test_params[field].append(self.limits[field]['max'])

    def add_limits_bad(self, field, params=None):
        if params is None:
            params = self.bad_params
        params[field].append(self.limits[field]['min'] - 1)
        if 'max' in self.limits[field]:
            params[field].append(self.limits[field]['max'] + 1)

    def small_enough(self, field, value):
        return value <= self.limits[field]['max']

    def large_enough(self, field, value):
        return value >= self.limits[field]['min']

    def within_limits(self, field, value):
        return self.small_enough(field, value) and \
               self.large_enough(field, value)

    def outside_limits(self, field, value):
        return value < self.limits[field]['min'] or \
               value > self.limits[field]['max']

    """
    Populate test_params with valid values for all supported parameters.
    None indicates that the parameter may be omitted for bucket create.
    is_creation specifies whether the test is for a new bucket (create) or an 
    old bucket (update). All parameters may be omitted for bucket update.
    """

    def init_test_params(self, param, bucket_type, storage_backend,
                         auto_compaction_defined, bucket_placer,
                         allowed_time_period, conflict_resolution_type,
                         just_validate, is_creation, rank):
        self.test_params = collections.defaultdict(list)

        # Bucket Name
        # ----------------------------------------------------------------------

        if is_creation:
            self.test_params['name'] = [
                # Covers all valid characters
                self.good_symbols,
                # Max length name
                "a" * 100]

        # Bucket Type
        # ----------------------------------------------------------------------
        if is_creation:
            self.test_params['bucketType'] = [bucket_type]

        # Ram Quota
        # ----------------------------------------------------------------------
        self.test_params['ramQuota'] = []
        if param == 'ramQuota':
            if is_creation or bucket_type != "memcached":
                self.add_limits('ramQuota')
            self.test_params['ramQuotaMB'] = self.test_params['ramQuota']
        else:
            self.test_params['ramQuota'] = [self.limits['ramQuota']['max']]

        # Flush Enabled
        # ----------------------------------------------------------------------

        self.test_params['flushEnabled'] = [None]
        self.add_limits('flushEnabled')

        if bucket_type != "memcached":

            # Replica Number
            # ------------------------------------------------------------------

            self.test_params['replicaNumber'] = [None]

            # A warning is given as an error when just validating an update of
            # replicaNumber, so we don't test this case
            if self.num_nodes > 1 and (not just_validate or is_creation):
                self.add_limits('replicaNumber')

            # Storage Backend
            # ------------------------------------------------------------------

            self.test_params['storageBackend'] = [storage_backend]

            # Eviction Policy
            # ------------------------------------------------------------------

            self.test_params['evictionPolicy'] = [None]
            if bucket_type == "ephemeral":
                if is_creation:
                    self.test_params['evictionPolicy'] += ["noEviction",
                                                           "nruEviction"]
            elif bucket_type != "memcached":
                self.test_params['evictionPolicy'] += ["valueOnly",
                                                       "fullEviction"]

            # Durability Minimum Level
            # ------------------------------------------------------------------

            self.test_params['durabilityMinLevel'] = [None, "none"]

            # Durability minimum level cannot be specified with 3 replicas,
            # so to avoid that error, we don't test both at the same time
            if self.num_nodes > 1 and param != "replicaNumber":
                self.test_params['durabilityMinLevel'] += ["majority"]
                if bucket_type != "ephemeral":
                    self.test_params['durabilityMinLevel'] += [
                        "majorityAndPersistActive",
                        "persistToMajority"]

            # Threads Number
            # ------------------------------------------------------------------

            self.test_params['threadsNumber'] = [None]
            self.add_limits('threadsNumber')

            # Rank
            # ------------------------------------------------------------------
            self.test_params['rank'] = [rank]
            self.add_limits('rank')

            # Compression Mode
            # ------------------------------------------------------------------

            self.test_params['compressionMode'] = [None]
            if self.is_enterprise:
                self.test_params['compressionMode'] += ["off", "passive",
                                                        "active"]

            # Max TTL
            # ------------------------------------------------------------------

            self.test_params['maxTTL'] = [None]
            if self.is_enterprise:
                self.add_limits('maxTTL')

            # Replica Index
            # ------------------------------------------------------------------

            self.test_params['replicaIndex'] = [None]
            if is_creation and bucket_type != "ephemeral" \
                    and storage_backend != "magma" \
                    and not self.is_serverless:
                self.add_limits('replicaIndex')

            # Conflict Resolution Type
            # ------------------------------------------------------------------

            if is_creation:
                self.test_params['conflictResolutionType'] = [
                    conflict_resolution_type]

            # Magma Max Shards
            # ------------------------------------------------------------------

            self.test_params['magmaMaxShards'] = [None]
            if self.is_serverless and is_creation and self.is_76 \
                    and storage_backend == "magma":
                self.add_limits('magmaMaxShards')

            # Pitr Enabled
            # ------------------------------------------------------------------

            self.test_params['pitrEnabled'] = [None]
            if self.is_enterprise and self.is_76 \
                    and bucket_type != "memcached":
                self.test_params['pitrEnabled'] += ["true", "false"]

            if self.is_76 and self.is_enterprise:
                # Pitr Granularity and Pitr Max History Age
                # --------------------------------------------------------------

                self.test_params[PITR_PARAMS] = [None]
                if param == PITR_PARAMS:
                    granu_min = self.limits['pitrGranularity']['min']
                    granu_max = self.limits['pitrGranularity']['max']
                    mha_min = self.limits['pitrMaxHistoryAge']['min']
                    mha_max = self.limits['pitrMaxHistoryAge']['max']
                    self.test_params[PITR_PARAMS] += [
                        (granu_min, mha_min),
                        (granu_max, mha_max),
                        (granu_max, granu_max)
                    ]

            if conflict_resolution_type == "lww":
                # Drift Ahead Threshold ms
                # --------------------------------------------------------------

                self.test_params['driftAheadThresholdMs'] = [None]
                self.add_limits('driftAheadThresholdMs')

                # Drift Behind Threshold ms
                # --------------------------------------------------------------

                self.test_params['driftBehindThresholdMs'] = [None]
                self.add_limits('driftBehindThresholdMs')

            # Storage Quota Percentage
            # ------------------------------------------------------------------

            self.test_params['storageQuotaPercentage'] = [None]
            if self.is_enterprise and storage_backend == "magma":
                self.add_limits('storageQuotaPercentage')

            # Width
            # ------------------------------------------------------------------

            if not bucket_placer:
                self.test_params['width'] = [None]
            if self.is_serverless and is_creation and bucket_placer:
                self.add_limits('width')

            # Weight
            # ------------------------------------------------------------------
            if not bucket_placer:
                self.test_params['weight'] = [None]
            if self.is_serverless and is_creation and bucket_placer:
                self.add_limits('weight')

            # Num VBuckets
            # ------------------------------------------------------------------

            self.test_params['numVBuckets'] = [None]
            if self.is_serverless and is_creation:
                self.add_limits('numVBuckets')

            # Auto Compaction Defined
            # ------------------------------------------------------------------

            self.test_params['autoCompactionDefined'] = [
                auto_compaction_defined]

            if auto_compaction_defined == "true" and storage_backend != "magma":
                self.test_params['parallelDBAndViewCompaction'] = ["true",
                                                                   "false"]

            self.test_params['databaseFragmentationThreshold[percentage]'] = [
                None]
            self.test_params['databaseFragmentationThreshold[size]'] = [None]
            self.test_params['viewFragmentationThreshold[percentage]'] = [None]
            self.test_params['viewFragmentationThreshold[size]'] = [None]

            if not allowed_time_period:
                self.test_params[ALLOWED_TIME_PERIOD_PARAMS] = [None]

            # Not magma and autoCompactionDefined
            if storage_backend != "magma" and auto_compaction_defined == "true":
                # Only valid values allowed
                self.add_limits('databaseFragmentationThreshold[percentage]')

                self.add_limits('databaseFragmentationThreshold[size]')

                self.add_limits('viewFragmentationThreshold[percentage]')

                self.add_limits('viewFragmentationThreshold[size]')

                if allowed_time_period:
                    # Generate legal sets of values, rather than ignoring tests
                    # with start time equals end time
                    self.test_params[ALLOWED_TIME_PERIOD_PARAMS] = [
                        (0, 0, 23, 59, "true"),
                        (23, 59, 0, 0, "false")
                    ]

            # Purge Interval
            # ------------------------------------------------------------------

            self.test_params['purgeInterval'] = [None]
            if auto_compaction_defined == "true" or bucket_type == "ephemeral":
                self.add_limits('purgeInterval')

            # Magma Fragmentation Percentage
            # ------------------------------------------------------------------

            self.test_params['magmaFragmentationPercentage'] = [None]
            if auto_compaction_defined == "true" and \
                    storage_backend == "magma":
                self.add_limits('magmaFragmentationPercentage')

        controlled_by_main_dict = [
            'bucketType',
            'rank',
            'storageBackend',
            'parallelDBAndViewCompaction',
            'autoCompactionDefined',
            'width',
            'weight',
            'allowedTimePeriod[fromHour]',
            'allowedTimePeriod[fromMinute]',
            'allowedTimePeriod[toHour]',
            'allowedTimePeriod[toMinute]',
            'allowedTimePeriod[abortOutside]',
            'conflictResolutionType'
        ]

        # Make all parameters optional if updating
        if not is_creation:
            for key in self.test_params:
                if None not in self.test_params[key] and \
                        key not in controlled_by_main_dict:
                    self.test_params[key].append(None)

    def init_bad_params(self, bucket_type, storage_backend,
                        auto_compaction_defined, bucket_placer,
                        allowed_time_period, conflict_resolution_type,
                        just_validate, is_creation, rank):
        self.bad_params = collections.defaultdict(list)

        # Bucket Name
        # ----------------------------------------------------------------------

        if is_creation:
            self.bad_params['name'] = ["", ".", ".test",
                                       "_users.couch.", "_replicator.couch.",
                                       # Reserved
                                       "a" * 101]
            # Common bad characters, only every 4th to save time
            self.bad_params['name'] += [char for char in
                                        string.punctuation + string.whitespace
                                        if char not in self.good_symbols][::4]
            all_unicode_chars = [chr(uc) for uc in range(0xFF + 1)]
            all_bad_chars = [char for char in all_unicode_chars
                             if char not in self.good_symbols]
            # Only sample 10 of rare bad characters
            self.bad_params['name'] += random.sample(all_bad_chars, 10)

            if is_creation:
                # name is required for bucket create
                self.bad_params['name'].append(None)

        # Bucket Type
        # ----------------------------------------------------------------------
        if is_creation:
            if self.is_serverless:
                self.bad_params['bucketType'].append("memcached")
            self.bad_params['bucketType'].append("bogus")
        elif bucket_type == "ephemeral":
            self.bad_params['bucketType'] = ["couchbase"]
        else:
            self.bad_params['bucketType'] = ["ephemeral"]

        # Ram Quota
        # ----------------------------------------------------------------------
        if bucket_placer or self.is_serverless:
            # Serverless ram quota validation is a bit more complicated so for
            # now we don't comprehensively test this
            self.bad_params['ramQuota'] = [self.limits['ramQuota']['min'] - 1]
        else:
            if not is_creation:
                if bucket_type == "memcached":
                    # -1 is added to avoid setting ramQuota to current value
                    self.bad_params['ramQuota'] = \
                        [self.limits['ramQuota']['max'] - 1]
                else:
                    self.add_limits_bad('ramQuota')

        # Flush Enabled
        # ----------------------------------------------------------------------
        self.add_limits_bad('flushEnabled')

        # Threads Number
        # ----------------------------------------------------------------------
        self.add_limits_bad('threadsNumber')
        self.bad_params['threadsNumber'].append("bogus")

        # Replica Number
        # ----------------------------------------------------------------------
        if bucket_type == "memcached":
            self.bad_params['replicaNumber'] = [2]
        self.add_limits_bad('replicaNumber')
        self.bad_params['replicaNumber'].append("bogus")

        # All further params are ignored for memcached
        # ----------------------------------------------------------------------
        if bucket_type == "memcached":
            return
        # ----------------------------------------------------------------------

        # Storage Backend
        # ----------------------------------------------------------------------
        if bucket_type != "ephemeral" and is_creation:
            if not self.is_enterprise:
                self.bad_params['storageBackend'].append("magma")
            self.bad_params['storageBackend'].append("bogus")

        # Eviction Policy
        # ----------------------------------------------------------------------
        if is_creation or bucket_type != "ephemeral":
            if bucket_type not in [None, "membase", "couchbase"]:
                self.bad_params['evictionPolicy'] = ["valueOnly",
                                                     "fullEviction"]
            if bucket_type != "ephemeral":
                self.bad_params['evictionPolicy'] += ["noEviction",
                                                      "nruEviction"]
            self.bad_params['evictionPolicy'].append("bogus")

        # Durability Minimum Level
        # ----------------------------------------------------------------------
        if bucket_type == "ephemeral":
            self.bad_params['durabilityMinLevel'] += [
                "majorityAndPersistActive",
                "persistToMajority"]
        self.bad_params['durabilityMinLevel'].append("bogus")

        # Compression Mode
        # ----------------------------------------------------------------------
        if not self.is_enterprise:
            self.bad_params['compressionMode'] = ["off", "passive", "active"]
        self.bad_params['compressionMode'].append("bogus")

        # Max TTL
        # ----------------------------------------------------------------------
        if not self.is_enterprise:
            self.bad_params['maxTTL'] = [0]
        self.add_limits_bad('maxTTL')
        self.bad_params['maxTTL'].append("bogus")

        # Replica Index
        # ----------------------------------------------------------------------
        if is_creation:
            self.add_limits_bad('replicaIndex')
            self.bad_params['replicaIndex'].append("bogus")

        # Conflict Resolution Type
        # ----------------------------------------------------------------------
        if not is_creation:
            self.bad_params['conflictResolutionType'] = ["seqno"]
        if not self.is_enterprise:
            self.bad_params['conflictResolutionType'] += ["lww"]
        if not self.is_dev_preview:
            self.bad_params['conflictResolutionType'] += ["custom"]
        self.bad_params['conflictResolutionType'].append("bogus")

        # Magma Max Shards
        # ----------------------------------------------------------------------
        self.add_limits_bad('magmaMaxShards')
        if not (self.is_enterprise and self.is_76 and
                storage_backend == 'magma'):
            self.bad_params['magmaMaxShards'] = [1]
        else:
            self.add_limits_bad('magmaMaxShards')

        if self.is_enterprise:
            # Pitr Enabled
            # ------------------------------------------------------------------
            self.bad_params['pitrEnabled'] = ["bogus"]

            # Pitr Granularity and Pitr Max History Age
            # ------------------------------------------------------------------
            granu_min = self.limits['pitrGranularity']['min']
            granu_max = self.limits['pitrGranularity']['max']
            mha_min = self.limits['pitrMaxHistoryAge']['min']
            mha_max = self.limits['pitrMaxHistoryAge']['max']
            self.bad_params['pitr'] = [
                (granu_min - 1, mha_min),
                (granu_min, mha_min - 1),
                (granu_max + 1, mha_min),
                (granu_min, mha_max + 1),
                (granu_max, granu_max - 1),
                ("bogus", mha_max),
                (granu_min, "bogus")
            ]
        else:
            # Pitr Enabled
            # ------------------------------------------------------------------
            self.bad_params['pitrEnabled'] = ["true"]

            # Pitr Granularity
            # ------------------------------------------------------------------
            self.bad_params['pitrGranularity'] = [1]

            # Pitr Max History Age
            # ------------------------------------------------------------------
            self.bad_params['pitrMaxHistoryAge'] = [2]

        if conflict_resolution_type == "lww":
            # Drift Ahead Threshold ms
            # ------------------------------------------------------------------
            self.add_limits_bad('driftAheadThresholdMs')

            # Drift Behind Threshold ms
            # ------------------------------------------------------------------
            self.add_limits_bad('driftBehindThresholdMs')

        # Storage Quota Percentage
        # ----------------------------------------------------------------------
        self.add_limits_bad('storageQuotaPercentage')

        # Width
        # ----------------------------------------------------------------------
        self.add_limits_bad('width')

        # Weight
        # ----------------------------------------------------------------------
        self.add_limits_bad('weight')

        # Num VBuckets
        # ----------------------------------------------------------------------
        self.add_limits_bad('numVBuckets')

        # Auto Compaction Defined
        # ----------------------------------------------------------------------

        self.bad_params['autoCompactionDefined'] = ["bogus"]
        if auto_compaction_defined == "true":
            # Purge Interval
            # ------------------------------------------------------------------
            self.add_limits_bad('purgeInterval')
            if storage_backend == "magma":
                # Magma Fragmentation Percentage
                # --------------------------------------------------------------
                self.add_limits_bad('magmaFragmentationPercentage')
            else:
                # Parallel DB And View Compaction
                # --------------------------------------------------------------
                self.bad_params['parallelDBAndViewCompaction'] = ["bogus"]

                # Database Fragmentation Threshold [percentage]
                # --------------------------------------------------------------
                self.add_limits_bad(
                    'databaseFragmentationThreshold[percentage]')

                # Database Fragmentation Threshold [size]
                # --------------------------------------------------------------
                self.add_limits_bad('databaseFragmentationThreshold[size]')

                # View Fragmentation Threshold [percentage]
                # --------------------------------------------------------------
                self.add_limits_bad('viewFragmentationThreshold[percentage]')

                # View Fragmentation Threshold [size]
                # --------------------------------------------------------------
                self.add_limits_bad('viewFragmentationThreshold[size]')

                # Allowed Time Period
                # --------------------------------------------------------------

                self.bad_params[ALLOWED_TIME_PERIOD_PARAMS] = [
                    (-1, 0, 0, 0, "true"),
                    (24, 0, 0, 0, "true"),
                    (0, -1, 0, 0, "true"),
                    (0, 60, 0, 0, "true"),
                    (0, 0, -1, 0, "true"),
                    (0, 0, 24, 0, "true"),
                    (0, 0, 0, -1, "true"),
                    (0, 0, 0, 60, "true"),
                    (0, 0, 0, 0, "true"),
                    (0, 0, 0, 1, "bogus"),
                ]

    def teardown(self):
        pass

    def test_teardown(self):
        # Remove all buckets between tests, to ensure there is space for new
        # buckets to be created
        buckets = self.test_get(BUCKETS_ENDPOINT)
        for bucket in buckets.json():
            name = bucket['name']
            self.cluster.delete_bucket(name=name)

    def get_next_name(self):
        name = f"test_{self.next_bucket_id}"
        self.next_bucket_id += 1
        return name

    def add_required_fields(self, request_data):
        data = {
            "name": self.get_next_name(),
            "ramQuota": 256
        }
        for key, value in request_data.items():
            data[key] = value
        return data

    def name_error(self, test_data):
        field = "name"
        if field in test_data:
            name = test_data[field]
            if name == "":
                return {field: "Bucket name cannot be empty"}
            elif name[0] == ".":
                return {field: "Bucket name cannot start with dot."}
            elif any(char not in self.good_symbols for char in name):
                return {field: "Bucket name can only contain characters in range"
                           " A-Z, a-z, 0-9 as well as underscore, period,"
                           " dash & percent. Consult the documentation."}
            elif name.lower() in ["_users.couch.", "_replicator.couch."]:
                return {field: "This name is reserved for the internal use."}
            elif len(name) > 100:
                return {field: "Bucket name cannot exceed 100 characters"}
            else:
                return {}
        else:
            return {field: "Bucket name needs to be specified"}

    def rank_error(self, test_data):
        field = "rank"
        if field in test_data:
            rank = test_data[field]
            if rank <= 0 and rank >= 1000:
                return {field: f"Rank must be in the range 0-1000. Got '\"{rank}\"' instead."}
        return {}

    def bucket_type_error(self, test_data):
        field = "bucketType"
        if field in test_data:
            bucket_type = test_data[field]
            if bucket_type not in ["couchbase", "membase", "ephemeral"]:
                if self.is_serverless or bucket_type != "memcached":
                    return {field: "invalid bucket type"}

        return {}

    def ram_quota_error(self, test_data, original_data, is_creation,
                        just_validate):
        field1 = "ramQuota"
        field2 = "ramQuotaMB"
        if field1 in test_data:
            quota = test_data[field1]
        elif field2 in test_data:
            quota = test_data[field2]
        else:
            return {}

        storage_backend = test_data.get("storageBackend", "couchstore")
        bucket_type = test_data.get("bucketType", "couchbase")

        if not isinstance(quota, int):
            return {field1: "The RAM quota number must be specified and must be "
                            "a non-negative integer."}
        elif quota < 100 and bucket_type != "memcached":
            return {field1: "RAM quota cannot be less than 100 MiB"}
        elif quota < 64 and bucket_type == "memcached":
            return {field1: "RAM quota cannot be less than 64 MiB"}
        elif not is_creation and bucket_type == "memcached":
            return {field1: "cannot change quota of memcached buckets"}
        elif quota > self.memsize:
            if not is_creation and just_validate and bucket_type == "memcached":
                return {field1: "cannot change quota of memcached buckets"}
            elif "width" in test_data:
                if just_validate:
                    return {}
                else:
                    return {"_top_level_error":
                                '{"_":"Need more space in availability zones '
                                '[<<\"Group 1\">>]."}'}
            else:
                return {field1: "RAM quota specified is too large to be "
                                "provisioned into this cluster."}
        elif storage_backend == "magma" and quota < 1024:
            return {field1: "Ram quota for magma must be at least 1024 MiB"}
        elif not self.is_76 and original_data is not None \
                and original_data['ramQuota'] > quota:
            return {field1: "RAM quota cannot be set below current usage."}
        else:
            return {}

    def storage_backend_error(self, test_data):
        field = "storageBackend"
        if field in test_data:
            if test_data[field] not in ["couchstore", "magma"]:
                return {"storage_mode":
                            "storage backend must be couchstore or magma"}
            if test_data[field] == "magma":
                if not self.is_enterprise:
                    return {field: "Magma is supported in enterprise edition "
                                   "only"}
        return {}

    def eviction_policy_error(self, test_data):
        field1 = "evictionPolicy"
        if field1 in test_data:
            policy = test_data[field1]

            field2 = "bucketType"
            if field2 in test_data:
                bucket_type = test_data[field2]
            else:
                bucket_type = self.cur_main_dict['bucket_type']
            if bucket_type in ["couchbase", "membase"] and \
                    policy not in ["valueOnly", "fullEviction"]:
                return {field1: "Eviction policy must be either "
                                "'valueOnly' or 'fullEviction' "
                                "for couchbase buckets"}
            elif bucket_type == "ephemeral" and \
                    policy not in ["noEviction", "nruEviction"]:
                return {field1: "Eviction policy must be either 'noEviction' "
                                "or 'nruEviction' for ephemeral buckets"}
        return {}

    def dura_min_level_error(self, test_data, replica_number, just_validate):
        field1 = "durabilityMinLevel"
        if field1 in test_data:
            level = test_data[field1]

            field2 = "bucketType"
            if field2 in test_data:
                bucket_type = test_data[field2]
            else:
                bucket_type = "couchbase"
            if bucket_type == "ephemeral" and level not in ["none", "majority"]:
                return {"durability_min_level":
                            "Durability minimum level must be either 'none' "
                            "or 'majority' for ephemeral buckets"}
            elif bucket_type in ["couchbase", "membase"] and \
                    level not in ["none", "majority",
                                  "majorityAndPersistActive",
                                  "persistToMajority"]:
                return {"durability_min_level":
                            "Durability minimum level must be one of 'none', "
                            "'majority', 'majorityAndPersistActive', or "
                            "'persistToMajority'"}
            elif level != "none" and replica_number == 3:
                return {"durability_min_level":
                            "Durability minimum level cannot be specified with "
                            "3 replicas"}
            elif level != "none" and self.num_nodes == 1 and not just_validate:
                return {"durability_min_level":
                            "You do not have enough data servers to support "
                            "this durability level"}
        return {}

    def threads_number_error(self, test_data):
        field = "threadsNumber"
        if field in test_data:
            if not isinstance(test_data[field], int):
                return {field: "The number of threads must be an integer "
                               "between 2 and 8, inclusive"}
            elif test_data[field] < 2:
                return {field: "The number of threads can't be less than 2"}
            elif test_data[field] > 8:
                return {field: "The number of threads can't be greater than 8"}
        return {}

    def replica_number_error(self, test_data, is_creation, just_validate):
        field = "replicaNumber"
        if field in test_data:
            num = test_data[field]
            if test_data.get('bucketType') == "memcached":
                return {field:
                            "replicaNumber is not valid for memcached buckets"}
            elif not isinstance(num, int):
                return {field:
                            "The replica number must be specified and must be "
                            "a non-negative integer."}
            elif not self.small_enough(field, num):
                return {field: "Replica number larger than 3 is not supported."}
            elif not self.large_enough(field, num):
                return {field: f"Replica number must be equal to or greater "
                        f"than {self.limits[field]['min']}"}
            elif num > self.num_nodes - 1 and just_validate:
                return {field: "Warning: you do not have enough data servers "
                               "or server groups to support this number of "
                               "replicas."}
        return {}

    def compression_mode_error(self, test_data):
        field = "compressionMode"
        if field in test_data:
            if not self.is_enterprise:
                return {field: "Compression mode is supported in enterprise "
                               "edition only"}
            if test_data[field] not in ["off", "passive", "active"]:
                return {field: "compressionMode can be set to 'off', 'passive' "
                               "or 'active'"}
        return {}

    def max_ttl_error(self, test_data):
        field = "maxTTL"
        if field in test_data:
            if not self.is_enterprise:
                return {field:
                            "Max TTL is supported in enterprise edition only"}
            max_ttl = test_data[field]
            if not isinstance(max_ttl,
                              int) or max_ttl < 0 or max_ttl > 2147483647:
                return {field: "Max TTL must be an integer between 0 and "
                               "2147483647"}
        return {}

    def replica_index_error(self, test_data):
        field = "replicaIndex"
        if field in test_data:
            if test_data.get("bucketType") == "ephemeral":
                return {field:
                            "replicaIndex not supported for ephemeral buckets"}
            elif test_data[field] not in [0, 1]:
                return {field: "replicaIndex can only be 1 or 0"}
        return {}

    def conflict_resolution_type_error(self, test_data, is_creation):
        field = "conflictResolutionType"
        if field in test_data:
            if not is_creation:
                return {field: "Conflict resolution type not allowed in update "
                               "bucket"}
            elif test_data[field] not in ["seqno", "lww", "custom"]:
                return {field: "Conflict resolution type must be 'seqno' or "
                               "'lww' or 'custom'"}
            elif test_data[field] == "lww" and not self.is_enterprise:
                return {field: "Conflict resolution type 'lww' is supported"
                               " only in enterprise edition"}
            elif test_data[field] == "custom" and not self.is_enterprise:
                return {field: "Conflict resolution type 'custom' is supported"
                               " only in enterprise edition"}
            elif test_data[field] == "custom" and not self.is_dev_preview:
                return {field: "Conflict resolution type 'custom' is supported"
                               " only with developer preview enabled"}
        return {}

    def flush_enabled_error(self, test_data):
        field = "flushEnabled"
        if field in test_data:
            if test_data[field] not in [0, 1]:
                return {field: "flushEnabled can only be 1 or 0"}
        return {}

    def magma_max_shards_error(self, test_data, is_creation):
        field = "magmaMaxShards"
        if field in test_data:
            if not is_creation:
                return {field: "Number of maximum magma shards cannot be "
                               "modified after bucket creation"}
            elif test_data.get('storageBackend') != "magma":
                return {field: "Cannot set maximum magma shards on non-magma "
                               "storage backend"}
            elif not self.is_76:
                return {field: "Not allowed until entire cluster is upgraded "
                               "to 7.6"}
            value = test_data[field]
            if self.outside_limits(field, value):
                return {field: "Must be an integer between 1 and 128"}
        return {}

    def pitr_errors(self, test_data, original_data):
        field_enabled = "pitrEnabled"
        field_granu = "pitrGranularity"
        field_max_history_age = "pitrMaxHistoryAge"
        errors = {}
        mha_max = self.limits[field_max_history_age]['max']
        granu_default = 600
        mha_default = 86400

        if field_enabled in test_data:
            if not self.is_76:
                return {field_enabled:
                            "Point in time recovery is not supported until "
                            "cluster is fully Elixir"}
            elif not self.is_enterprise:
                return {field_enabled:
                            "\"pitrEnabled\" can only be set in Enterprise "
                            "edition"}
            elif test_data[field_enabled] not in ["true", "false"]:
                errors.update(
                    {field_enabled: "pitrEnabled must be true or false"})

        if field_granu in test_data:
            if not self.is_76:
                return {field_granu:
                            "Point in time recovery is not supported until "
                            "cluster is fully Elixir"}
            elif not self.is_enterprise:
                return {field_granu:
                            "\"pitrGranularity\" can only be set in Enterprise "
                            "edition"}
            granu = test_data[field_granu]

            if not isinstance(granu, int):
                errors.update({field_granu:
                                   f"The value of pitrGranularity "
                                   f"({granu}) must be a non-negative integer"})
                if original_data is None:
                    granu = mha_max + 1
                else:
                    granu = granu_default

            elif self.outside_limits('pitrGranularity', granu):
                errors.update({field_granu:
                                   f"The value of pitrGranularity ({granu}) "
                                   f"must be in the range 1 to 18000 "
                                   f"inclusive"})
                if original_data is None:
                    granu = mha_max + 1
                else:
                    granu = granu_default
        else:
            granu = granu_default

        if field_max_history_age in test_data:
            if not self.is_76:
                return {field_max_history_age:
                            "Point in time recovery is not supported until "
                            "cluster is fully Elixir"}
            elif not self.is_enterprise:
                return {field_max_history_age:
                            "\"pitrMaxHistoryAge\" can only be set in "
                            "Enterprise edition"}
            max_history_age = test_data[field_max_history_age]
            if not isinstance(max_history_age, int):
                errors.update({
                    field_max_history_age:
                        f"The value of pitrMaxHistoryAge ({max_history_age}) "
                        f"must be a non-negative integer"})

                if original_data is None:
                    max_history_age = mha_max + 1
                else:
                    max_history_age = mha_default
            elif self.outside_limits('pitrMaxHistoryAge', max_history_age):
                errors.update({field_max_history_age:
                                   f"The value of pitrMaxHistoryAge "
                                   f"({max_history_age}) must be in the"
                                   f" range 1 to 172800 inclusive"})
                if original_data is None:
                    max_history_age = mha_max + 1
                else:
                    max_history_age = mha_default
        else:
            max_history_age = mha_default

        # ns_server actually responds with the following error as well as the
        # outside bounds error with the same key when they both occur, but we
        # rely on consistent discarding of the first error when converted to a
        # python dictionary, rather than looking for both errors in the response
        if granu > max_history_age:
            errors.update({field_granu: "PITR granularity must be less "
                                        "than or equal to max history age"})

        return errors

    def drift_threshold_errors(self, test_data):
        field = "driftAheadThresholdMs"
        errors = {}
        if field in test_data:
            value = test_data[field]

            if not isinstance(value, int):
                errors.update({field: ""})
            elif not self.large_enough(field, value):
                errors.update({field:
                                   "The drift ahead threshold can't be less "
                                   "than 100ms"})
        field = "driftBehindThresholdMs"
        if field in test_data:
            value = test_data[field]

            if not isinstance(value, int):
                errors.update({field: ""})
            elif not self.large_enough(field, value):
                errors.update({field:
                                   "The drift behind threshold can't be less "
                                   "than 100ms"})
        return errors

    def storage_quota_percentage_error(self, test_data):
        field = "storageQuotaPercentage"
        if field in test_data:
            if not self.is_enterprise:
                return {field:
                            "Storage Quota Percentage is supported in "
                            "enterprise edition only"}
            elif test_data.get("storageBackend") != "magma":
                return {field:
                            "Storage Quota Percentage is only used with Magma"}
            value = test_data[field]
            if self.outside_limits(field, value):
                return {field:
                            "Storage Quota Percentage must be between 1 and "
                            "85, inclusive"}
        return {}

    def width_weight_error(self, test_data, original_data):
        field1 = "width"
        field2 = "weight"
        errors = {}
        if field1 in test_data:
            width = test_data[field1]
            min_width = self.limits['width']['min']
            if original_data is not None and field1 not in original_data:
                errors.update({field1: "width cannot be updated since it was "
                                       "not specified during the bucket "
                                       "creation"})
                return errors
            elif not isinstance(width, int):
                errors.update({field1: ""})
            elif width < min_width:
                errors.update({field1: "width must be 1 or more"})
            if field2 not in test_data:
                errors.update({field2: "weight must be specified"})

        if field2 in test_data:
            weight = test_data[field2]
            min_weight = self.limits['weight']['min']

            if original_data is not None and field2 not in original_data:
                errors.update({field2: "weight cannot be updated since it was "
                                       "not specified during the bucket "
                                       "creation"})
                return errors
            elif not isinstance(weight, int):
                errors.update({field2: ""})
            elif weight < min_weight:
                errors.update({field2: "weight must be 0 or more"})
            if field1 not in test_data:
                errors.update({field1: "width must be specified"})
        return errors

    def num_vbuckets_error(self, test_data, is_creation):
        field = "numVBuckets"
        if field in test_data:
            value = test_data[field]
            if not is_creation:
                return {"numVBuckets": "Number of vbuckets cannot be modified"}
            if not isinstance(value, int) or self.outside_limits(field, value):
                return {"numVBuckets": "Number of vbuckets must be an integer "
                                       "between 16 and 1024"}
        return {}

    def auto_compaction_defined_error(self, test_data, errors):
        field1 = "autoCompactionDefined"
        field2 = "bucketType"
        if field1 in test_data:
            if test_data.get(field2) == "ephemeral":
                errors.update({field1: "autoCompactionDefined must not be set "
                                       "for ephemeral buckets"})
            auto_compaction = test_data[field1]
            if auto_compaction == "true":
                return True
            elif auto_compaction == "false":
                return False
            else:
                errors.update({field1: "autoCompactionDefined is invalid"})
                return False
        return False

    def magma_fragmentation_percentage_error(self, test_data):
        field = "magmaFragmentationPercentage"
        if field in test_data:
            value = test_data[field]

            if not isinstance(value, int):
                return {field: "magma fragmentation percentage must be an "
                               "integer. Allowed range is 10 - 100"}
            elif not self.large_enough(field, value):
                return {field: "magma fragmentation percentage is too small. "
                               "Allowed range is 10 - 100"}
            elif not self.small_enough(field, value):
                return {field: "magma fragmentation percentage is too large. "
                               "Allowed range is 10 - 100"}
        return {}

    def parallel_db_and_view_compaction_error(self, test_data):
        field = "parallelDBAndViewCompaction"
        if field in test_data:
            if test_data[field] not in ["true", "false"]:
                return {field: "parallelDBAndViewCompaction is invalid"}
            else:
                return {}
        else:
            return {field: "parallelDBAndViewCompaction is missing"}

    def database_fragmentation_threshold(self, test_data):
        field_root = "databaseFragmentationThreshold"
        field_perc = field_root + "[percentage]"
        field_size = field_root + "[size]"
        errors = {}
        if field_perc in test_data:
            value_perc = test_data[field_perc]
            min_val = self.limits[field_perc]['min']
            max_val = self.limits[field_perc]['max']
            if not isinstance(value_perc, int):
                errors.update({
                    field_perc: f"database fragmentation must be an integer. "
                                f"Allowed range is {min_val} - {max_val}"})
            elif value_perc < min_val:
                errors.update({field_perc:
                                   f"database fragmentation is too small. "
                                   f"Allowed range is {min_val} - {max_val}"})
            elif value_perc > max_val:
                errors.update({field_perc:
                                   f"database fragmentation is too large. "
                                   f"Allowed range is {min_val} - {max_val}"})
        if field_size in test_data:
            value_size = test_data[field_size]
            min_val = self.limits[field_size]['min']
            if not isinstance(value_size, int):
                errors.update({field_size: f"database fragmentation size must "
                                           f"be an integer. Allowed range is "
                                           f"{min_val} - infinity"})
            elif value_size < min_val:
                errors.update({field_size:
                                   f"database fragmentation size is too small. "
                                   f"Allowed range is {min_val} - infinity"})
        return errors

    def view_fragmentation_threshold(self, test_data):
        field_root = "viewFragmentationThreshold"
        field_perc = field_root + "[percentage]"
        field_size = field_root + "[size]"
        errors = {}
        if field_perc in test_data:
            value_perc = test_data[field_perc]
            min_val = self.limits[field_perc]['min']
            max_val = self.limits[field_perc]['max']
            if not isinstance(value_perc, int):
                errors.update({field_perc:
                                   f"view fragmentation must be an integer. "
                                   f"Allowed range is {min_val} - {max_val}"})
            elif value_perc < min_val:
                errors.update({field_perc:
                                   f"view fragmentation is too small. "
                                   f"Allowed range is {min_val} - {max_val}"})
            elif value_perc > max_val:
                errors.update({field_perc:
                                   f"view fragmentation is too large. Allowed "
                                   f"range is {min_val} - {max_val}"})
        if field_size in test_data:
            value_size = test_data[field_size]
            min_val = self.limits[field_size]['min']
            if not isinstance(value_size, int):
                errors.update({
                    field_size: f"view fragmentation size must be an integer. "
                                f"Allowed range is {min_val} - infinity"})
            elif value_size < min_val:
                errors.update({field_size:
                                   f"view fragmentation size is too small. "
                                   f"Allowed range is {min_val} - infinity"})
        return errors

    def purge_interval_error(self, test_data):
        field = "purgeInterval"
        if field in test_data:
            value = test_data[field]
            min_val = self.limits[field]['min']
            max_val = self.limits[field]['max']
            if not isinstance(value, float) and not isinstance(value, int):
                return {field: f"metadata purge interval must be a number. "
                               f"Allowed range is {min_val} - {max_val}"}
            elif value < min_val:
                return {field: f"metadata purge interval is too small. "
                               f"Allowed range is {min_val} - {max_val}"}
            elif value > max_val:
                return {field: f"metadata purge interval is too large. "
                               f"Allowed range is {min_val} - {max_val}"}
        return {}

    def allowed_time_period_error(self, test_data):
        field_root = "allowedTimePeriod"
        field_from_hour = field_root + "[fromHour]"
        field_from_minute = field_root + "[fromMinute]"
        field_to_hour = field_root + "[toHour]"
        field_to_minute = field_root + "[toMinute]"
        field_abort_outside = field_root + "[abortOutside]"

        fields = [field_from_hour, field_from_minute, field_to_hour,
                  field_to_minute, field_abort_outside]
        have = [field in test_data for field in fields]

        errors = {}

        if any(have):
            if not all(have):
                errors.update({
                    field_root:
                        "Must specify all of the following: "
                        "fromHour, fromMinute, toHour, toMinute, abortOutside"})
            field = field_from_hour
            if field in test_data:
                from_hour = test_data[field]
                min_val = self.limits[field]['min']
                max_val = self.limits[field]['max']
                if not isinstance(from_hour, int):
                    errors.update({
                        field_from_hour:
                            f"from hour must be an integer. "
                            f"Allowed range is {min_val} - {max_val}"})
                elif from_hour < min_val:
                    errors.update({
                        field: f"from hour is too small."
                               f" Allowed range is {min_val} - {max_val}"})
                elif from_hour > max_val:
                    errors.update({
                        field: f"from hour is too large. "
                               f"Allowed range is {min_val} - {max_val}"})

            field = field_from_minute
            if field in test_data:
                from_minute = test_data[field]
                min_val = self.limits[field]['min']
                max_val = self.limits[field]['max']

                if not isinstance(from_minute, int):
                    errors.update({
                        field_from_minute:
                            f"from minute must be an integer. "
                            f"Allowed range is {min_val} - {max_val}"})
                elif from_minute < min_val:
                    errors.update({
                        field: f"from minute is too small. "
                               f"Allowed range is {min_val} - {max_val}"})
                elif from_minute > max_val:
                    errors.update({
                        field: f"from minute is too large. "
                               f"Allowed range is {min_val} - {max_val}"})

            field = field_to_hour
            if field in test_data:
                to_hour = test_data[field]
                min_val = self.limits[field]['min']
                max_val = self.limits[field]['max']

                if not isinstance(to_hour, int):
                    errors.update({
                        field_to_hour:
                            f"to hour must be an integer. "
                            f"Allowed range is {min_val} - {max_val}"})
                elif to_hour < min_val:
                    errors.update({
                        field: f"to hour is too small. "
                               f"Allowed range is {min_val} - {max_val}"})
                elif to_hour > max_val:
                    errors.update({
                        field: f"to hour is too large. "
                               f"Allowed range is {min_val} - {max_val}"})

            field = field_to_minute
            if field in test_data:
                to_minute = test_data[field]
                min_val = self.limits[field]['min']
                max_val = self.limits[field]['max']

                if not isinstance(to_minute, int):
                    errors.update({
                        field_to_minute:
                            f"to minute must be an integer. "
                            f"Allowed range is {min_val} - {max_val}"})
                elif to_minute < min_val:
                    errors.update({
                        field: f"to minute is too small. "
                               f"Allowed range is {min_val} - {max_val}"})
                elif to_minute > max_val:
                    errors.update({
                        field: f"to minute is too large. "
                               f"Allowed range is {min_val} - {max_val}"})
            if test_data.get(field_abort_outside, "false") \
                    not in ["true", "false"]:
                errors.update({field_abort_outside:
                                   f"{field_abort_outside} is invalid"})

            if all(have) and \
                    (from_hour, from_minute) == (to_hour, to_minute) and \
                    len(errors) == 0:
                errors.update({
                    field_root: "Start time must not be the same as end time"})

        return errors

    def get_errors(self, endpoint, test_data, original_data, just_validate,
                   is_creation):
        errors = {}
        auto_compaction = False

        if is_creation:
            # Bucket creation specific errors
            errors.update(self.name_error(test_data))
            errors.update(self.bucket_type_error(test_data))
            if test_data.get('bucketType') != "ephemeral":
                errors.update(self.storage_backend_error(test_data))
            errors.update(self.replica_index_error(test_data))

        if BUCKETS_ENDPOINT in endpoint:
            errors.update(self.rank_error(test_data))
            if not is_creation:
                test_data['bucketType'] = self.cur_main_dict['bucket_type']
                test_data['storageBackend'] = self.cur_main_dict['storage_backend']
            if "bucketType" not in errors and "storageBackend" not in errors:
                # Bucket
                errors.update(self.conflict_resolution_type_error(test_data,
                                                                  is_creation))
                errors.update(self.ram_quota_error(test_data, original_data,
                                                   is_creation, just_validate))
                errors.update(self.eviction_policy_error(test_data))
                replica_number = test_data.get('replicaNumber', None)
                if replica_number is None and original_data is not None and \
                        "replicaNumber" in original_data:
                    replica_number = original_data.get('replicaNumber')
                errors.update(self.dura_min_level_error(test_data,
                                                        replica_number,
                                                        just_validate))
                errors.update(self.threads_number_error(test_data))
                errors.update(self.replica_number_error(test_data, is_creation,
                                                        just_validate))
                errors.update(self.compression_mode_error(test_data))
                errors.update(self.max_ttl_error(test_data))
                errors.update(self.flush_enabled_error(test_data))
                errors.update(self.magma_max_shards_error(test_data, is_creation))
                if test_data.get('bucketType') != "memcached":
                    errors.update(self.pitr_errors(test_data, original_data))
                errors.update(self.drift_threshold_errors(test_data))
                errors.update(self.storage_quota_percentage_error(test_data))
                errors.update(self.width_weight_error(test_data, original_data))
                errors.update(self.num_vbuckets_error(test_data, is_creation))

            auto_compaction = self.auto_compaction_defined_error(test_data,
                                                                 errors)
        if endpoint == SET_AUTO_COMPACTION_ENDPOINT or auto_compaction:
            # AutoCompaction errors
            errors.update(self.magma_fragmentation_percentage_error(test_data))
            if test_data.get("storageBackend") != "magma":
                errors.update(
                    self.parallel_db_and_view_compaction_error(test_data))
                errors.update(self.database_fragmentation_threshold(test_data))
                errors.update(self.view_fragmentation_threshold(test_data))
                errors.update(self.allowed_time_period_error(test_data))
            if test_data.get('bucketType') != "ephemeral":
                errors.update(self.purge_interval_error(test_data))

        # When errors occur, the changing replica number warning is shown whether
        # or not just_validate is True
        if "replicaNumber" in test_data and "replicaNumber" not in errors \
                and (errors or just_validate) and not is_creation:
            errors.update({"replicaNumber":
                               "Warning: changing replica number may require "
                               "rebalance."})
        return errors

    def gen_params(self, good, just_validate, test_param):
        if good:
            test_values = self.test_params.get(test_param)
        else:
            test_values = self.bad_params.get(test_param)
        if test_values is None:
            return []

        def set_param(params, param, value):
            # None is specified as a valid value for optional parameters.
            # The dictionary contains elements of the form: {'key': None} in
            # combinations in which the 'key' is absent.
            if value != None:
                # Requires special handling
                sub_params = param.split(',')
                if len(sub_params) == 1:
                    params[param] = value
                else:
                    for i, sub_param in enumerate(sub_params):
                        if value[i] is not None:
                            params[sub_param] = value[i]

        for test_value in test_values:
            params = {}

            for other_param, other_values in self.test_params.items():
                if test_param != other_param:
                    if other_param == "name" and not just_validate and good:
                        set_param(params, other_param, self.get_next_name())
                    else:
                        set_param(params, other_param,
                                  random.choice(other_values))

            # Add parameter of interest at end to override prior value
            # (for parameters requiring special handling)
            set_param(params, test_param, test_value)
            yield params

    """
    When good=True, all possible good values for param are exercised.
    When good=False, iterate through all good combinations and replace a random
    parameter's value with self.bad_params[key], or specify an unsupported key.
    """

    def test_body(self, good, endpoint, param, original_data):
        just_validate = self.cur_main_dict['just_validate']
        gen = self.gen_params(good, just_validate, param)

        for test_data in gen:
            if just_validate:
                params = {'just_validate': 1}
            else:
                params = {}

            is_creation = endpoint == BUCKETS_ENDPOINT

            # Determine expected result of the test
            errors = self.get_errors(endpoint, test_data, original_data,
                                     just_validate, is_creation)
            expected_response = errors.get("_top_level_error")

            r = self.test_request('POST', endpoint, test_data, params=params,
                                  just_validate=just_validate,
                                  errors=errors,
                                  expected_response=expected_response)

            if good and not just_validate:
                if endpoint == BUCKETS_ENDPOINT and r.status_code == 202:
                    name = test_data['name']
                    # Test that the bucket was properly created
                    self.cluster.wait_for_bucket(name)
                    # Since we are testing bucket creation then we will not
                    # reuse this bucket, so we need to delete it.
                    self.test_delete(f"{endpoint}/{name}")
                elif self.is_enterprise and self.is_76 \
                        and test_data['bucketType'] != "memcached":
                    pitr_reset = {'pitrGranularity': 600,
                                  'pitrMaxHistoryAge': 86400}
                    self.test_request('POST', endpoint, pitr_reset)

    def main_params_valid(self, main_dict):
        memcached = main_dict['bucket_type'] == "memcached"
        ephemeral = main_dict['bucket_type'] == "ephemeral"
        magma = main_dict['storage_backend'] == "magma"
        auto_compaction = main_dict['auto_compaction_defined'] is not None

        if memcached and self.is_serverless:
            # No memcached on serverless
            return False
        elif ephemeral and (auto_compaction or magma):
            # No auto compaction or magma for ephemeral buckets
            return False
        else:
            return True

    """
    Given an input dictionary with possible parameter value(s):
    param1: [ value1, value2, ... ]
    param2: [ value3, value4, ... ],
    generate all possible combinations:
    { param1: value1, param2: value3 }, { param1: value1, param2: value4 },
    { param1: value2: param2: value3 }, { param1: value2: param2: value4 }
    and filter out any invalid configurations
    """

    def get_main_dicts(self, **kwargs):
        keys = kwargs.keys()
        vals = kwargs.values()
        for instance in itertools.product(*vals):
            elem = dict(zip(keys, instance))
            if self.main_params_valid(elem):
                yield elem

    """
    get_main_dicts generates all valid combinations {k1:v1, k2:v2, k3:v3... },
    for every (k, v) in main_params {k1: [v1...], k2: [v2...], k3: [v3...]}
    Each configuration is tested with test_body. Bucket update tests are
    prepared with a compatible bucket to update
    """

    def test_all_configurations(self, good, param, main_params):

        gen = self.get_main_dicts(**main_params)

        for main_dict in gen:
            self.cur_main_dict = main_dict

            bucket_type = main_dict['bucket_type']
            storage_backend = main_dict['storage_backend']
            rank = main_dict['rank']
            conflict_resolution_type = main_dict['conflict_resolution_type']
            is_creation = main_dict['is_creation']

            self.init_limits(bucket_type, storage_backend, is_creation, param)
            self.init_bad_params(**main_dict)
            self.init_test_params(param, **main_dict)

            if is_creation:
                endpoint = BUCKETS_ENDPOINT
                initial_data = None
            else:
                # Test increasing ram quota when good, and decreasing when bad
                min_or_max = ['max', 'min'][good]

                # Prepare bucket for updating
                initial_data = {
                    "name": self.get_next_name(),
                    "ramQuota": self.limits['ramQuota'][min_or_max],
                    "bucketType": bucket_type,
                    "rank": rank,
                    "storageBackend": storage_backend,
                    "conflictResolutionType": conflict_resolution_type
                }
                self.test_request('POST', BUCKETS_ENDPOINT, initial_data)
                endpoint = BUCKETS_ENDPOINT + "/" + initial_data["name"]

            self.test_body(good, endpoint, param, initial_data)

            # We don't delete the bucket after immediately after testing each
            # bucket update, as we reuse it for testing updates with the same
            # initial configuration. We must therefore delete it here, to allow
            # space for testing the next bucket configuration.
            if not is_creation:
                self.test_delete(endpoint)

    def eliminate_incompatible_values(self, main_params):
        def remove_value(param, value):
            if value in main_params[param]:
                main_params[param].remove(value)

        if not self.is_enterprise:
            remove_value('conflict_resolution_type', "lww")
        if not self.is_enterprise or not self.is_dev_preview:
            remove_value('conflict_resolution_type', "custom")

    """
    Perform a comprehensive test of a parameter, checking representative bad
    and good values for the parameter, as well as compatible good values for
    other parameters to ensure that combinations behave as expected.
    - param: The parameter to be tested
    - **main_params: Defines lists of higher level configuration details to be
                     tested. The following can be specified:
                     - bucket_type: List of bucket types to test.
                     - storage_backend: List of storage backends to test.
                     - auto_compaction_defined: List of values for
                         autoCompactionDefined.
                     - conflict_resolution_type: List of values for
                         conflictResolutionType to test.
                     - bucket_placer: Sublist of [False, True] defining whether
                         to test with/without bucket placer parameters or both.
                     - allowed_time_period: Sublist of [False, True] defining
                         whether to test with/without allowedTimePeriod
                         parameters or both.
                     - just_validate: Sublist of [False, True] defining whether
                         to test with/without just_validate flag or both.
                     - is_creation: Sublist of [False, True] defining whether to
                         test bucket creates, updates, or both.
    """
    def test_param(self, param, **main_params):
        self.eliminate_incompatible_values(main_params)
        self.test_all_configurations(True, param, main_params)


class BasicBucketTestSet(BucketTestSetBase):
    @staticmethod
    def requirements():
        # 1024MiB is required to test magma
        return testlib.ClusterRequirements(min_memsize=1024,
                                           num_nodes=1,
                                           edition="Enterprise")

    def name_test(self):
        self.test_param("name",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        rank=[0],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True])

    def rank_test(self):
        self.test_param("rank",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        rank=[0, 10, 1000],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[False],
                        is_creation=[False])

    def bucket_type_test(self):
        self.test_param("bucketType",
                        bucket_type=["membase", "couchbase", "ephemeral", None],
                        storage_backend=["couchstore", "magma"],
                        auto_compaction_defined=[None],
                        rank=[0],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True])

    def ram_quota_test(self):
        self.test_param("ramQuota",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore", "magma"],
                        auto_compaction_defined=[None],
                        rank=[0],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False, True],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def storage_backend_test(self):
        self.test_param("storageBackend",
                        bucket_type=["couchbase", "ephemeral"],
                        storage_backend=["couchstore", "magma"],
                        auto_compaction_defined=[None],
                        rank=[0],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True])

    def eviction_policy_test(self):
        self.test_param("evictionPolicy",
                        bucket_type=["couchbase", "memcached", "ephemeral"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno"],
                        rank=[0],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def dura_min_level_test(self):
        self.test_param("durabilityMinLevel",
                        bucket_type=["couchbase", "memcached", "ephemeral"],
                        storage_backend=["couchstore"],
                        rank=[0],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def threads_number_test(self):
        self.test_param("threadsNumber",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        rank=[0],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def compression_mode_test(self):
        self.test_param("compressionMode",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        rank=[0],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def max_ttl_test(self):
        self.test_param("maxTTL",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        rank=[0],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def conflict_resolution_type_test(self):
        self.test_param("conflictResolutionType",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno", "lww", "custom"],
                        rank=[0],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def flush_enabled_test(self):
        self.test_param("flushEnabled",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno"],
                        rank=[0],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def pitr_test(self):
        main_params = {
            "bucket_type": ["couchbase"],
            "storage_backend": ["couchstore"],
            "auto_compaction_defined": [None],
            "conflict_resolution_type": ["seqno"],
            "bucket_placer": [False],
            "rank": [0],
            "allowed_time_period": [False],
            "just_validate": [True, False],
            "is_creation": [True, False]
        }
        self.test_param("pitrEnabled", **main_params)
        self.test_param("pitrGranularity,pitrMaxHistoryAge", **main_params)

    def drift_threshold_test(self):
        main_params = {
            "bucket_type": ["couchbase"],
            "storage_backend": ["couchstore"],
            "auto_compaction_defined": [None],
            "conflict_resolution_type": ["seqno", "lww"],
            "bucket_placer": [False],
            "rank": [0],
            "allowed_time_period": [False],
            "just_validate": [True, False],
            "is_creation": [True, False]
        }
        self.test_param("driftAheadThresholdMs", **main_params)
        self.test_param("driftBehindThresholdMs", **main_params)

    def storage_quota_percentage_test(self):
        main_params = {
            "bucket_type": ["couchbase"],
            "storage_backend": ["couchstore", "magma"],
            "auto_compaction_defined": [None],
            "conflict_resolution_type": ["seqno"],
            "bucket_placer": [False],
            "rank": [0],
            "allowed_time_period": [False],
            "just_validate": [True, False],
            "is_creation": [True, False]
        }
        self.test_param("storageQuotaPercentage", **main_params)

    def duplicate_name_test(self):
        self.init_limits("couchbase", "couchstore", True)
        # Simple test
        bucket_data = self.add_required_fields({})
        self.test_post(BUCKETS_ENDPOINT, data=bucket_data)

        # Duplicate name
        self.test_post(BUCKETS_ENDPOINT, data=bucket_data,
                       errors={"name": "Bucket with given name already exists"})

    # MB-54441 Bucket ram quota can sometimes be set too high when updating...
    def ram_quota_rapid_update_test(self):
        self.cur_main_dict = {
            "bucket_type": "couchbase",
            "storage_backend": "couchstore"
        }
        self.init_limits("couchbase", "couchstore", True)

        self.test_request('POST', BUCKETS_ENDPOINT,
                     data={
                         "name": "default",
                         "ramQuota": 256
                     })
        self.init_limits("couchbase", "couchstore", False)
        self.test_request('POST', BUCKET_ENDPOINT_DEFAULT,
                          data={"ramQuota": self.cluster.memory_quota()*2},
                          errors={'ramQuota':
                                  'RAM quota specified is too large to be '
                                  'provisioned into this cluster.'})


class ServerlessBucketTestSet(BucketTestSetBase):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(min_memsize=1024,
                                           num_nodes=1,
                                           edition="Serverless")

    def bucket_placer_test(self):
        main_params = {
            "bucket_type": ["couchbase"],
            "storage_backend": ["couchstore"],
            "auto_compaction_defined": [None],
            "conflict_resolution_type": ["seqno"],
            "rank": [0],
            "bucket_placer": [False, True],
            "allowed_time_period": [False],
            "just_validate": [True, False],
            "is_creation": [True, False]
        }
        self.test_param("width", **main_params)
        self.test_param("weight", **main_params)

    def magma_max_shards_test(self):
        self.test_param("magmaMaxShards",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore", "magma"],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno"],
                        rank=[0],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def num_vbuckets_test(self):
        self.test_param("numVBuckets",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        rank=[0],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])


class OnPremBucketTestSet(BucketTestSetBase):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise")

    def bucket_type_test(self):
        self.test_param("bucketType",
                        bucket_type=["memcached"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        rank=[0],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True])

    def ram_quota_test(self):
        self.test_param("ramQuota",
                        bucket_type=["memcached"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno"],
                        rank=[0],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    # TOFIX: replica_index and replica_number give different errors based
    # whether just_validate is True or False and also if the num_nodes is less
    # than or greater than the set value.

    # def replica_index_test(self):
    #     self.test_param("replicaIndex",
    #                     bucket_type=["couchbase", "memcached"],
    #                     storage_backend=["couchstore"],
    #                     auto_compaction_defined=[None],
    #                     conflict_resolution_type=["seqno"],
    #                     bucket_placer=[False],
    #                     allowed_time_period=[False],
    #                     just_validate=[True, False],
    #                     is_creation=[True])

    # def replica_number_test(self):
    #     self.test_param("replicaNumber",
    #                     bucket_type=["couchbase"],
    #                     storage_backend=["couchstore"],
    #                     auto_compaction_defined=[None],
    #                     conflict_resolution_type=["seqno"],
    #                     bucket_placer=[False],
    #                     allowed_time_period=[False],
    #                     just_validate=[True, False],
    #                     is_creation=[True, False])


class MultiNodeBucketTestSet(BucketTestSetBase):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(num_nodes=4)

    # def replica_number_test(self):
    #     self.test_param("replicaNumber",
    #                     bucket_type=["couchbase"],
    #                     storage_backend=["couchstore"],
    #                     auto_compaction_defined=[None],
    #                     conflict_resolution_type=["seqno"],
    #                     bucket_placer=[False],
    #                     allowed_time_period=[False],
    #                     just_validate=[True, False],
    #                     is_creation=[True, False])

    def dura_min_level_test(self):

        self.test_param("durabilityMinLevel",
                        bucket_type=["couchbase", "memcached", "ephemeral"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None],
                        conflict_resolution_type=["seqno"],
                        rank=[0],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])
