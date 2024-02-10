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

import requests
import testlib
from bucket_test import BucketTestSetBase
from bucket_test import SET_AUTO_COMPACTION_ENDPOINT, ALLOWED_TIME_PERIOD_PARAMS


class AutoCompactionTestSet(BucketTestSetBase):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(min_memsize=1024)

    def auto_compaction_defined_test(self):
        self.test_param("autoCompactionDefined",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=[None, "true", "false"],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def parallel_db_and_view_compaction_test(self):
        self.test_param("parallelDBAndViewCompaction",
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore", "magma"],
                        auto_compaction_defined=["true"],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def database_fragmentation_threshold_test(self):
        main_params = {
            "bucket_type": ["couchbase"],
            "storage_backend": ["couchstore"],
            "auto_compaction_defined": ["true"],
            "conflict_resolution_type": ["seqno"],
            "bucket_placer": [False],
            "allowed_time_period": [False],
            "just_validate": [True, False],
            "is_creation": [True, False]
        }
        self.test_param("databaseFragmentationThreshold[percentage]",
                        **main_params)
        self.test_param("databaseFragmentationThreshold[size]",
                        **main_params)

    def view_fragmentation_threshold_test(self):
        main_params = {
            "bucket_type": ["couchbase"],
            "storage_backend": ["couchstore"],
            "auto_compaction_defined": ["true"],
            "conflict_resolution_type": ["seqno"],
            "bucket_placer": [False],
            "allowed_time_period": [False],
            "just_validate": [True, False],
            "is_creation": [True, False]
        }
        self.test_param("viewFragmentationThreshold[percentage]",
                        **main_params)
        self.test_param("viewFragmentationThreshold[size]",
                        **main_params)

    def purge_interval_test(self):
        self.test_param("purgeInterval",
                        bucket_type=["couchbase", "ephemeral"],
                        storage_backend=["couchstore", "magma"],
                        auto_compaction_defined=["true"],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def allowed_time_period_test(self):
        self.test_param(ALLOWED_TIME_PERIOD_PARAMS,
                        bucket_type=["couchbase"],
                        storage_backend=["couchstore"],
                        auto_compaction_defined=["true"],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[True],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def magma_fragmentation_percentage_test(self):
        self.test_param("magmaFragmentationPercentage",
                        bucket_type=["couchbase"],
                        storage_backend=["magma"],
                        auto_compaction_defined=["true"],
                        conflict_resolution_type=["seqno"],
                        bucket_placer=[False],
                        allowed_time_period=[False],
                        just_validate=[True, False],
                        is_creation=[True, False])

    def global_test(self):
        self.init_limits(None, None, None)

        # Valid auto-compaction settings
        request = {
            "magmaFragmentationPercentage": 40,
            "databaseFragmentationThreshold[percentage]": 10,
            "databaseFragmentationThreshold[size]": 20,
            "viewFragmentationThreshold[percentage]": 10,
            "viewFragmentationThreshold[size]": 20,
            "allowedTimePeriod[fromHour]": 12,
            "allowedTimePeriod[fromMinute]": 32,
            "allowedTimePeriod[toHour]": 13,
            "allowedTimePeriod[toMinute]": 34,
            "allowedTimePeriod[abortOutside]": "false",
            "parallelDBAndViewCompaction": "true",
            "purgeInterval": 2.3,
            "indexCompactionMode": "full",
            "indexCircularCompaction[daysOfWeek]": "Monday,Wednesday",
            "indexCircularCompaction[interval][fromHour]": 14,
            "indexCircularCompaction[interval][fromMinute]": 21,
            "indexCircularCompaction[interval][toHour]": 16,
            "indexCircularCompaction[interval][toMinute]": 12,
            "indexCircularCompaction[interval][abortOutside]": "true"
        }
        self.test_post(SET_AUTO_COMPACTION_ENDPOINT, data=request)

        # Test circular index compaction mode
        request['indexCompactionMode'] = "circular"

        self.test_post(SET_AUTO_COMPACTION_ENDPOINT, data=request)

        # Invalid value for parallelDBAndViewCompaction
        request = {"parallelDBAndViewCompaction": "truee"}
        self.test_post(SET_AUTO_COMPACTION_ENDPOINT, data=request)

        # Missing required parameter parallelDBAndViewCompaction
        self.test_post(SET_AUTO_COMPACTION_ENDPOINT, data={})
