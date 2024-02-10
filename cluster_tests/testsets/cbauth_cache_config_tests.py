# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
import testlib
import re

class CbAuthCacheConfigTests(testlib.BaseTestSet):

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    def setup(self):
        self.test_size = 1234
        self.cache_dict = {"up": None, "auth": None}
        self.store_original_sizes()

    def teardown(self):
        for cache_name, original_size in self.cache_dict.items():
            config_name = to_config_name(cache_name)
            self.set_cbauth_cache_size(config_name, original_size)

    def set_cbauth_cache_size(self, config_name, cache_size):
        d = 'ns_config:set({cbauth_cache_size, xdcr, ' + \
            f'{config_name}}}, {cache_size})'

        testlib.post_succ(self.cluster, "/diag/eval", data=d)

    def get_cbauth_cache_size(self, stat_name):
        res = testlib.get_succ(self.cluster, '/_prometheusMetrics')
        descriptor = '{category="cbauth",service="xdcr"}'
        pattern = f'({stat_name}{descriptor}[\s]+)([0-9]+)'
        match = re.search(pattern, res.text)
        if match is None:
            return None
        return int(match.group(2))

    def check_cbauth_stats(self, stat_name):
        res = self.get_cbauth_cache_size(stat_name)
        testlib.assert_eq(res, self.test_size, f"cache size - {stat_name}")

    def store_original_sizes(self):
        for cache_name in self.cache_dict.keys():
            config_name = to_config_name(cache_name)
            stat_name = to_stat_name(cache_name)

            def cache_initialized():
                return (self.get_cbauth_cache_size(stat_name) or 0) > 0

            testlib.poll_for_condition(cache_initialized, 1, attempts=20,
                                       verbose=True,
                                       msg=f"Wait for cache to be initialized")
            original_size = self.get_cbauth_cache_size(stat_name)
            self.cache_dict[cache_name] = original_size

    def set_cbauth_cache_size_test(self):
        for cache_name in self.cache_dict:
            config_name = to_config_name(cache_name)
            stat_name = to_stat_name(cache_name)
            self.set_cbauth_cache_size(config_name, self.test_size)
            self.check_cbauth_stats(stat_name)

def to_config_name(cache_name):
    return cache_name + '_cache_size'

def to_stat_name(cache_name):
    return 'cm_' + cache_name + '_cache_max_items'
