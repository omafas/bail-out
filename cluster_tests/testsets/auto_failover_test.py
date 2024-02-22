# @author Couchbase <info@couchbase.com>
# @copyright 2022-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import collections
import itertools
import random
import testlib

class AutoFailoverSettingsTestBase(testlib.BaseTestSet):
    def __init__(self, cluster):
        super().__init__(cluster)
        self.addr = None
        self.auth = None

    @staticmethod
    def requirements():
        return testlib.ClusterRequirements()

    def init_limits(self):
        self.limits = collections.defaultdict(collections.defaultdict)
        if self.is_enterprise:
            self.limits['timeout']['min'] = 1
        else:
            self.limits['timeout']['min'] = 30
        self.limits['timeout']['max'] = 3600

        if self.is_enterprise:
            self.limits['maxCount']['min'] = 1
            self.limits['maxCount']['max'] = 100
            self.limits['failoverOnDataDiskIssues[timePeriod]']['min'] = 5
            self.limits['failoverOnDataDiskIssues[timePeriod]']['max'] = 3600

    def setup(self):
        self.endpoint = '/settings/autoFailover'
        self.prev_settings = testlib.get(self.cluster, self.endpoint).json()
        # count cannot be modified using this request
        self.result_keys = list(self.prev_settings.keys())
        self.result_keys.remove('count')
        self.is_enterprise = self.cluster.is_enterprise
        self.is_76 = self.cluster.is_76
        self.is_serverless = self.cluster.is_serverless

        failKey = 'failoverOnDataDiskIssues'
        self.post_data_keys = [ k for k in self.result_keys if k != failKey ]
        if failKey in self.result_keys:
            assert self.is_enterprise
            self.post_data_keys.append(failKey + '[enabled]')
            self.post_data_keys.append(failKey + '[timePeriod]')

        assert 'enabled' in self.post_data_keys
        assert 'timeout' in self.post_data_keys
        self.enterprise_only = [ 'maxCount',
                                 'failoverOnDataDiskIssues[enabled]',
                                 'failoverOnDataDiskIssues[timePeriod]',
                                 'failoverServerGroup',
                                 'canAbortRebalance',
                                 'disableMaxCount' ]
        if self.is_enterprise:
            assert 'canAbortRebalance' in self.post_data_keys
            if self.is_76:
                assert 'disableMaxCount' in self.post_data_keys
                if self.is_serverless:
                    assert self.prev_settings['disableMaxCount']
                else:
                    assert not self.prev_settings['disableMaxCount']
                if 'maxCount' not in self.result_keys:
                    assert self.is_serverless
                    assert self.prev_settings['disableMaxCount']
                    # 'maxCount' is supported but not returned if
                    # disableMaxCount is True
                    self.post_data_keys.append('maxCount')
            else:
                assert 'maxCount' in self.result_keys
        else:
            for x in self.enterprise_only:
                assert x not in self.post_data_keys

        self.init_limits()
        self.init_test_params()
        self.init_bad_params()
        self.bad_count = self.good_count = 0

    def teardown(self):
        pass

    """
    Populate test_params with valid values for all supported parameters.
    None indicates that the parameter may be omitted.
    """
    def init_test_params(self):
        self.test_params={}
        self.test_params['enabled'] = ['true', 'false']
        self.test_params['timeout'] = [ None, self.limits['timeout']['min'],
                                        self.limits['timeout']['max']]
        if self.is_enterprise:
            self.test_params['maxCount'] = [ None,
                                             self.limits['maxCount']['min'],
                                             self.limits['maxCount']['max']]
            self.test_params['failoverOnDataDiskIssues[enabled]'] = [ None,
                                                                      'true',
                                                                     'false']
            self.test_params['failoverOnDataDiskIssues[timePeriod]'] = [ None,
                self.limits['failoverOnDataDiskIssues[timePeriod]']['min'],
                self.limits['failoverOnDataDiskIssues[timePeriod]']['max']]

        if 'failoverServerGroup' in self.post_data_keys:
            self.test_params['failoverServerGroup'] = [ None, 'true', 'false']

        if 'canAbortRebalance' in self.post_data_keys:
            self.test_params['canAbortRebalance'] = [ None, 'true', 'false' ]

        if 'disableMaxCount' in self.post_data_keys:
            self.test_params['disableMaxCount'] = [ None, 'true', 'false' ]

    # Populate bad_params with invalid values for all supported parameters.
    def init_bad_params(self):
        self.bad_params = {}
        self.bad_params['enabled'] = [ None ]
        self.bad_params['timeout'] = [ 'bogus',
                                       self.limits['timeout']['min'] - 1,
                                       self.limits['timeout']['max'] + 1 ]
        if self.is_enterprise:
            self.bad_params['maxCount'] = [
                self.limits['maxCount']['min'] - 1,
                self.limits['maxCount']['max'] + 1,
                'invalid' ]
            self.bad_params['failoverOnDataDiskIssues[enabled]'] = [ 'truue' ]
            self.bad_params['failoverOnDataDiskIssues[timePeriod]'] = [
                self.limits['failoverOnDataDiskIssues[timePeriod]']['min'] - 1,
                self.limits['failoverOnDataDiskIssues[timePeriod]']['max'] + 1 ]

        if 'failoverServerGroup' in self.post_data_keys:
            self.bad_params['failoverServerGroup'] = [ 0 ]
        if 'canAbortRebalance' in self.post_data_keys:
            self.bad_params['canAbortRebalance'] = [ 1 ]
        if 'disableMaxCount' in self.post_data_keys:
            self.bad_params['disableMaxCount'] = ['disabled']

    def get_integer_error(self, testData, field):
        if field in testData:
            try:
                val = int(testData[field])
                assert field in self.limits
                if val < self.limits[field]['min'] or \
                   val > self.limits[field]['max']:
                    return { field: 'The value must be in range from ' +
                             str(self.limits[field]['min']) + ' to ' +
                             str(self.limits[field]['max']) }
            except ValueError:
                return { field: 'The value must be an integer' }
        return {}

    def get_boolean_error(self, testData, field):
        if field in testData and testData[field] not in ['true', 'false']:
            return { field: 'The value must be one of the following: [true,'
                     'false]' }
        return {}

    def get_unsupported_errors(self, testData):
        errors = {}
        for key in testData.keys():
            if key not in self.post_data_keys:
                errors[key] = 'Unsupported key'
        return errors

    def is_undefined(self, testData, field, type_field):
        assert type_field in ['boolean', 'integer']
        if field not in testData:
            return True
        elif type_field == 'boolean':
            return self.get_boolean_error(testData, field)
        elif type_field == 'integer':
            return self.get_integer_error(testData, field)
        return False

    def check_max_count(self, testData):
        disableCountUndefined = self.is_undefined(testData, 'disableMaxCount',
                                                  'boolean')
        maxCountUndefined = self.is_undefined(testData, 'maxCount', 'integer')
        err = {'_': 'disableMaxCount is true. Set it to false for maxCount'
               ' to take effect.' }
        if 'disableMaxCount' in testData and \
           testData['disableMaxCount'] == 'true' and not maxCountUndefined:
            return err
        elif disableCountUndefined and not maxCountUndefined and \
             self.prev_settings['disableMaxCount']:
            return err
        return {}

    def check_enabled_time(self, testData, field1, field2):
        field1_undefined = self.is_undefined(testData, field1, 'boolean')
        field2_undefined = self.is_undefined(testData, field2, 'integer')
        if field1 in testData and testData[field1] == 'true' and \
           field2_undefined:
            return {'_': field1 + ' is true. A value must be supplied for ' +
                    field2 }
        elif field1_undefined and not field2_undefined:
            return {'_': field1 + ' must be true for ' + field2 + ' to take '
                    'effect'}
        return {}

    def get_errors(self, testData):
        errors = {}

        if 'enabled' not in testData:
            errors['enabled'] = 'The value must be supplied'
        errors.update(self.get_boolean_error(testData, 'enabled'))
        errors.update(self.get_integer_error(testData, 'timeout'))
        errors.update(self.check_enabled_time(testData, 'enabled', 'timeout'))

        if not self.is_enterprise:
            errors.update(self.get_unsupported_errors(testData))
            return errors

        errors.update(self.get_integer_error(testData, 'maxCount'))
        if 'disableMaxCount' in self.post_data_keys:
            errors.update(self.get_boolean_error(testData, 'disableMaxCount'))

        # Only one validate_multiple error is tracked
        if 'disableMaxCount' in self.post_data_keys and '_' not in errors:
            errors.update(self.check_max_count(testData))

        errors.update(self.get_boolean_error(testData,
                                    'failoverOnDataDiskIssues[enabled]'))
        errors.update(self.get_integer_error(testData,
                                    'failoverOnDataDiskIssues[timePeriod]'))

        # Only one validate_multiple error is tracked
        if '_' not in errors:
            errors.update(self.check_enabled_time(testData,
                                            'failoverOnDataDiskIssues[enabled]',
                                        'failoverOnDataDiskIssues[timePeriod]'))

        if 'canAbortRebalance' in self.post_data_keys:
            errors.update(self.get_boolean_error(testData, 'canAbortRebalance'))
        if 'failoverServerGroup' in self.post_data_keys:
            errors.update(self.get_boolean_error(testData,
                                                 'failoverServerGroup'))

        errors.update(self.get_unsupported_errors(testData))
        return errors

    """
    Nested keys in the POST form are encoded in the dictionary as:
    { keyA[subKey] : value }
    Modify the dictionary to reflect nesting to compare with the JSON response.
    { keyA: { subKey: value } }
    Booleans are encoded as "true"/"false". Switch to True/False to compare.
    """
    def get_delta_from_form_data(self, delta):
        formDict = {}
        for key in delta:
            tokens = key.split('[')
            if len(tokens) == 2:
                subkey = tokens[1].split(']')
                if tokens[0] not in formDict:
                    formDict[tokens[0]] = {}
                formDict[tokens[0]][subkey[0]] = self.get_form_value(delta[key])
            elif len(tokens) == 1:
                formDict[tokens[0]] = \
                    self.get_form_value(delta[key])
            else:
                assert(0)

        return formDict

    def get_form_value(self, s):
        if s == 'true':
            return True
        elif s == 'false':
            return False
        else:
            return s

    def compare_settings(self, resp, delta):
        newDict = self.get_delta_from_form_data(delta)
        for key in self.result_keys:
            if key in newDict:
                dict2 = newDict
            else:
                dict2 = self.prev_settings
            assert resp[key] == dict2[key]

    def validate_settings(self, testData, errors):
        # Get new settings to check whether the requested settings were applied.
        resp = testlib.get(self.cluster, self.endpoint)
        assert resp.status_code == 200

        delta = {}
        if not errors:
            delta = testData

            # A request with 'enabled' set to 'false', ignores the rest of the
            # settings but sets failoverOnDataDiskIssues[enabled] to false.
            # All other settings are not modified (i.e. they retain previous
            # values) even if new values are specified in the POST request.

            if 'enabled' in delta and delta['enabled'] == 'false':
                delta = {'enabled':'false'}
                if 'failoverOnDataDiskIssues' in self.result_keys:
                    delta['failoverOnDataDiskIssues[enabled]'] = 'false'

            # If failoverOnDataDiskIssues[enabled] is false,
            # failoverOnDataDiskIssues[timePeriod] is ignored.
            if 'failoverOnDataDiskIssues[enabled]' in delta and \
               delta['failoverOnDataDiskIssues[enabled]'] == 'false':
                delta['failoverOnDataDiskIssues[timePeriod]'] = \
                    self.prev_settings['failoverOnDataDiskIssues']['timePeriod']

            # Toggling disableMaxCount causes maxCount to be added/removed
            if 'disableMaxCount' in delta and \
               delta['disableMaxCount'] == 'true' and \
               not self.prev_settings['disableMaxCount']:
                # maxCount will be pruned from queried settings
                assert 'maxCount' in self.prev_settings
                del self.prev_settings['maxCount']
                self.result_keys = list(self.prev_settings.keys())
            elif 'disableMaxCount' in delta and \
                 delta['disableMaxCount'] == 'false' and \
                 self.prev_settings['disableMaxCount']:
                # maxCount will appear in the queried settings
                assert 'maxCount' not in self.prev_settings
                self.prev_settings['maxCount'] = resp.json()['maxCount']
                self.result_keys = list(self.prev_settings.keys())

        self.compare_settings(resp.json(), delta)
        self.prev_settings = resp.json()

    def test_request(self, testData):
        # Use query string just_validate=1 the first time around
        resp = testlib.post(self.cluster, self.endpoint, data=testData,
                            params={'just_validate': 1})
        errors = self.get_errors(testData)
        if not errors:
            assert resp.status_code == 200
        else:
            assert resp.status_code == 400
            assert resp.json()['errors'] == errors

        # The settings should not have changed
        self.validate_settings({}, errors)

        # POST the request for real without the query string
        resp = testlib.post(self.cluster, self.endpoint, data=testData)
        if not errors:
            assert resp.status_code == 200
            self.good_count = self.good_count + 1
        else:
            assert resp.status_code == 400
            assert resp.json()['errors'] == errors
            self.bad_count = self.bad_count + 1

        # The settings must change if there aren't any errors
        self.validate_settings(testData, errors)

    def get_unsupported_key(self):
        if not self.is_enterprise:
            return random.choice(self.enterprise_only)

        if self.is_76:
            return 'failoverServerGroup'

        return random.choice(['failoverServerGroup', 'disableMaxCount'])

    """
    Given an input dictionary with possible parameter value(s):
    param1: [ value1, value2, ... ]
    param2: [ value3, value4, ... ],
    generate all possible combinations:
    { param1: value1, param2: value3 }, { param1: value1, param2: value4 },
    { param1: value2: param2: value3 }, { param1: value2: param2: value4 }
    """
    def product_dict(self, **kwargs):
        keys = kwargs.keys()
        vals = kwargs.values()
        for instance in itertools.product(*vals):
            yield dict(zip(keys, instance))

    def mangle_param(self, testData):
        index = random.randint(0, len(testData))
        if index == len(testData):
            key = self.get_unsupported_key()
            assert key not in self.post_data_keys
            testData[key] = 'true'
        else:
            key = list(testData.keys())[index]
            assert key in self.bad_params
            testData[key] = random.choice(self.bad_params[key])
            if testData[key] is None:
                del testData[key]

    """
    product_dict generates all possible combinations {k1:v1, k2:v2, k3:v3... },
    for every (k, v) in self.test_params{k1: [v1...], k2: [v2...], k3: [v3...]}
    When good=True, all possible good combinations are exercised.
    When good=False, iterate through all good combinations and replace a random
    parameter's value with self.bad_params[key], or specify an unsupported key.
    """
    def test_body(self, good=True):
        self.bad_count = self.good_count = 0
        gen = self.product_dict(**self.test_params)

        for elem in gen:
            # None is specified as a valid value for optional parameters.
            # The dictionary contains elements of the form: {'key': None} in
            # combinations in which the 'key' is absent.
            testData = {k: v for k, v in elem.items() if v is not None}

            # 'timeout' (or 'failoverOnDataDiskIssues[timePeriod]') must be
            # specified if 'enabled' (or 'failoverOnDataDiskIssues[timePeriod]')
            # is True.
            # 'enabled' (or 'failoverOnDataDiskIssues[enabled]') must be
            # specified if 'timeout' (or 'failoverOnDataDiskIssues[timePeriod]')
            # is a valid value.
            # disableMaxCount must be false if maxCount is specified.
            bad_param = \
                self.check_enabled_time(testData, 'enabled', 'timeout') or \
                self.check_enabled_time(testData,
                                        'failoverOnDataDiskIssues[enabled]',
                                    'failoverOnDataDiskIssues[timePeriod]') or \
                self.is_76 and self.check_max_count(testData)

            if good:
                if bad_param:
                    continue
            elif not bad_param:
                self.mangle_param(testData)
            # {'failover...[enabled]': 'true', absent 'failover...[timePeriod]'}
            # {absent 'failover...[enabled], valid 'failover...[timePeriod]'}
            # are invalid combinations. Test them as is without mangling i.e.
            # exercise test cases with bad_param=True. They must fail.

            self.test_request(testData)

        print("Ran " + str(self.good_count) + " good cases, " +
              str(self.bad_count) + " bad cases.")

    def run_combinations(self):
        self.test_body(good=True)
        assert(self.bad_count == 0)
        self.test_body(good=False)
        assert(self.good_count == 0)

class OnPremAutoFailoverSettingsTest(AutoFailoverSettingsTestBase):
    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Enterprise")

    def server_test(self):
        self.run_combinations()

class ServerlessAutoFailoverSettingsTest(AutoFailoverSettingsTestBase):
    @staticmethod
    def requirements():
        return testlib.ClusterRequirements(edition="Serverless")

    def server_test(self):
        self.run_combinations()
