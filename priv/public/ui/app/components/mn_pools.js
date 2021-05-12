/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default 'mnPools';

angular
  .module('mnPools', [])
  .factory('mnPools', mnPoolsFactory);

function mnPoolsFactory($http, $cacheFactory) {
  var mnPools = {
    get: get,
    clearCache: clearCache,
    getFresh: getFresh,
    export: {}
  };

  var launchID =  (new Date()).valueOf() + '-' + ((Math.random() * 65536) >> 0);

  return mnPools;

  function get(mnHttpParams) {
    return $http({
      method: 'GET',
      url: '/pools',
      cache: true,
      mnHttp: mnHttpParams,
      requestType: 'json'
    }).then(function (resp) {
      var pools = resp.data;
      pools.isInitialized = !!pools.pools.length;
      pools.launchID = pools.uuid + '-' + launchID;
      Object.assign(mnPools.export, pools);
      return pools;
    });
  }
  function clearCache() {
    $cacheFactory.get('$http').remove('/pools');
    return this;
  }
  function getFresh() {
    return mnPools.clearCache().get();
  }
}
