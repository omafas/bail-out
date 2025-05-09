/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

export default 'mnBucketsStats';

angular
  .module("mnBucketsStats", [])
  .factory("mnBucketsStats", ["$http", "$cacheFactory", mnBucketsFactory]);

function mnBucketsFactory($http, $cacheFactory) {
  var mnBucketsStats = {
    get: get,
    clearCache: clearCache,
  };

  return mnBucketsStats;

  function get(mnHttpParams) {
    return $http({
      method: "GET",
      cache: true,
      url: '/pools/default/buckets?basic_stats=true&skipMap=true',
      mnHttp: mnHttpParams
    });
  }

  function clearCache() {
    $cacheFactory.get('$http').remove('/pools/default/buckets?basic_stats=true&skipMap=true');
    return this;
  }
}
