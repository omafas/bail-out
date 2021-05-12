/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import _ from "/ui/web_modules/lodash.js";

import mnPoolDefault from "/ui/app/components/mn_pool_default.js";
import mnTasksDetails from "/ui/app/components/mn_tasks_details.js";

import mnBucketsService from "./mn_buckets_service.js";
import mnServersService from "./mn_servers_service.js";

export default "mnSettingsSampleBucketsService";

angular
  .module("mnSettingsSampleBucketsService", [
    mnPoolDefault,
    mnTasksDetails,
    mnBucketsService,
    mnServersService
  ])
  .factory("mnSettingsSampleBucketsService", mnSettingsSampleBucketsFactory);

function mnSettingsSampleBucketsFactory($http, $q, mnPoolDefault, mnTasksDetails, mnBucketsService, mnServersService) {
  var mnSettingsSampleBucketsService = {
    getSampleBuckets: getSampleBuckets,
    installSampleBuckets: installSampleBuckets,
    getSampleBucketsState: getSampleBucketsState
  };

  return mnSettingsSampleBucketsService;

  function getSampleBucketsState(selectedBuckets) {
    return $q.all([
      getSampleBuckets(),
      mnPoolDefault.getFresh(),
      mnTasksDetails.get(),
      mnBucketsService.getBucketsByType(),
      mnServersService.getNodes()
    ]).then(function (resp) {
      var warnings = {
        quota: false,
        rebalance: false,
        maxBucketCount: false
      };
      var attentions = {};
      var sampleBuckets = resp[0];
      var poolDefault = resp[1];
      var tasks = resp[2];
      var buckets = resp[3];
      var servers = resp[4];

      let ram = poolDefault.storageTotals.ram;
      var numServers = servers.active.length;
      var quotaAvailable = ram ? (ram.quotaTotal - ram.quotaUsed) : 0;
      var maxNumBuckets = poolDefault.maxBucketCount;
      var numExistingBuckets = buckets.length;

      var storageNeeded = _.reduce(selectedBuckets, function (acc, quotaNeeded) {
        return acc + parseInt(quotaNeeded, 10);
      }, 0) * numServers;

      if (!(storageNeeded <= quotaAvailable)) {
        warnings.quota = Math.ceil(storageNeeded - quotaAvailable) / 1024 / 1024 / numServers;
      }
      warnings.maxBucketCount = (numExistingBuckets + _.keys(selectedBuckets).length > maxNumBuckets) && maxNumBuckets;
      warnings.rebalance = tasks.inRebalance;

      attentions.noIndexOrQuery = !_.find(servers.active, function (server) {
        return _.indexOf(server.services, "index") > -1;
      }) || !_.find(servers.active, function (server) {
        return _.indexOf(server.services, "n1ql") > -1;
      });

      return {
        installed: _.filter(sampleBuckets, 'installed', true),
        available: _.filter(sampleBuckets, 'installed', false),
        warnings: warnings,
        attentions: attentions
      };
    });
  }
  function getSampleBuckets() {
    return $http({
      url: '/sampleBuckets',
      method: 'GET'
    }).then(function (resp) {
      return resp.data;
    });
  }
  function installSampleBuckets(selectedSamples) {
    return $http({
      url: '/sampleBuckets/install',
      method: 'POST',
      timeout: 140000,
      data: JSON.stringify(_.keys(_.pick(selectedSamples, _.identity)))
    });
  }
}
