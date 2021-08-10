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
import mnFilters from "/ui/app/components/mn_filters.js";
import mnServersService from "./mn_servers_service.js";
import mnBucketsDetailsService from "./mn_buckets_details_service.js";
import mnSettingsAutoCompactionService from "./mn_settings_auto_compaction_service.js";

export default "mnBucketsDetailsDialogService";

angular
  .module('mnBucketsDetailsDialogService', [
    mnFilters,
    mnPoolDefault,
    mnServersService,
    mnBucketsDetailsService,
    mnSettingsAutoCompactionService])
  .factory('mnBucketsDetailsDialogService', mnBucketsDetailsDialogServiceFactory);

function mnBucketsDetailsDialogServiceFactory($http, $q, mnBytesToMBFilter, mnCountFilter, mnSettingsAutoCompactionService, mnPoolDefault, mnServersService, bucketsFormConfiguration, mnBucketsDetailsService) {
  var mnBucketsDetailsDialogService = {
    prepareBucketConfigForSaving: prepareBucketConfigForSaving,
    adaptValidationResult: adaptValidationResult,
    getNewBucketConf: getNewBucketConf,
    reviewBucketConf: reviewBucketConf,
    postBuckets: postBuckets
  };

  return mnBucketsDetailsDialogService;

  function postBuckets(data, uri) {
    return $http({
      data: data,
      method: 'POST',
      url: uri
    });
  }
  function prepareBucketConfigForSaving(bucketConf, autoCompactionSettings, poolDefault, pools) {
    var conf = {};
    var isMembase = bucketConf.bucketType === "membase";
    function copyProperty(property) {
      if (bucketConf[property] !== undefined) {
        conf[property] = bucketConf[property];
      }
    }
    function copyProperties(properties) {
      properties.forEach(copyProperty);
    }
    if (bucketConf.isNew) {
      copyProperties(["name", "bucketType"]);
    }
    if (pools.isDeveloperPreview && pools.isEnterprise && isMembase) {
      copyProperty("storageBackend");
      if (bucketConf.storageBackend === "magma") {
        copyProperty("fragmentationPercentage");
      }
    }
    if (isMembase) {
      copyProperties(["autoCompactionDefined", "evictionPolicy"]);
    }
    if (bucketConf.bucketType === "ephemeral") {
      copyProperties(["purgeInterval", "durabilityMinLevel"]);
      conf["evictionPolicy"] = bucketConf["evictionPolicyEphemeral"];
    }

    if (isMembase || bucketConf.bucketType === "ephemeral") {
      copyProperties(["threadsNumber", "replicaNumber", "durabilityMinLevel"]);
      if (pools.isEnterprise && poolDefault.compat.atLeast55) {
        copyProperty("compressionMode");
        if (!bucketConf.enableMaxTTL) {
          conf.maxTTL = 0;
        } else {
          copyProperty("maxTTL");
        }
      }
      if (bucketConf.isNew) {
        if (bucketConf.bucketType !== "ephemeral") {
          copyProperty("replicaIndex");
        }

        if (pools.isEnterprise) {
          copyProperty("conflictResolutionType");
        }
      }

      if (bucketConf.autoCompactionDefined) {
        _.extend(conf, mnSettingsAutoCompactionService.prepareSettingsForSaving(autoCompactionSettings));
      }
    }

    if (bucketConf.isWizard) {
      copyProperty("otherBucketsRamQuotaMB");
    }

    copyProperties(["ramQuota", "flushEnabled"]);

    return conf;
  }
  function adaptValidationResult(result) {
    var ramSummary = result.summaries.ramSummary;

    return {
      totalBucketSize: mnBytesToMBFilter(ramSummary.thisAlloc),
      nodeCount: mnCountFilter(ramSummary.nodesCount, 'node'),
      perNodeMegs: ramSummary.perNodeMegs,
      guageConfig: mnBucketsDetailsService.getBucketRamGuageConfig(ramSummary),
      errors: mnSettingsAutoCompactionService.prepareErrorsForView(result.errors)
    };
  }
  function getNewBucketConf() {
    return $q.all([
      mnServersService.getNodes(),
      mnPoolDefault.get()
    ]).then(function (resp) {
      var activeServersLength = resp[0].reallyActiveData.length;
      var totals = resp[1].storageTotals;
      var bucketConf = _.clone(bucketsFormConfiguration);
      bucketConf.isNew = true;
      bucketConf.ramQuota = totals.ram ? mnBytesToMBFilter(Math.floor((totals.ram.quotaTotal - totals.ram.quotaUsed) / activeServersLength)) : 0;
      return bucketConf;
    });
  }
  function reviewBucketConf(bucketDetails) {
    return mnBucketsDetailsService.doGetDetails(bucketDetails).then(function (bucketConf) {
      bucketConf["evictionPolicyEphemeral"] = bucketConf["evictionPolicy"];
      bucketConf.ramQuota = mnBytesToMBFilter(bucketConf.quota.rawRAM);
      bucketConf.threadsNumber = bucketConf.threadsNumber.toString();
      bucketConf.isDefault = bucketConf.name === 'default';
      bucketConf.enableMaxTTL = bucketConf.maxTTL !== 0;
      bucketConf.replicaIndex = bucketConf.replicaIndex ? 1 : 0;
      bucketConf.flushEnabled = (bucketConf.controllers !== undefined && bucketConf.controllers.flush !== undefined) ? 1 : 0;
      return bucketConf;
    });
  }
}
