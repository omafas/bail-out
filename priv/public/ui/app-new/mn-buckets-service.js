/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnBuckets = (function (Rx) {
  "use strict";

  MnBucketsService.annotations = [
    new ng.core.Injectable()
  ];

  MnBucketsService.parameters = [
    ng.common.http.HttpClient,
    mn.services.MnAdmin
  ];

  MnBucketsService.prototype.get = get;
  MnBucketsService.prototype.getBucketRamGuageConfig = getBucketRamGuageConfig;
  MnBucketsService.prototype.getGuageConfig = getGuageConfig;
  MnBucketsService.prototype.postBucket = postBucket;
  MnBucketsService.prototype.prepareBucketConfigForSaving = prepareBucketConfigForSaving;

  return MnBucketsService;

  function MnBucketsService(http, mnAdminService) {
    this.http = http;
    this.stream = {};

    this.stream.updateBucketsPoller = new Rx.BehaviorSubject();

    var bucketsUri =
        mnAdminService.stream.getPoolsDefault.pipe(
          Rx.operators.pluck("buckets", "uri"),
          Rx.operators.distinctUntilChanged()
        );

    this.stream.bucketsWithTimer =
      Rx.combineLatest(
        bucketsUri,
        Rx.timer(0, 4000),
        this.stream.updateBucketsPoller
      ).pipe(
        Rx.operators.pluck("0"),
        Rx.operators.switchMap(this.get.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.buckets =
      bucketsUri.pipe(
        Rx.operators.switchMap(this.get.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.bucketsMembaseEphemeral =
      this.stream.buckets.pipe(Rx.operators.map(R.filter(R.anyPass([
        R.propEq('bucketType', 'membase'),
        R.propEq('bucketType', 'ephemeral')
      ]))));

    this.stream.bucketHttp =
      new mn.core.MnPostHttp(this.postBucket.bind(this))
      .addSuccess()
      .addError();

  }

  function postBucket(bucket) {
    return this.http.post(bucket[2] || "/pools/default/buckets", bucket[0], {
      params: new ng.common.http.HttpParams().set("just_validate", bucket[1] ? 1 : 0)
    });
  }

  function get(url) {
    return this.http.get(url, {
      params: new ng.common.http.HttpParams()
        .set("basic_stats", true)
        .set("skipMap", true)
    });
  }

  function prepareBucketConfigForSaving(values, isEnterprise, compatVersion55, isNew) {
    var conf = {};
    function copyProperty(property) {
      if (values[property] !== undefined) {
        conf[property] = values[property];
      }
    }
    function copyProperties(properties) {
      properties.forEach(copyProperty);
    }
    if (isNew) {
      copyProperties(["name", "bucketType"]);
    }
    if (values.bucketType === "membase") {
      copyProperties(["autoCompactionDefined", "evictionPolicy"]);
    }
    if (values.bucketType === "ephemeral") {
      copyProperty("purgeInterval");
      conf["evictionPolicy"] = values["evictionPolicyEphemeral"];
    }
    if (values.bucketType === "membase" ||
        values.bucketType === "ephemeral") {
      copyProperties(["threadsNumber", "replicaNumber"]);
      if (isEnterprise && compatVersion55) {
        copyProperties(["compressionMode", "maxTTL"]);
      }
      if (isNew) {
        if (values.bucketType !== "ephemeral") {
          conf.replicaIndex = values.replicaIndex ? 1 : 0
        }

        if (isEnterprise) {
          copyProperty("conflictResolutionType");
        }
      }

      if (values.autoCompactionDefined) {
        _.extend(conf, mnSettingsAutoCompactionService.prepareSettingsForSaving(autoCompactionSettings));
      }
    }

    conf.flushEnabled = values.flushEnabled ? 1 : 0

    copyProperties(["ramQuotaMB"]);

    return conf;
  }

  function getBucketRamGuageConfig(ramSummary) {
    if (!ramSummary) {
      return;
    }
    var bucketRamGuageConfig = {};
    bucketRamGuageConfig.topRight = {
      name: 'cluster quota',
      value: ramSummary.total
    };

    bucketRamGuageConfig.items = [{
      name: 'other buckets',
      value: ramSummary.otherBuckets
    }, {
      name: 'this bucket',
      value: ramSummary.thisAlloc
    }];

    bucketRamGuageConfig.bottomLeft = {
      name: 'remaining',
      value: ramSummary.total - ramSummary.otherBuckets - ramSummary.thisAlloc
    };

    if (bucketRamGuageConfig.bottomLeft.value < 0) {
      bucketRamGuageConfig.items[1].value = ramSummary.total - ramSummary.otherBuckets;
      bucketRamGuageConfig.bottomLeft = {
        name: 'overcommitted',
        value: ramSummary.otherBuckets + ramSummary.thisAlloc - ramSummary.total
      };
      bucketRamGuageConfig.topLeft = {
        name: 'total allocated',
        value: ramSummary.otherBuckets + ramSummary.thisAlloc
      };
    }
    return bucketRamGuageConfig;
  }

  function getGuageConfig(summary) {
    var guageConfig = {};

    guageConfig.topRight = {
      name: 'total cluster storage',
      value: summary.total
    };
    guageConfig.items = [{
      name: 'other buckets',
      value: summary.otherBuckets
    }, {
      name: 'this bucket',
      value: summary.thisBucket
    }];

    guageConfig.bottomLeft = {
      name: 'remaining',
      value: summary.total - summary.otherData - summary.thisBucket - summary.otherBuckets
    };

    return guageConfig;
  }

})(window.rxjs);
