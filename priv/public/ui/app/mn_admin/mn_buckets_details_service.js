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

import mnTasksDetails from "/ui/app/components/mn_tasks_details.js";
import mnCompaction from "/ui/app/components/mn_compaction.js";

export default 'mnBucketsDetailsService';

angular
  .module('mnBucketsDetailsService', [mnTasksDetails, mnCompaction])
  .factory('mnBucketsDetailsService', mnBucketsDetailsServiceFcatory);

function mnBucketsDetailsServiceFcatory($http, mnTasksDetails, mnCompaction) {
  var mnBucketsDetailsService = {
    getBucketRamGuageConfig: getBucketRamGuageConfig,
    deleteBucket: deleteBucket,
    flushBucket: flushBucket,
    doGetDetails: doGetDetails,
    getWarmUpTasks: getWarmUpTasks,
    getGuageConfig: getGuageConfig,
    getCompactionTask: getCompactionTask
  };

  var bucketRamGuageConfig = {};
  var guageConfig = {};

  return mnBucketsDetailsService;

  function getWarmUpTasks(bucket, tasks) {
    if (!bucket || !tasks) {
      return;
    }
    return _.filter(tasks.tasksWarmingUp, function (task) {
      var isNeeded = task.bucket === bucket.name;
      if (isNeeded) {
        task.hostname = _.find(bucket.nodes, function (node) {
          return node.otpNode === task.node;
        }).hostname;
      }
      return isNeeded;
    });
  }

  function getCompactionTask(bucket) {
    return mnTasksDetails.get().then(function (tasks) {
      var rv = {};
      rv.thisBucketCompactionTask = _.find(tasks.tasks, function (task) {
        return task.type === 'bucket_compaction' && task.bucket === bucket.name;
      });
      if (rv.thisBucketCompactionTask && !!rv.thisBucketCompactionTask.cancelURI) {
        rv.disableCancel = !!mnCompaction.getStartedCompactions()[rv.thisBucketCompactionTask.cancelURI];
      } else {
        rv.disableCompact = !!(mnCompaction.getStartedCompactions()[bucket.controllers.compactAll] || rv.thisBucketCompactionTask);
      }
      return rv
    });
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
    }, {
      name: 'available',
      value: ramSummary.total - ramSummary.otherBuckets - ramSummary.thisAlloc
    }];

    if (bucketRamGuageConfig.items[2].value < 0) {
      bucketRamGuageConfig.items[1].value = ramSummary.total - ramSummary.otherBuckets;
      bucketRamGuageConfig.items[2] = {
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

  function getGuageConfig(total, thisBucket, otherBuckets, otherData) {
    var free = total - otherData - thisBucket - otherBuckets;

    guageConfig.topRight = {
      name: 'total cluster storage',
      value: total
    };
    guageConfig.items = [{
      name: 'other buckets',
      value: otherBuckets
    }, {
      name: 'this bucket',
      value: thisBucket
    }, {
      name: 'available',
      value: free
    }];

    return guageConfig;
  }

  function deleteBucket(bucket) {
    return $http({
      method: 'DELETE',
      url: bucket.uri
    });
  }
  function flushBucket(bucket) {
    return $http({
      method: 'POST',
      url: bucket.controllers.flush
    });
  }
  function doGetDetails(bucket) {
    return $http({
      method: 'GET',
      url: bucket.uri + "&basic_stats=true&skipMap=true"
    }).then(function (resp) {
      return resp.data;
    });
  }
}
