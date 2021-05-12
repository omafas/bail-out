/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnBucketsListItemController

function mnBucketsListItemController(mnServersService, $scope) {
  var vm = this;
  vm.getWarmUpProgress = getWarmUpProgress;
  vm.getResidentRatio = getResidentRatio;

  $scope.$watch("bucket", function (bucket) {
    vm.bucketStatus = mnServersService.addNodesByStatus(bucket.nodes);
  }, true);

  function getResidentRatio(bucket) {
    var items = bucket.basicStats.itemCount;
    var activeResident = bucket.basicStats.vbActiveNumNonResident;
    if (items === 0) {
      return 100;
    }
    if (items < activeResident) {
      return 0;
    }
    return (items - activeResident) * 100 / items;
  }

  function getWarmUpProgress(bucket, tasks) {
    if (!bucket || !tasks) {
      return false;
    }
    var totalPercent = 0;
    var exists = false;
    tasks.tasksWarmingUp.forEach(function (task) {
      if (task.bucket === bucket.name) {
        exists = true;
        if (!Number(task.stats.ep_warmup_estimated_key_count) ||
            !Number(task.stats.ep_warmup_estimated_value_count)) {
          return;
        }
        var message = task.stats.ep_warmup_state;
        switch (message) {
        case "loading keys":
          totalPercent += ((Number(task.stats.ep_warmup_key_count) || 1) /
                           (Number(task.stats.ep_warmup_estimated_key_count) || 1)) * 100;
          break;
        case "loading data":
          totalPercent += ((Number(task.stats.ep_warmup_value_count) || 1) /
                           (Number(task.stats.ep_warmup_estimated_value_count) || 1)) * 100;
          break;
        default:
          return 100;
        }
      }
    });

    return exists ? (totalPercent / bucket.nodes.length) : false;
  }

}
