/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnBucketsDetailsController;

function mnBucketsDetailsController($scope, mnBucketsDetailsService, mnPromiseHelper, mnSettingsAutoCompactionService, mnCompaction, $uibModal, mnBucketsDetailsDialogService, mnPoller, mnHelper, permissions) {
  var vm = this;
  vm.editBucket = editBucket;
  vm.deleteBucket = deleteBucket;
  vm.flushBucket = flushBucket;
  vm.registerCompactionAsTriggeredAndPost = registerCompactionAsTriggeredAndPost;
  vm.getGuageConfig = getGuageConfig;
  vm.getEndings = mnHelper.getEndings;

  var compactionTasks;
  var warmUpTasks;

  activate();


  function activate() {
    if (permissions.cluster.tasks.read) {
      compactionTasks = new mnPoller($scope, function () {
        return mnBucketsDetailsService.getCompactionTask($scope.bucket);
      })
        .subscribe("compactionTasks", vm)
        .reloadOnScopeEvent("mnTasksDetailsChanged")
        .cycle();
    }

    $scope.$watch('bucket', function () {
      permissions.cluster.tasks.read && compactionTasks.reload();
      mnPromiseHelper(vm, mnBucketsDetailsService.doGetDetails($scope.bucket)).applyToScope("bucketDetails");
    });

    if (permissions.cluster.tasks.read) {
      $scope.$watchGroup(['bucket', 'adminCtl.tasks'], function (values) {
        vm.warmUpTasks = mnBucketsDetailsService.getWarmUpTasks(values[0], values[1]);
      });
    }
  }

  $scope.$watch("bucketsDetailsCtl.bucketDetails", getBucketRamGuageConfig);
  function getBucketRamGuageConfig(details) {
    if (!details) {
      return;
    }
    let ram = details.basicStats.storageTotals.ram;
    vm.bucketRamGuageConfig = mnBucketsDetailsService.getBucketRamGuageConfig({
      total: ram ? (ram.quotaTotalPerNode * details.nodes.length) : 0,
      thisAlloc: details.quota.ram,
      otherBuckets: ram ? (ram.quotaUsedPerNode * details.nodes.length - details.quota.ram) : 0
    });
  }
  function getGuageConfig(details) {
    if (!details) {
      return;
    }
    let hdd = details.basicStats.storageTotals.hdd;
    return mnBucketsDetailsService.getGuageConfig(
      hdd ? hdd.total : 0,
      details.basicStats.diskUsed,
      hdd ? (hdd.usedByData - details.basicStats.diskUsed) : 0,
      hdd ? (hdd.used - hdd.usedByData) : 0
    );
  }
  function editBucket() {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_buckets_details_dialog.html',
      controller: 'mnBucketsDetailsDialogController as bucketsDetailsDialogCtl',
      resolve: {
        bucketConf: function () {
          return mnBucketsDetailsDialogService.reviewBucketConf(vm.bucketDetails);
        },
        autoCompactionSettings: function () {
          if (vm.bucketDetails.autoCompactionSettings === undefined) {
            return;
          }
          return !vm.bucketDetails.autoCompactionSettings ?
            mnSettingsAutoCompactionService.getAutoCompaction(true) :
            mnSettingsAutoCompactionService.prepareSettingsForView(vm.bucketDetails);
        }
      }
    });
  }
  function deleteBucket(bucket) {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_buckets_delete_dialog.html',
      controller: 'mnBucketsDeleteDialogController as bucketsDeleteDialogCtl',
      resolve: {
        bucket: function () {
          return bucket;
        }
      }
    });
  }
  function flushBucket(bucket) {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_buckets_flush_dialog.html',
      controller: 'mnBucketsFlushDialogController as bucketsFlushDialogCtl',
      resolve: {
        bucket: function () {
          return bucket;
        }
      }
    });
  }
  function registerCompactionAsTriggeredAndPost(url, disableButtonKey) {
    vm.compactionTasks[disableButtonKey] = true;
    mnPromiseHelper(vm, mnCompaction.registerAsTriggeredAndPost(url))
      .onSuccess(function () {
        compactionTasks.reload()
      });
  };
}
