/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiBootstrap from "/ui/web_modules/angular-ui-bootstrap.js";

import mnBucketsDeleteDialogController from "./mn_buckets_delete_dialog_controller.js";
import mnBucketsDetailsController from "./mn_buckets_details_controller.js";
import mnBucketsDetailsDialogController from "./mn_buckets_details_dialog_controller.js";
import mnBucketsFlushDialogController from "./mn_buckets_flush_dialog_controller.js";
import mnBucketsListItemController from "./mn_buckets_list_item_controller.js";
import mnBucketsList from "./mn_buckets_list_directive.js";
import mnBucketsDetailsDialogService from "./mn_buckets_details_dialog_service.js";

import mnHelper from "/ui/app/components/mn_helper.js";
import mnBarUsage from "/ui/app/components/directives/mn_bar_usage/mn_bar_usage.js";
import mnBucketsForm from "/ui/app/components/directives/mn_buckets_form/mn_buckets_form.js";
import mnSearch from "/ui/app/components/directives/mn_search/mn_search_directive.js";
import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnPoll from "/ui/app/components/mn_poll.js";
import mnPoolDefault from "/ui/app/components/mn_pool_default.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";
import mnFilters from "/ui/app/components/mn_filters.js";
import mnTasksDetails from "/ui/app/components/mn_tasks_details.js";
import mnWarmupProgress from "/ui/app/components/directives/mn_warmup_progress/mn_warmup_progress.js";
import mnElementCrane from "/ui/app/components/directives/mn_element_crane/mn_element_crane.js";
import mnSortableTable from "/ui/app/components/directives/mn_sortable_table.js";

export default 'mnBuckets';

angular
  .module('mnBuckets', [
    mnHelper,
    uiBootstrap,
    mnBucketsDetailsDialogService,
    mnBarUsage,
    mnBucketsForm,
    mnPromiseHelper,
    mnPoll,
    mnPoolDefault,
    mnSpinner,
    mnMainSpinner,
    mnSearch,
    mnFilters,
    mnTasksDetails,
    mnWarmupProgress,
    mnElementCrane,
    mnSortableTable
  ])
  .config(configure)
  .controller('mnBucketsController', mnBucketsController)
  .controller('mnBucketsDeleteDialogController', mnBucketsDeleteDialogController)
  .controller('mnBucketsDetailsController', mnBucketsDetailsController)
  .controller('mnBucketsDetailsDialogController', mnBucketsDetailsDialogController)
  .controller('mnBucketsFlushDialogController', mnBucketsFlushDialogController)
  .controller('mnBucketsListItemController', mnBucketsListItemController)
  .directive('mnBucketsList', mnBucketsList);

function configure($stateProvider) {
  $stateProvider
    .state('app.admin.buckets', {
      url: '/buckets?openedBucket',
      params: {
        openedBucket: {
          array: true,
          dynamic: true
        }
      },
      views: {
        "main@app.admin": {
          controller: 'mnBucketsController as bucketsCtl',
          templateUrl: 'app/mn_admin/mn_buckets.html'
        },
        "details@app.admin.buckets": {
          templateUrl: 'app/mn_admin/mn_buckets_details.html',
          controller: 'mnBucketsDetailsController as bucketsDetailsCtl'
        },
        "item@app.admin.buckets": {
          templateUrl: 'app/mn_admin/mn_buckets_list_item.html',
          controller: 'mnBucketsListItemController as bucketsItemCtl'
        }
      },
      data: {
        title: "Buckets",
        permissions: "cluster.bucket['.'].settings.read"
      }
    });
}

function mnBucketsController($scope, mnPoolDefault, mnPromiseHelper, $uibModal, $rootScope, $interval) {
  var vm = this;

  var poolDefault = mnPoolDefault.latestValue();

  vm.isCreateNewDataBucketDisabled = isCreateNewDataBucketDisabled;
  vm.isBucketCreationWarning = isBucketCreationWarning;
  vm.isMaxBucketCountWarning = isMaxBucketCountWarning;
  vm.areThereCreationWarnings = areThereCreationWarnings;
  vm.addBucket = addBucket;

  vm.maxBucketCount = poolDefault.value.maxBucketCount;

  activate();

  function activate() {
    var pull = $interval(function () {
      $rootScope.$broadcast("reloadBucketStats");
    }, 3000);

    $rootScope.$broadcast("reloadBucketStats");

    $scope.$on('$destroy', function () {
      $interval.cancel(pull);
    });
  }

  function isCreateNewDataBucketDisabled() {
    return !$scope.buckets || !$scope.buckets.details || areThereCreationWarnings();
  }
  function isBucketCreationWarning() {
    return poolDefault.value.rebalancing;
  }
  function isMaxBucketCountWarning() {
    return (($scope.buckets && $scope.buckets.details) || []).length >= poolDefault.value.maxBucketCount;
  }
  function areThereCreationWarnings() {
    return isMaxBucketCountWarning() || isBucketCreationWarning();
  }
  function addBucket() {
    mnPromiseHelper(vm, mnPoolDefault.getFresh())
      .onSuccess(function (poolDefault) {
        let ram = poolDefault.storageTotals.ram;
        if (!ram || ram.quotaTotal === ram.quotaUsed) {
          $uibModal.open({
            templateUrl: 'app/mn_admin/mn_bucket_full_dialog.html'
          });
        } else {
          !areThereCreationWarnings() && $uibModal.open({
            templateUrl: 'app/mn_admin/mn_buckets_details_dialog.html',
            controller: 'mnBucketsDetailsDialogController as bucketsDetailsDialogCtl',
            resolve: {
              bucketConf: function (mnBucketsDetailsDialogService) {
                return mnBucketsDetailsDialogService.getNewBucketConf();
              },
              autoCompactionSettings: function (mnSettingsAutoCompactionService) {
                return mnSettingsAutoCompactionService.getAutoCompaction(true);
              }
            }
          });
        }
      });
  }
}
