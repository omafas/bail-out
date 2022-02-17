/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import _ from "lodash";
import mnStatisticsDescription from "./mn_statistics_description.js";
import mnGsiItemDetailsTemplate from "./mn_gsi_item_details.html";
import mnGsiDropConfirmDialogTemplate from "./mn_gsi_drop_confirm_dialog.html";

export {mnGsiItemController, mnGsiItemStatsController, mnGsiItemDetails};

mnGsiItemStatsController.$inject = ["$scope"];
function mnGsiItemStatsController($scope) {
  var vm = this;
  vm.zoom = "minute";
  vm.onSelectZoom = onSelectZoom;

  activate();

  function onSelectZoom() {
    activate();
  }

  function activate() {
    var row = $scope.row;
    vm.hosts = row.hosts.join(', ');
  }
}

mnGsiItemController.$inject = ["$scope", "mnGsiService", "mnStatisticsNewService", "mnPoolDefault", "mnPermissions"];
function mnGsiItemController($scope, mnGsiService, mnStatisticsNewService, mnPoolDefault, mnPermissions) {
  var row = $scope.row;

  //check permissions
  let interestingPermissions = row.collection ?
      mnPermissions.getPerCollectionPermissions(row.bucket, row.scope, row.collection) :
      mnPermissions.getPerScopePermissions(row.bucket, row.scope);
  interestingPermissions.forEach(mnPermissions.set);
  mnPermissions.throttledCheck();


  //get stats
  let isAtLeast70 = mnPoolDefault.export.compat.atLeast70;

  let perItemStats = [
    "@index-.@items.index_num_requests", "@index-.@items.index_resident_percent",
    "@index-.@items.index_items_count", "@index-.@items.index_data_size",
    "@index-.@items.index_num_docs_pending_and_queued"
  ];

  let uiStatNames = perItemStats.map(
    stat => mnStatisticsDescription.mapping70(stat).split(".").pop());

  if (!isAtLeast70) {
    perItemStats = perItemStats.map(mnStatisticsDescription.mapping70);
  }

  let getStatSamples = isAtLeast70 ? getStatSamples70 : getStatSamplesPre70;


  $scope.mnGsiTableCtl.mnGsiStatsPoller.subscribeUIStatsPoller({
    bucket: row.bucket,
    scope: row.scope,
    collection: row.collection,
    node: $scope.nodeName || "all",
    zoom: 3000,
    step: 1,
    stats: perItemStats,
    items: {
      index: isAtLeast70 ?
        row.index : ("index/" + row.index + "/")
    }
  }, $scope);

  let permissions = $scope.rbac.cluster.collection[row.bucket + ':.:.'];
  if (permissions && permissions.stats.read) {
    $scope.$watch("mnUIStats", updateValues);
    $scope.$watch("row", updateValues);
  }

  function getIndexStatName(statName) {
    return 'index/' + $scope.row.index + '/' + statName;
  }

  function getStats(statName) {
    let stats = $scope.mnUIStats && $scope.mnUIStats && $scope.mnUIStats.stats;
    return stats && stats[statName] && stats[statName][$scope.nodeName || "aggregate"];
  }

  function getStatSamples70(statName) {
    statName = mnStatisticsDescription.mapping65("@index-.@items." + statName);
    let stats = getStats(statName);
    let last = stats && stats.values[stats.values.length - 1];
    let val = last && last[1];
    val = val ? Number(val) : !!val;
    return val;
  }

  function getStatSamplesPre70(statName) {
    let stats = getStats(getIndexStatName(statName));
    return stats && stats.slice().reverse().find(stat => stat != null);
  }

  function updateValues() {
    uiStatNames.forEach(statName => $scope.row[statName] = getStatSamples(statName))
  }


  $scope.$on("$destroy", onDestroy);


  function onDestroy() {
    interestingPermissions.forEach(mnPermissions.remove);
  }

}

function mnGsiItemDetails() {
  var mnGsiItemDetails = {
    restrict: 'E',
    scope: {
      row: "=",
      rbac: "=",
      pools: "=",
      nodeName: "@?"
    },
    controller: mnGsiItemDetailsController,
    controllerAs: "mnGsiItemDetailsCtl",
    template: mnGsiItemDetailsTemplate
  };

  return mnGsiItemDetails;

  function mnGsiItemDetailsController($rootScope, $scope, mnGsiService, $uibModal, $filter, mnPromiseHelper, mnAlertsService) {
    var vm = this;
    vm.dropIndex = dropIndex;
    vm.getFormattedScanTime = getFormattedScanTime;
    var row = $scope.row;

    vm.keyspace = row.bucket + ":" + row.scope + (row.collection ? (":" + row.collection) : "");

    function getFormattedScanTime(row) {
      if (row && row.lastScanTime != 'NA')
        return $filter('date')(Date.parse(row.lastScanTime), 'hh:mm:ss a, d MMM, y', 'UTC');
      else
        return 'NA';
    }

    function dropIndex(row, dropReplicaOnly) {
      var scope = $rootScope.$new();
      scope.partitioned = row.partitioned;
      $uibModal.open({
        windowClass: "z-index-10001",
        backdrop: 'static',
        template: mnGsiDropConfirmDialogTemplate,
        scope: scope
      }).result.then(function () {
        row.awaitingRemoval = true;
        mnPromiseHelper(vm, mnGsiService.postDropIndex(row, dropReplicaOnly))
          .showGlobalSpinner()
          .catchErrors(function (resp) {
            if (!resp) {
              return;
            } else if (_.isString(resp)) {
              mnAlertsService.formatAndSetAlerts(resp.data, "error", 4000);
            } else if (resp.errors && resp.errors.length) {
              mnAlertsService.formatAndSetAlerts(_.map(resp.errors, "msg"), "error", 4000);
            }
            row.awaitingRemoval = false;
          })
          .showGlobalSuccess("Index dropped successfully!")
          .catchGlobalErrors("Error dropping index.");
      });
    }

  }
}
