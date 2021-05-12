/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import mnStatsDesc from "./mn_statistics_description.js";

export default mnGsiFooterController;

function mnGsiFooterController($scope, $rootScope, $state, mnStatisticsNewService, mnPoolDefault, mnPermissions, $timeout) {
  var vm = this;

  vm.onSelectBucket = onSelectBucket;

  vm.getLatestStat = mnPoolDefault.export.compat.atLeast70 ? getLatestStat70 : getLatestStat;

  vm.currentBucket = $state.params.footerBucket ||
    (mnPermissions.export.bucketNames['.stats!read'] &&
     mnPermissions.export.bucketNames['.stats!read'][0]);

  vm.mnGSIFooterStatsPoller = mnStatisticsNewService.createStatsPoller($scope);

  vm.stats = $scope.stats || (mnPoolDefault.export.compat.atLeast70 ? [
    '@index.index_memory_quota','@index.index_memory_used_total',
    '@index-.index_num_rows_returned','@index-.index_fragmentation',
    '@index-.index_data_size','@index-.index_disk_size'
  ] : [
    '@index.index_memory_quota','@index.index_memory_used',
    '@index.index_remaining_ram','@index.index_ram_percent',
    '@index-.index/num_rows_returned','@index-.index/fragmentation',
    '@index-.index/data_size','@index-.index/disk_size']);

  var config = {
    bucket: vm.currentBucket,
    node: "all",
    zoom: 3000,
    step: 1,
    stats: vm.stats
  };

  activate();

  function activate() {
    vm.mnGSIFooterStatsPoller.subscribeUIStatsPoller(config, $scope);
  }

  function doGetLatestStat70(statName) {
    var stats = $scope.mnUIStats && $scope.mnUIStats.stats;
    return stats && stats[statName]  && stats[statName].aggregate &&
      Number(stats[statName].aggregate.values.slice().reverse().find(stat => stat != null)[1]);
  }

  function getLatestStat70(statName) {
    switch (statName) {
    case "@index.index_remaining_ram":
      let quota = doGetLatestStat70("@index.index_memory_quota");
      let used = doGetLatestStat70("@index.index_memory_used_total");
      return used > quota ? 0 : quota - used;
    case "@index.index_ram_percent":
      return doGetLatestStat70("@index.index_memory_used_total") /
        doGetLatestStat70("@index.index_memory_quota") * 100;
    default:
      return doGetLatestStat70(mnStatsDesc.mapping65(statName));
    }
  }

  function getLatestStat(statName) {
    statName = statName.split(".").pop();
    return $scope.mnUIStats &&
      $scope.mnUIStats.stats[statName] &&
      $scope.mnUIStats.stats[statName].aggregate.slice().reverse().find(stat => stat != null);
  }

  function onSelectBucket(selectedOption) {
    //reload ng-controller in the template
    $rootScope.destroyGsiFooter = true;
    $timeout(() => {
      $state.go(".", {footerBucket: selectedOption}).then(function () {
        $rootScope.destroyGsiFooter = false;
      });
    }, 0);
  }
}
