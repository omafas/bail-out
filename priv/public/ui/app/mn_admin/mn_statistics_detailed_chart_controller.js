/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnStatisticsDetailedChartController;

mnStatisticsDetailedChartController.$inject = ["$scope", "$timeout", "$state", "chart", "items", "mnStatisticsNewService", "mnStatisticsNewScope"];
function mnStatisticsDetailedChartController($scope, $timeout, $state, chart, items, mnStatisticsNewService, mnStatisticsNewScope) {
  var vm = this;
  vm.chart = Object.assign({}, chart, {size: "extra"});

  vm.items = items;
  vm.onSelectZoom = onSelectZoom;
  vm.bucket = $state.params.commonBucket;
  vm.zoom = $state.params.scenarioZoom !== "minute" ? $state.params.scenarioZoom : "hour";
  vm.node = $state.params.statsHostname;
  vm.options = {showFocus: true, showTicks: true, showLegends: true};

  mnStatisticsNewScope.$broadcast("mnStatsCancelTimer");

  mnStatisticsNewService.mnAdminStatsPoller.heartbeat.pause();
  vm.statsPoller = mnStatisticsNewService.createStatsPoller($scope);
  vm.statsPoller.heartbeat.setInterval(mnStatisticsNewService.defaultZoomInterval(vm.zoom));

  function onSelectZoom(selectedOption) {
    vm.options.showFocus = selectedOption !== "minute";
    let interval = mnStatisticsNewService.defaultZoomInterval(selectedOption);
    vm.statsPoller.heartbeat.setInterval(interval);
    vm.reloadChartDirective = true;
    $timeout(function () {
      vm.reloadChartDirective = false;
    });
  }

  $scope.$on("$destroy", function () {
    mnStatisticsNewService.mnAdminStatsPoller.heartbeat.resume();
  });

}
