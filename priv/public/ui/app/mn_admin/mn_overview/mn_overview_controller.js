(function () {
  "use strict";

  angular.module('mnOverview', [
    'mnServersService',
    'mnBarUsage',
    'mnPlot',
    'mnBucketsService',
    'mnPoll',
    'ui.bootstrap',
    'mnElementCrane',
    'mnDropdown',
    'mnPromiseHelper',
    'mnXDCRService',
    'mnHelper',
    'mnPoolDefault'
  ]).controller('mnOverviewController', mnOverviewController);

  function mnOverviewController($scope, $rootScope, mnBucketsService, mnServersService, mnPoller, mnPromiseHelper, mnHelper, mnXDCRService, permissions, pools, mnPoolDefault) {
    var vm = this;

    vm.getEndings = mnHelper.getEndings;
    vm.isIPv6 = pools.isIPv6;
    vm.nodeEncryption = mnPoolDefault.export.thisNode.nodeEncryption;

    activate();

    function activate() {
      new mnPoller($scope, function () {
        return mnServersService.getServicesStatus(mnPoolDefault.export.isEnterprise);
      })
        .reloadOnScopeEvent("nodesChanged")
        .subscribe("nodes", vm)
        .cycle();
    }
  }
})();
