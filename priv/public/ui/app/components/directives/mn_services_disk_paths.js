/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import mnClusterConfigurationService from "/ui/app/mn_wizard/mn_cluster_configuration/mn_cluster_configuration_service.js";

export default "mnServicesDiskPaths";

angular
  .module("mnServicesDiskPaths", [mnClusterConfigurationService])
  .directive("mnServicesDiskPaths", mnServicesDiskPathsDirective);

function mnServicesDiskPathsDirective() {
  var mnServicesDiskPaths = {
    restrict: "AE",
    scope: {
      config: "=",
      postDiskStorageErrors: "=?",
      isEnterprise: "=",
      isDisabled: "=?"
    },
    templateUrl: "app/components/directives/mn_services_disk_paths.html",
    controller: controller,
    controllerAs: "mnCtl"
  };

  return mnServicesDiskPaths;

  function controller($scope, mnClusterConfigurationService) {
    var vm = this;
    vm.onDbPathChange = onDbPathChange;
    vm.onIndexPathChange = onIndexPathChange;
    vm.onEventingPathChange = onEventingPathChange;
    vm.onCbasDirsChange = onCbasDirsChange;
    vm.addCbasPath = addCbasPath;

    activate();

    function activate() {
      vm.onDbPathChange();
      vm.onIndexPathChange();
      vm.onEventingPathChange();
      if ($scope.config.cbasDirs) {
        $scope.config.cbasDirs.forEach(function (path, index) {
          vm.onCbasDirsChange(index);
        });
      }
    }

    function onDbPathChange() {
      vm.dbPathTotal =
        mnClusterConfigurationService.lookup(
          $scope.config.dbPath,
          $scope.config.selfConfig.preprocessedAvailableStorage);
    }
    function onIndexPathChange() {
      vm.indexPathTotal =
        mnClusterConfigurationService.lookup(
          $scope.config.indexPath,
          $scope.config.selfConfig.preprocessedAvailableStorage);
    }
    function onEventingPathChange() {
      vm.eventingPathTotal =
        mnClusterConfigurationService.lookup(
          $scope.config.eventingPath,
          $scope.config.selfConfig.preprocessedAvailableStorage);
    }
    function onCbasDirsChange(index) {
      vm["cbasDirsTotal" + index] =
        mnClusterConfigurationService.lookup(
          $scope.config.cbasDirs[index],
          $scope.config.selfConfig.preprocessedAvailableStorage);
    }
    function addCbasPath() {
      var last = $scope.config.cbasDirs.length-1;
      vm["cbasDirsTotal" + (last + 1)] = vm["cbasDirsTotal" + last];
      $scope.config.cbasDirs.push($scope.config.cbasDirs[last]);
    }
  }

}
