/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnPoolDefault from "/ui/app/components/mn_pool_default.js";
import mnPermissions from "/ui/app/components/mn_permissions.js";
import mnSettingsClusterService from "/ui/app/mn_admin/mn_settings_cluster_service.js";
import mnPeriod from "/ui/app/components/directives/mn_period/mn_period.js";

export default "mnAutoCompactionForm";

angular
  .module('mnAutoCompactionForm', [
    mnPeriod,
    mnPermissions,
    mnPoolDefault,
    mnPromiseHelper,
    mnSettingsClusterService
  ])
  .directive('mnAutoCompactionForm', mnAutoCompactionFormDirective);

function mnAutoCompactionFormDirective($http, daysOfWeek, mnPermissions, mnPoolDefault, mnPromiseHelper, mnSettingsClusterService) {
  var mnAutoCompactionForm = {
    restrict: 'A',
    scope: {
      autoCompactionSettings: '=',
      validationErrors: '=',
      isBucketsSettings: '='
    },
    isolate: false,
    replace: true,
    templateUrl: 'app/components/directives/mn_auto_compaction_form/mn_auto_compaction_form.html',
    controller: controller
  };

  function controller($scope) {
    $scope.daysOfWeek = daysOfWeek;
    $scope.rbac = mnPermissions.export;
    $scope.poolDefault = mnPoolDefault.export;
    $scope.maybeDisableTimeInterval = maybeDisableTimeInterval;
    $scope.props = {};

    if ($scope.rbac.cluster.settings.indexes.read) {
      mnPromiseHelper($scope, mnSettingsClusterService.getIndexSettings())
        .applyToScope(function (indexSettings) {
          $scope.indexSettings = indexSettings;
          maybeDisableTimeInterval();
        });
    }

    function isFragmentationProvided(value) {
      return (value.percentageFlag && value.percentage) ||
        (value.sizeFlag && value.size);
    }

    function maybeDisableTimeInterval() {
      $scope.props.isFragmentationProvided =
        isFragmentationProvided($scope.autoCompactionSettings.databaseFragmentationThreshold) ||
        isFragmentationProvided($scope.autoCompactionSettings.viewFragmentationThreshold);
      if (!$scope.props.isFragmentationProvided) {
        $scope.autoCompactionSettings.allowedTimePeriodFlag = false;
      }
    }
  }

  return mnAutoCompactionForm;
}
