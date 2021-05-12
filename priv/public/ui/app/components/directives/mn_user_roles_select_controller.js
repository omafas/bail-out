/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import {Subject} from "/ui/web_modules/rxjs.js";
import mnKeyspaceSelectorDowngradeModule from "/ui/app/mn.keyspace.selector.downgrade.module.js"

export default "mnUserRolesSelect";

angular
  .module('mnUserRolesSelect', [mnKeyspaceSelectorDowngradeModule])
  .directive('mnUserRolesSelect', mnUserRolesSelectDirective)
  .directive('mnUserRolesSelectForm', mnUserRolesSelectFormDirective);

function mnUserRolesSelectDirective() {
  var mnUserRolesSelect = {
    restrict: 'E',
    templateUrl: 'app/components/directives/mn_user_roles_select.html',
    controller: mnUserRolesSelectController,
    scope: {
      state: "="
    }
  };

  return mnUserRolesSelect;

  function mnUserRolesSelectController($scope) {

    $scope.toggleWrapper = toggleWrapper;
    $scope.hasSelectedItems = hasSelectedItems;
    $scope.hasSelectedConfigs = hasSelectedConfigs;
    $scope.isRoleDisabled = isRoleDisabled;

    function isRoleDisabled(role) {
      return role.role !== 'admin' && $scope.state.selectedRoles["admin"];
    }

    function toggleWrapper(name) {
      $scope.state.openedWrappers[name] = !$scope.state.openedWrappers[name];
    }

    function hasSelectedItems(name) {
      return $scope.state.folders.find(o => o.name == name).roles.find(o => {
        let groups = ($scope.state.selectedGroupsRoles &&
                      $scope.state.selectedGroupsRoles[o.role]);
        return $scope.state.selectedRoles[o.role] ||
          (groups && groups.length) ||
          hasSelectedConfigs(o.role);
      });
    }

    function hasSelectedConfigs(name) {
      let config = $scope.state.selectedRolesConfigs[name];
      let groupConfigs = Object.values(($scope.state.selectedGroupsRolesConfigs &&
                                        $scope.state.selectedGroupsRolesConfigs[name]) || {});
      return (config && config.length) || groupConfigs.find(v => v.length);
    }

  }
}

function mnUserRolesSelectFormDirective(mnCollectionsServiceDowngrade) {
  var mnUserRolesSelectForm = {
    restrict: 'AE',
    templateUrl: 'app/components/directives/mn_user_roles_select_form.html',
    controller: mnUserRolesSelectFormController,
    scope: {
      item: "=",
      state: "="
    }
  };

  return mnUserRolesSelectForm;

  function mnUserRolesSelectFormController($scope) {
    let mnOnDestroy = new Subject();
    let params = $scope.item.params.map(v => v.split("_")[0]);

    $scope.mnCollectionSelectorService =
      mnCollectionsServiceDowngrade.createCollectionSelector({
        isRolesMode: true,
        component: {mnOnDestroy},
        buckets: $scope.state.parameters[$scope.item.params[0]],
        steps: params
      });

    var rolesConfigs = $scope.state.selectedRolesConfigs[$scope.item.role] || [];
    $scope.state.selectedRolesConfigs[$scope.item.role] = rolesConfigs;

    $scope.submit = submit;
    $scope.del = del;
    $scope.isRoleDisabled = isRoleDisabled;

    $scope.$on("$destroy", function () {
      mnOnDestroy.next();
      mnOnDestroy.complete();
    });

    function isRoleDisabled(role) {
      return role.role !== 'admin' && $scope.state.selectedRoles["admin"];
    }

    function del(cfg) {
      rolesConfigs.splice(rolesConfigs.indexOf(cfg), 1);
    }

    function submit() {
      let result = $scope.mnCollectionSelectorService.stream.result
      let resultValue = result.getValue();
      let isInvalid = Object.keys(resultValue).some(key => {
        if (resultValue[key] == null) {
          $scope.mnCollectionSelectorService.stream.doFocus.next(key);
          return true;
        }
      });
      if (isInvalid) {
        return;
      }
      let cfg = Object.values(resultValue).map(v => v.value).join(":");
      if (rolesConfigs.includes(cfg)) {
        return;
      }
      rolesConfigs.unshift(cfg);
      $scope.mnCollectionSelectorService.reset();
    }
  }
}
