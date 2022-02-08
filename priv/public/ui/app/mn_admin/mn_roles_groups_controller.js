/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import _ from "lodash";
import uiSelect from "ui-select";

import mnHelper from "../components/mn_helper.js";
import mnPromiseHelper from "../components/mn_promise_helper.js";
import mnPoll from "../components/mn_poll.js";
import mnSpinner from "../components/directives/mn_spinner.js";
import mnMainSpinner from "../components/directives/mn_main_spinner.js";
import mnEqual from "../components/directives/mn_validation/mn_equal.js";
import mnFilters from "../components/mn_filters.js";
import mnAutocompleteOff from "../components/directives/mn_autocomplete_off.js";
import mnFocus from "../components/directives/mn_focus.js";
import mnSearch from "../components/directives/mn_search/mn_search_directive.js";
import mnUserRolesSelect from "../components/directives/mn_user_roles_select_controller.js";

import mnUserRolesService from "./mn_user_roles_service.js";

import mnRolesGroupsDeleteDialogController from "./mn_roles_groups_delete_dialog_controller.js";
import mnRolesGroupsAddDialogController from "./mn_roles_groups_add_dialog_controller.js";
import mnRolesGroupsAddDialogTemplate from "./mn_roles_groups_add_dialog.html";
import mnRolesGroupsDeleteDialogTemplate from "./mn_roles_groups_delete_dialog.html";

export default "mnRolesGroups";

angular
  .module("mnRolesGroups", [
    uiSelect,
    mnHelper,
    mnPromiseHelper,
    mnPoll,
    mnSpinner,
    mnMainSpinner,
    mnEqual,
    mnFilters,
    mnAutocompleteOff,
    mnFocus,
    mnUserRolesSelect,
    mnUserRolesService,
    mnSearch
  ])
  .controller("mnRolesGroupsController", ["$scope", "$uibModal", "mnPromiseHelper", "mnUserRolesService", "mnPoller", "mnHelper", "$state", mnRolesGroupsController])
  .controller("mnRolesGroupsDeleteDialogController", mnRolesGroupsDeleteDialogController)
  .controller("mnRolesGroupsAddDialogController", mnRolesGroupsAddDialogController);

function mnRolesGroupsController($scope, $uibModal, mnPromiseHelper, mnUserRolesService, mnPoller, mnHelper, $state) {
  var vm = this;

  vm.addRolesGroup = addRolesGroup;
  vm.deleteRolesGroup = deleteRolesGroup;
  vm.editRolesGroup = editRolesGroup;

  vm.filterField = $state.params.substr;

  vm.stateParams = $state.params;

  vm.pageSize = $state.params.pageSize;
  vm.pageSizeChanged = pageSizeChanged;
  vm.sortByChanged = sortByChanged;
  vm.isOrderBy = isOrderBy;
  vm.isDesc = isDesc;
  vm.getRoleParams = getRoleParams;

  activate();

  function isOrderBy(sortBy) {
    return sortBy === $state.params.sortBy;
  }

  function isDesc() {
    return $state.params.order === "desc";
  }

  function pageSizeChanged(selectedOption) {
    $state.go('.', {
      pageSize: selectedOption
    });
  }

  function sortByChanged(sortBy) {
    $state.go('.', {
      order: $state.params.sortBy != sortBy ? "asc" :
        $state.params.order === "asc" ? "desc" : "asc",
      sortBy: sortBy
    })
  }

  function getRoleParams(role) {
    return mnUserRolesService.getRoleParams(vm.rolesByRole, role);
  }

  function activate() {
    $scope.$watch('rolesGroupsCtl.filterField', _.debounce(function () {
      $state.go('.', {
        substr: vm.filterField || undefined
      })
    }, 500, {leading: true}), true);

    $scope.$watchGroup(["rolesGroupsCtl.stateParams.order",
                        "rolesGroupsCtl.stateParams.sortBy",
                        "rolesGroupsCtl.stateParams.substr"], _.debounce(function () {
                          $scope.$broadcast("reloadRolesGroupsPoller");
                        }, 500, {leading: true}));

    mnHelper.initializeDetailsHashObserver(vm, 'openedRolesGroups', '.');

    mnUserRolesService.getRoles().then(roles => {
      vm.rolesByRole = roles.rolesByRole;
    });

    new mnPoller($scope, function () {
      return mnUserRolesService.getRolesGroupsState($state.params);
    })
        .subscribe("state", vm)
        .setInterval(10000)
        .reloadOnScopeEvent("reloadRolesGroupsPoller")
        .cycle();
  }

  function editRolesGroup(rolesGroup) {
    $uibModal.open({
      template: mnRolesGroupsAddDialogTemplate,
      controller: 'mnRolesGroupsAddDialogController as rolesGroupsAddDialogCtl',
      resolve: {
        rolesGroup: mnHelper.wrapInFunction(rolesGroup)
      }
    });
  }
  function addRolesGroup() {
    $uibModal.open({
      template: mnRolesGroupsAddDialogTemplate,
      controller: 'mnRolesGroupsAddDialogController as rolesGroupsAddDialogCtl',
      resolve: {
        rolesGroup: mnHelper.wrapInFunction(undefined)
      }
    });
  }
  function deleteRolesGroup(rolesGroup) {
    $uibModal.open({
      template: mnRolesGroupsDeleteDialogTemplate,
      controller: 'mnRolesGroupsDeleteDialogController as rolesGroupsDeleteDialogCtl',
      resolve: {
        rolesGroup: mnHelper.wrapInFunction(rolesGroup)
      }
    });
  }
}
