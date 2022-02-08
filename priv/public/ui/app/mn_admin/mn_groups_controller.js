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
import uiBootstrap from "angular-ui-bootstrap";

import mnPromiseHelper from "../components/mn_promise_helper.js";
import mnHelper from "../components/mn_helper.js";
import mnFilters from "../components/mn_filters.js";
import mnSpinner from "../components/directives/mn_spinner.js";
import mnPoll from "../components/mn_poll.js";
import mnPoolDefault from "../components/mn_pool_default.js";
import mnAlertsService from "../components/mn_alerts.js";
import mnDragAndDrop from "../components/directives/mn_drag_and_drop.js";
import mnElementCrane from "../components/directives/mn_element_crane/mn_element_crane.js";

import mnGroupsDeleteDialogController from "./mn_groups_delete_dialog_controller.js";
import mnGroupsGroupDialogController from "./mn_groups_group_dialog_controller.js";
import mnGroupsService from "./mn_groups_service.js";

export default 'mnGroups';

angular
  .module('mnGroups', [
    mnGroupsService,
    mnSpinner,
    mnHelper,
    mnPoll,
    mnPromiseHelper,
    mnDragAndDrop,
    uiBootstrap,
    mnFilters,
    mnAlertsService,
    mnPoolDefault,
    mnElementCrane
  ])
  .config(["$stateProvider", configure])
  .controller('mnGroupsController', ["$scope", "$uibModal", "mnGroupsService", "mnPromiseHelper", "mnHelper", "$window", "mnAlertsService", mnGroupsController])
  .controller('mnGroupsDeleteDialogController', mnGroupsDeleteDialogController)
  .controller('mnGroupsGroupDialogController', mnGroupsGroupDialogController);

function configure($stateProvider) {
  $stateProvider
    .state('app.admin.groups', {
      url: '/groups',
      views: {
        "main@app.admin": {
          templateUrl: 'app/mn_admin/mn_groups.html',
          controller: 'mnGroupsController as groupsCtl'
        }
      },
      data: {
        enterprise: true,
        permissions: "cluster.server_groups.read",
        title: "Server Groups",
        parent: {name: 'Servers', link: 'app.admin.servers.list'}
      }
    });
}

function mnGroupsController($scope, $uibModal, mnGroupsService, mnPromiseHelper, mnHelper, $window, mnAlertsService) {
  var vm = this;

  vm.createGroup = createGroup;
  vm.deleteGroup = deleteGroup;
  vm.applyChanges = applyChanges;
  vm.reloadState = mnHelper.reloadState;
  vm.changeNodeGroup = changeNodeGroup;
  vm.disableApplyChangesBtn = true;

  activate();

  function applyChanges() {
    mnPromiseHelper($scope, mnGroupsService.applyChanges(vm.state.uri, vm.state.currentGroups))
      .reloadState("app.admin.groups")
      .showGlobalSuccess("Group changes applied successfully!")
      .getPromise()
      .then(null, function (resp) {
        if (resp.status === 409) {
          vm.disableAddGroupBtn = true;
          vm.disableApplyChangesBtn = true;
          vm.revisionMismatch = true;
        } else {
          mnAlertsService.showAlertInPopup(resp.data, 'error');
        }
      });
  }

  function isGroupsEqual() {
    return _.isEqual(vm.state.initialGroups, vm.state.currentGroups);
  }

  function deleteGroup(group) {
    if (isGroupsEqual()) {
      return $uibModal.open({
        templateUrl: 'app/mn_admin/mn_groups_delete_dialog.html',
        controller: 'mnGroupsDeleteDialogController as groupsDeleteDialogCtl',
        resolve: {
          group: mnHelper.wrapInFunction(group)
        }
      });
    } else {
      $window.scrollTo(0, 0);
      vm.serverGroupsWarnig = true;
      vm.disableApplyChangesBtn = false;
    }
  }

  function createGroup(group) {
    return $uibModal.open({
      templateUrl: 'app/mn_admin/mn_groups_group_dialog.html',
      controller: 'mnGroupsGroupDialogController as groupsGroupDialogCtl',
      resolve: {
        group: mnHelper.wrapInFunction(group)
      }
    });
  }

  function changeNodeGroup(groupOld, groupNew, server) {
    if (groupOld === groupNew || groupNew === server.toGroupPending) {
      return;
    }
    var fromGroup = _.find(vm.state.currentGroups, function (cGroup) {
      return cGroup.name === groupOld;
    });

    var toGroup = _.find(vm.state.currentGroups, function (cGroup) {
      return cGroup.name === groupNew;
    });

    _.remove(fromGroup.nodes, function (node) {
      return node.hostname === server.hostname;
    });

    toGroup.nodes.push(server);

    if (server.toGroupPending === groupOld) {
      delete server.toGroupPending;
    } else {
      server.toGroupPending = toGroup.name
    }

    vm.disableApplyChangesBtn = false;
  }

  function activate() {
    mnPromiseHelper(vm, mnGroupsService.getGroupsState())
      .applyToScope("state");
  }
}
