/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import _ from "/ui/web_modules/lodash.js";

import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";
import uiBootstrap from "/ui/web_modules/angular-ui-bootstrap.js";
import ngMessages from "/ui/web_modules/angular-messages.js";

import mnFilters from "/ui/app/components/mn_filters.js";
import mnPoll from "/ui/app/components/mn_poll.js";
import mnFocus from "/ui/app/components/directives/mn_focus.js";
import mnPools from "/ui/app/components/mn_pools.js";
import mnHelper from "/ui/app/components/mn_helper.js";
import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnPoolDefault from "/ui/app/components/mn_pool_default.js";
import mnServices from "/ui/app/components/directives/mn_services/mn_services.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";
import mnAutocompleteOff from "/ui/app/components/directives/mn_autocomplete_off.js";
import mnStorageMode from "/ui/app/components/directives/mn_storage_mode/mn_storage_mode.js";
import mnMemoryQuotaService from "/ui/app/components/directives/mn_memory_quota/mn_memory_quota_service.js";
import mnWarmupProgress from "/ui/app/components/directives/mn_warmup_progress/mn_warmup_progress.js";
import mnElementCrane from "/ui/app/components/directives/mn_element_crane/mn_element_crane.js";
import mnSearch from "/ui/app/components/directives/mn_search/mn_search_directive.js";
import mnBarUsage from "/ui/app/components/directives/mn_bar_usage/mn_bar_usage.js";
import mnSortableTable from "/ui/app/components/directives/mn_sortable_table.js";
import mnSelectableNodesList from "/ui/app/components/directives/mn_selectable_nodes_list.js";
import mnServicesDiskPaths from "/ui/app/components/directives/mn_services_disk_paths.js";

import mnServersService from "./mn_servers_service.js";
import mnServersListItemDetailsService from "./mn_servers_list_item_details_service.js";
import mnGsiService from "./mn_gsi_service.js";
import mnGroupsService from "./mn_groups_service.js";
import mnRootCertificateService from "./mn_root_certificate_service.js";
import mnStatisticsNewService from "./mn_statistics_service.js";

import mnServersListItemDetailsController from "./mn_servers_list_item_details_controller.js";
import mnServersListItemController from "./mn_servers_list_item_controller.js";
import mnServersFailOverDialogController from "./mn_servers_failover_dialog_controller.js";
import mnServersEjectDialogController from "./mn_servers_eject_dialog_controller.js";
import mnServersAddDialogController from "./mn_servers_add_dialog_controller.js";
import mnMultipleFailoverDialogController from "./mn_multiple_failover_dialog.js";
import mnClusterConfigurationService from "/ui/app/mn_wizard/mn_cluster_configuration/mn_cluster_configuration_service.js";


export default "mnServers";

angular
  .module('mnServers', [
    uiRouter,
    uiBootstrap,
    ngMessages,
    mnPoll,
    mnFocus,
    mnPools,
    mnHelper,
    mnPromiseHelper,
    mnPoolDefault,
    mnFilters,
    mnServices,
    mnSpinner,
    mnMainSpinner,
    mnAutocompleteOff,
    mnStorageMode,
    mnMemoryQuotaService,
    mnWarmupProgress,
    mnElementCrane,
    mnServicesDiskPaths,
    mnSearch,
    mnBarUsage,
    mnSortableTable,
    mnSelectableNodesList,
    mnServersService,
    mnServersListItemDetailsService,
    mnGsiService,
    mnGroupsService,
    mnRootCertificateService,
    mnStatisticsNewService,
    mnClusterConfigurationService
  ])
  .config(configure)
  .controller('mnServersController', mnServersController)
  .controller('mnServersListItemDetailsController', mnServersListItemDetailsController)
  .controller("mnServersListItemController", mnServersListItemController)
  .controller('mnServersFailOverDialogController', mnServersFailOverDialogController)
  .controller('mnServersEjectDialogController', mnServersEjectDialogController)
  .controller('mnServersAddDialogController', mnServersAddDialogController)
  .controller('mnMultipleFailoverDialogController', mnMultipleFailoverDialogController);

function configure($stateProvider) {
  $stateProvider
    .state('app.admin.servers', {
      abstract: true,
      url: '/servers',
      views: {
        "main@app.admin": {
          controller: 'mnServersController as serversCtl',
          templateUrl: 'app/mn_admin/mn_servers.html'
        }
      },
      data: {
        title: "Servers"
      }
    })
    .state('app.admin.servers.list', {
      url: '/list?openedServers',
      params: {
        openedServers: {
          array: true,
          dynamic: true
        }
      },
      views: {
        "" : {
          templateUrl: 'app/mn_admin/mn_servers_list.html'
        },
        "details@app.admin.servers.list": {
          templateUrl: 'app/mn_admin/mn_servers_list_item_details.html',
          controller: 'mnServersListItemDetailsController as serversListItemDetailsCtl'
        },
        "item@app.admin.servers.list": {
          templateUrl: 'app/mn_admin/mn_servers_list_item.html',
          controller: 'mnServersListItemController as serversItemCtl'
        }
      }
    });
}

function mnServersController($scope, $state, $uibModal, mnPoolDefault, mnPoller, mnServersService, mnHelper, mnGroupsService, mnPromiseHelper, permissions, mnStatisticsNewService) {
  var vm = this;
  vm.mnPoolDefault = mnPoolDefault.latestValue();

  vm.postStopRebalance = postStopRebalance;
  vm.onStopRecovery = onStopRecovery;
  vm.postRebalance = postRebalance;
  vm.addServer = addServer;
  vm.filterField = "";
  vm.sortByGroup = sortByGroup;
  vm.multipleFailoverDialog = multipleFailoverDialog;
  vm.mnServersStatsPoller = mnStatisticsNewService.createStatsPoller($scope);

  function sortByGroup(node) {
    return vm.getGroupsByHostname[node.hostname] && vm.getGroupsByHostname[node.hostname].name;
  }

  activate();

  function activate() {
    mnHelper.initializeDetailsHashObserver(vm, 'openedServers', 'app.admin.servers.list');

    if (permissions.cluster.server_groups.read) {
      new mnPoller($scope, function () {
        return mnGroupsService.getGroupsByHostname();
      })
        .subscribe("getGroupsByHostname", vm)
        .reloadOnScopeEvent(["serverGroupsUriChanged", "reloadServersPoller"])
        .cycle();
    }

    new mnPoller($scope, function () {
      return mnServersService.getNodes();
    })
      .subscribe(function (nodes) {
        vm.showSpinner = false;
        vm.nodes = nodes;
      })
      .reloadOnScopeEvent(["mnPoolDefaultChanged", "reloadNodes"])
      .cycle();

    // $scope.$on("reloadServersPoller", function () {
    //   vm.showSpinner = true;
    // });
  }
  function multipleFailoverDialog() {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_multiple_failover_dialog.html',
      controller: 'mnMultipleFailoverDialogController as multipleFailoverDialogCtl',
      resolve: {
        groups: function () {
          return mnPoolDefault.get().then(function (poolDefault) {
            if (poolDefault.isGroupsAvailable && permissions.cluster.server_groups.read) {
              return mnGroupsService.getGroupsByHostname();
            }
          });
        },
        nodes: mnHelper.wrapInFunction(vm.nodes),
        allowUnsafe: mnHelper.wrapInFunction(false)

      }
    });
  }
  function addServer() {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_servers_add_dialog.html',
      controller: 'mnServersAddDialogController as serversAddDialogCtl',
      resolve: {
        groups: function () {
          return mnPoolDefault.get().then(function (poolDefault) {
            if (poolDefault.isGroupsAvailable) {
              return mnGroupsService.getGroups();
            }
          });
        }
      }
    });
  }
  function postRebalance() {
    mnPromiseHelper(vm, mnServersService.postRebalance(vm.nodes.allNodes))
      .onSuccess(function () {
        $state.go('app.admin.servers.list', {list: 'active'});
      })
      .broadcast("reloadServersPoller")
      .catchGlobalErrors()
      .showErrorsSensitiveSpinner();
  }
  function onStopRecovery() {
    mnPromiseHelper(vm, mnServersService.stopRecovery($scope.adminCtl.tasks.tasksRecovery.stopURI))
      .broadcast("reloadServersPoller")
      .showErrorsSensitiveSpinner();
  }
  function postStopRebalance() {
    return mnPromiseHelper(vm, mnServersService.stopRebalanceWithConfirm())
      .broadcast("reloadServersPoller");
  }
}
