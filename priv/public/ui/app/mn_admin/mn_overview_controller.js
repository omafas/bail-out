/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";
import _ from "/ui/web_modules/lodash.js";

import mnPoll from "/ui/app/components/mn_poll.js";
import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnHelper from "/ui/app/components/mn_helper.js";
import mnPoolDefault from "/ui/app/components/mn_pool_default.js";
import mnBucketsService from "./mn_buckets_service.js";
import mnServersService from "./mn_servers_service.js";
import mnStatisticsNew from "./mn_statistics_controller.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";

import mnElementCrane from "/ui/app/components/directives/mn_element_crane/mn_element_crane.js";

export default 'mnOverview';

angular
  .module('mnOverview', [
    uiRouter,
    mnStatisticsNew,
    mnPoll,
    mnPromiseHelper,
    mnHelper,
    mnPoolDefault,
    mnBucketsService,
    mnServersService,
    mnElementCrane,
    mnMainSpinner
  ])
  .config(mnOverviewConfig)
  .controller('mnOverviewController', mnOverviewController);

function mnOverviewConfig($stateProvider, $transitionsProvider) {
  $transitionsProvider.onBefore({
    from: state => (state.name !== "app.admin.overview.statistics"),
    to: "app.admin.overview.statistics"
  }, trans => {
    var $q = trans.injector().get("$q");
    var mnPermissionsService = trans.injector().get("mnPermissions");
    var mnUserRolesService = trans.injector().get("mnUserRolesService");
    var mnStoreService = trans.injector().get("mnStoreService");
    let original = Object.assign({}, trans.params());

    return $q.all([
      mnPermissionsService.check(),
      mnUserRolesService.getUserProfile()
    ]).then(function ([permissions, profile]) {
      let params = Object.assign({}, original);
      var statsRead = permissions.bucketNames['.stats!read'];
      let scenarios = mnStoreService.store("scenarios").share();
      let groups = mnStoreService.store("groups").share();

      params.scenario =
        (params.scenario && (scenarios.find(item => item.id == params.scenario) || {}).id) ||
        (scenarios.find(item =>
                        (item.uiid == "mn-cluster-overview") ||
                        (item.name == "Cluster Overview")) || {}).id ||
        mnStoreService.store("scenarios").last().id;

      if (!original.openedGroups.length) {
        params.openedGroups = groups
          .filter(g => g.uiid && ((g.uiid == "mn-cluster-overview-group") ||
                                  (g.uiid == "mn-all-services-data-group"))).map(g => g.id);
      }

      if (!params.commonBucket && statsRead && statsRead[0]) {
        params.commonBucket = statsRead[0];
      } else if (params.commonBucket &&
                 statsRead && statsRead.indexOf(params.commonBucket) < 0) {
        params.commonBucket = statsRead[0];
      } else if (params.commonBucket && (!statsRead || !statsRead[0])) {
        params.commonBucket = null;
      }

      if ((params.commonBucket !== original.commonBucket) ||
          (params.scenario !== original.scenario) ||
          (params.openedGroups.length !== original.openedGroups.length)) {
        return trans.router.stateService.target("app.admin.overview.statistics", params);
      }
    });
  });

  $stateProvider
    .state('app.admin.overview', {
      url: '/overview',
      abstract: true,
      views: {
        "main@app.admin": {
          controller: 'mnOverviewController as overviewCtl',
          templateUrl: 'app/mn_admin/mn_overview.html'
        }
      },
      data: {
        title: "Dashboard"
      }
    })
    .state('app.admin.overview.statistics', {
      url: '/stats?statsHostname',
      controller: 'mnStatisticsNewController as statisticsNewCtl',
      templateUrl: 'app/mn_admin/mn_statistics.html',
      params: {
        statsHostname: "all"
      }
    });
}


function mnOverviewController($scope, $rootScope, mnBucketsService, mnServersService, mnPoller, mnPromiseHelper, mnHelper, permissions, pools, mnPoolDefault) {
  var vm = this;

  vm.getEndings = mnHelper.getEndings;
  vm.addressFamily = mnPoolDefault.export.thisNode.addressFamily;
  vm.addressFamilyOnly = mnPoolDefault.export.thisNode.addressFamilyOnly;
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
