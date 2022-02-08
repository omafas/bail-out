/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import ngAnimate from "angular-animate";
import uiSelect from "ui-select";
import uiBootstrap from "angular-ui-bootstrap";
import uiRouter from "@uirouter/angularjs";
import {downgradeInjectable, setAngularJSGlobal} from "@angular/upgrade/static";
setAngularJSGlobal(angular);

import mnAdminController from "./mn_admin_controller.js";
import mnAlertsService from "../components/mn_alerts.js";
import mnPoolDefault from "../components/mn_pool_default.js";
import mnPoll from "../components/mn_poll.js";
import mnFilters from "../components/mn_filters.js";
import mnHelper from "../components/mn_helper.js";
import mnSpinner from "../components/directives/mn_spinner.js";
import mnMainSpinner from "../components/directives/mn_main_spinner.js";
import mnLaunchpad from "../components/directives/mn_launchpad.js";
import mnPluggableUiRegistry from "../components/mn_pluggable_ui_registry.js";
import mnSettingsAutoFailoverService from "./mn_settings_auto_failover_service.js";
import mnSettingsClusterService from "./mn_settings_cluster_service.js";

import mnAuthService from "../mn_auth/mn_auth_service.js";
import mnPermissions from "../components/mn_permissions.js";
import mnElementCrane from "../components/directives/mn_element_crane/mn_element_crane.js";
import mnDragAndDrop from "../components/directives/mn_drag_and_drop.js";
import mnTasksDetails from "../components/mn_tasks_details.js";

import mnLostConnection from "./mn_lost_connection_config.js";
import {MnAdminService} from "../mn.admin.service.js";
import {MnSessionService} from "../mn.session.service.js";
import {MnStatsService} from "../mn.stats.service.js";

import mnDetailStatsModule from "../components/directives/mn_detail_stats_controller.js";

import mnSelect from "../components/directives/mn_select/mn_select.js";

export default 'mnAdmin';

angular.module('mnAdmin', [
  ngAnimate,
  uiBootstrap,
  uiRouter,
  uiSelect,

  mnPoll,
  mnFilters,
  mnAlertsService,
  mnPoolDefault,
  mnAuthService,
  mnHelper,
  mnSpinner,
  mnMainSpinner,

  mnTasksDetails,

  mnLaunchpad,
  mnPluggableUiRegistry,
  mnLostConnection,
  mnPermissions,
  mnElementCrane,
  mnDragAndDrop,
  mnSettingsAutoFailoverService,
  mnSettingsClusterService,
  mnDetailStatsModule,
  mnSelect
]).config(["$stateProvider", "$urlMatcherFactoryProvider", "mnPluggableUiRegistryProvider", "$httpProvider", mnAdminConfig])
  .controller('mnAdminController', mnAdminController)
  .factory('mnAdminService', downgradeInjectable(MnAdminService))
  .factory('mnSessionService', downgradeInjectable(MnSessionService))
  .factory('mnStatsServiceDowngraded', downgradeInjectable(MnStatsService));

//https://github.com/angular-ui/ui-select/issues/1560
angular.module('ui.select').run(function($animate) {
  var origEnabled = $animate.enabled
  $animate.enabled = function (elem) {
    if (arguments.length !== 1) {
      return origEnabled.apply($animate, arguments);
    } else if (origEnabled(elem)) {
      return (/enable-ng-animation/).test(elem.classNames);
    }
    return false
  }
});

angular.module('mnAdmin').run(function($rootScope, $uibModal, $ocLazyLoad, $injector) {
  let mnPoolDefault = $injector.get('mnPoolDefault');

  $rootScope.$on("maybeShowMemoryQuotaDialog",
    loadAndRunMemoryQuotaDialog($uibModal, $ocLazyLoad, $injector, mnPoolDefault));

  function loadAndRunMemoryQuotaDialog($uibModal, $ocLazyLoad, $injector, mnPoolDefault) {
    return async function (_, services) {
      var poolsDefault = await mnPoolDefault.get();
      var servicesToCheck = ["index", "fts"];
      if (poolsDefault.isEnterprise) {
        servicesToCheck = servicesToCheck.concat(["cbas", "eventing"]);
      }
      await import("../components/directives/mn_memory_quota/mn_memory_quota_service.js");
      await $ocLazyLoad.load({name: 'mnMemoryQuotaService'});
      var mnMemoryQuotaService = $injector.get('mnMemoryQuotaService');

      var firstTimeAddedServices =
        mnMemoryQuotaService.getFirstTimeAddedServices(servicesToCheck,
          services, poolsDefault.nodes);
      if (!firstTimeAddedServices.count) {
        return;
      }

      await import("./memory_quota_dialog_controller.js");
      await $ocLazyLoad.load({name: 'mnMemoryQuotaDialogController'});
      $uibModal.open({
        windowTopClass: "without-titlebar-close",
        backdrop: 'static',
        templateUrl: 'app/mn_admin/memory_quota_dialog.html',
        controller: 'mnMemoryQuotaDialogController as memoryQuotaDialogCtl',
        resolve: {
          memoryQuotaConfig: function (mnMemoryQuotaService) {
            return mnMemoryQuotaService.memoryQuotaConfig(services, true, false);
          },
          indexSettings: function (mnSettingsClusterService) {
            return mnSettingsClusterService.getIndexSettings();
          },
          firstTimeAddedServices: function() {
            return firstTimeAddedServices;
          }
        }
      });
    }
  };
});

function mnAdminConfig($stateProvider, $urlMatcherFactoryProvider, mnPluggableUiRegistryProvider, $httpProvider) {

  $httpProvider.interceptors.push(['$q', '$injector', interceptorOf401]);

  function interceptorOf401($q, $injector) {
    return {
      responseError: function (rejection) {
        if (rejection.status === 401 &&
            rejection.config.url !== "/pools" &&
            rejection.config.url !== "/controller/changePassword" &&
            rejection.config.url !== "/uilogout" &&
            ($injector.get('$state').includes('app.admin') ||
             $injector.get('$state').includes('app.wizard')) &&
            !rejection.config.headers["ignore-401"] &&
            !$injector.get('mnLostConnectionService').getState().isActive) {
          $injector.get('mnAuthService').logout();
        }
        return $q.reject(rejection);
      }
    };
  }

  function valToString(val) {
    return val != null ? val.toString() : val;
  }
  $urlMatcherFactoryProvider.type("string", {
    encode: valToString,
    decode: valToString,
    is: function (val) {
      return (/[^/]*/).test(val);
    }
  });

  mnPluggableUiRegistryProvider.registerConfig({
    name: 'Indexes',
    state: 'app.admin.gsi',
    includedByState: 'app.admin.gsi',
    plugIn: 'workbenchTab',
    index: 2,
    ngShow: "rbac.cluster.bucket['.'].n1ql.index.read"
  });

  $stateProvider
    .state('app.admin', {
      url: "?commonBucket&commonScope&commonCollection&scenarioZoom&scenario",
      abstract: true,
      data: {
        requiresAuth: true
      },
      params: {
        openedGroups: {
          value: [],
          array: true,
          dynamic: true
        },
        commonBucket: {
          value: null,
          dynamic: true
        },
        commonScope: {
          value: null,
          dynamic: true
        },
        commonCollection: {
          value: null,
          dynamic: true
        },
        scenario: {
          value: null,
          dynamic: true
        },
        scenarioZoom: {
          value: "minute"
        }
      },
      resolve: {
        poolDefault: function (mnPoolDefault) {
          return mnPoolDefault.getFresh();
        },
        pools: function (mnPools) {
          return mnPools.get();
        },
        permissions: function (mnPermissions) {
          return mnPermissions.check();
        },
        whoami: function (mnAuthService) {
          return mnAuthService.whoami();
        }
      },
      views: {
        "": {
          controller: 'mnAdminController as adminCtl',
          templateUrl: 'app/mn_admin/mn_admin.html'
        },
        "lostConnection@app.admin": {
          templateUrl: 'app/mn_admin/mn_lost_connection.html',
          controller: 'mnLostConnectionController as lostConnCtl'
        }
      }
    });
}
