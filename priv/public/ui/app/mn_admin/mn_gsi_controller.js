/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiBootstrap from "/ui/web_modules/angular-ui-bootstrap.js";
import {format} from "/ui/web_modules/d3-format.js";
import _ from "/ui/web_modules/lodash.js";

import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnHelper from "/ui/app/components/mn_helper.js";
import mnFilters from "/ui/app/components/mn_filters.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";
import mnPoll from "/ui/app/components/mn_poll.js";
import mnPoolDefault from "/ui/app/components/mn_pool_default.js";
import mnAlertsService from "/ui/app/components/mn_alerts.js";
import mnPermissions from "/ui/app/components/mn_permissions.js";
import mnSearch from "/ui/app/components/directives/mn_search/mn_search_directive.js";
import mnSortableTable from "/ui/app/components/directives/mn_sortable_table.js";
import mnElementCrane from "/ui/app/components/directives/mn_element_crane/mn_element_crane.js";
import mnDetailStats from "/ui/app/components/directives/mn_detail_stats_controller.js";

import mnStatisticsNewService from "./mn_statistics_service.js";
import mnGsiService from "./mn_gsi_service.js";

import {mnGsiItemController, mnGsiItemStatsController, mnGsiItemDetails} from "./mn_gsi_item_details.js";
import mnFooterStatsController from "./mn_gsi_footer_controller.js";
import mnGsiTableDirective from "./mn_gsi_table_directive.js";
import mnKeyspaceSelectorDowngradeModule from "/ui/app/mn.keyspace.selector.downgrade.module.js"
import {Subject} from "/ui/web_modules/rxjs.js";
import {takeUntil, filter, withLatestFrom} from "/ui/web_modules/rxjs/operators.js";
export default 'mnGsi';

angular
  .module('mnGsi', [
    uiBootstrap,
    mnPromiseHelper,
    mnHelper,
    mnFilters,
    mnSpinner,
    mnMainSpinner,
    mnPoll,
    mnPoolDefault,
    mnAlertsService,
    mnPermissions,
    mnSearch,
    mnSortableTable,
    mnElementCrane,
    mnDetailStats,
    mnGsiService,
    mnStatisticsNewService,
    mnKeyspaceSelectorDowngradeModule
  ])
  .config(configure)
  .controller('mnGsiController', mnGsiController)
  .controller('mnGsiFooterController', mnFooterStatsController)
  .controller('mnGsiItemController', mnGsiItemController)
  .controller('mnGsiItemStatsController', mnGsiItemStatsController)
  .directive('mnGsiItemDetails', mnGsiItemDetails)
  .directive('mnGsiTable', mnGsiTableDirective);

function configure($stateProvider, $transitionsProvider) {
  $transitionsProvider.onBefore({
    from: state => (state.name !== "app.admin.gsi"),
    to: "app.admin.gsi"
  }, trans => {
    var mnPermissionsService = trans.injector().get("mnPermissions");
    var original = Object.assign({}, trans.params());

    return mnPermissionsService.check().then(function (permissions) {
      let params = Object.assign({}, original);
      var indexesRead = permissions.bucketNames['.n1ql.index!read'];

      if (!params.commonBucket && indexesRead && indexesRead[0]) {
        params.commonBucket = indexesRead[0];
      } else if (params.commonBucket &&
                 indexesRead && indexesRead.indexOf(params.commonBucket) < 0) {
        params.commonBucket = indexesRead[0];
      } else if (params.commonBucket && (!indexesRead || !indexesRead[0])) {
        params.commonBucket = null;
      }

      if (params.commonBucket && !params.commonScope) {
        params.commonScope = "_default";
      }
      if (!params.commonBucket) {
        params.commonScope = null;
      }

      if (original.commonBucket !== params.commonBucket ||
          original.commonScope !== params.commonScope) {
        return trans.router.stateService.target("app.admin.gsi", params);
      }
    });
  });

  $stateProvider
    .state('app.admin.gsi', {
      url: "/index?openedIndex&perIndexPage&perNodePage&indexesView",
      params: {
        openedIndex: {
          array: true,
          dynamic: true
        },
        indexesView: {
          value: 'viewByIndex',
          dynamic: true
        },
        footerBucket: {
          value: null,
          dynamic: true
        },
        perNodePage: {
          value: {},
          type: "json",
          dynamic: true
        },
        perIndexPage: {
          value: {page:1, size:15},
          type: "json",
          dynamic: true
        },
      },
      data: {
        title: "Indexes",
        permissions: "cluster.bucket['.'].n1ql.index.read"
      },
      views: {
        "main@app.admin": {
          controller: "mnGsiController as gsiCtl",
          templateUrl: "app/mn_admin/mn_gsi.html"
        }
      }
    });
}
function mnGsiController($scope, mnGsiService, mnPoller, $state, mnCollectionsServiceDowngrade,
                         poolDefault) {
  var vm = this;

  vm.setIndexesView = setIndexesView;

  activate();

  function setIndexesView(selectedOption) {
    $state.go('.', {indexesView: selectedOption}).then(() => vm.poller.reload());
  }

  function activate() {
    vm.viewBy = $state.params.indexesView;

    let mnOnDestroy = new Subject();

    vm.poller =
      new mnPoller($scope, () => {
        if (poolDefault.compat.atLeast70) {
          let params = vm.mnCollectionSelectorService.stream.result.getValue();
          if ($state.params.indexesView == "viewByNode") {
            return mnGsiService.getIndexesStateByNodes(params);
          } else {
            return mnGsiService.getIndexesState(params);
          }
        } else {
          if ($state.params.indexesView == "viewByNode") {
            return mnGsiService.getIndexesStateByNodesMixed();
          } else {
            return mnGsiService.getIndexesStateMixed();
          }
        }
      })
      .setInterval(10000)
      .subscribe("state", vm)
      .reloadOnScopeEvent("indexStatusURIChanged");

    if (!poolDefault.compat.atLeast70) {
      vm.poller.reload();
      return;
    }

    vm.mnCollectionSelectorService =
      mnCollectionsServiceDowngrade.createCollectionSelector({
        component: {mnOnDestroy},
        steps: ["bucket", "scope"]
      });

    vm.mnCollectionSelectorService.stream.showHideDropdown
      .pipe(filter(v => !v),
            takeUntil(mnOnDestroy))
      .subscribe(stateGo);

    $scope.$on("$destroy", function () {
      mnOnDestroy.next();
      mnOnDestroy.complete();
    });

    $scope.$watchCollection(() => ({
      bucket: $state.params.commonBucket,
      scope: $state.params.commonScope
    }), v => {
      vm.mnCollectionSelectorService.setKeyspace(v);
    });

    if (!$state.params.commonBucket) {
      stateGo();
    }

    function stateGo() {
      vm.poller.reload();
      let params = vm.mnCollectionSelectorService.stream.result.getValue();
      $state.go('.', {
        commonBucket: params.bucket ? params.bucket.name: null,
        commonScope: params.scope ? params.scope.name : null,
        commonCollection: null
      }, {notify: false});
    }
  }
}
