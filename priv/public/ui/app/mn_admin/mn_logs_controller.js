/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnPoll from "/ui/app/components/mn_poll.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";
import mnFilters from "/ui/app/components/mn_filters.js";
import mnSearch from "/ui/app/components/directives/mn_search/mn_search_directive.js";
import mnSortableTable from "/ui/app/components/directives/mn_sortable_table.js";
import mnElementCrane from "/ui/app/components/directives/mn_element_crane/mn_element_crane.js";
import mnSelectableNodesList from "/ui/app/components/directives/mn_selectable_nodes_list.js";

import mnLogsService from "./mn_logs_service.js";
import mnLogRedactionService from "./mn_redaction_service.js";
import mnGroupsService from "./mn_groups_service.js";
import mnLogsCollectInfoController from "./mn_logs_collect_info_controller.js";
import {mnLogsListController, moduleCodeFilter} from "./mn_logs_list_controller.js";
import mnLogsCollectInfoService from "./mn_logs_collect_info_service.js";


export default 'mnLogs';

angular.module('mnLogs', [
  mnPromiseHelper,
  mnPoll,
  mnSpinner,
  mnMainSpinner,
  mnFilters,
  mnSearch,
  mnSortableTable,
  mnElementCrane,
  mnSelectableNodesList,
  mnLogsService,
  mnLogRedactionService,
  mnLogsCollectInfoService,
  mnGroupsService
])
  .config(configure)
  .controller('mnLogsController', mnLogsController)
  .controller('mnLogsListController', mnLogsListController)
  .controller('mnLogsCollectInfoController', mnLogsCollectInfoController)
  .filter('moduleCode', moduleCodeFilter);

function configure($stateProvider) {
  $stateProvider
    .state('app.admin.logs', {
      url: '/logs',
      abstract: true,
      views: {
        "main@app.admin": {
          templateUrl: 'app/mn_admin/mn_logs.html',
          controller: 'mnLogsController as logsCtl'
        }
      },
      data: {
        title: "Logs",
        permissions: "cluster.logs.read"
      }
    })
    .state('app.admin.logs.list', {
      url: '',
      controller: 'mnLogsListController as logsListCtl',
      templateUrl: 'app/mn_admin/mn_logs_list.html'
    })
    .state('app.admin.logs.collectInfo', {
      url: '/collectInfo',
      abstract: true,
      controller: 'mnLogsCollectInfoController as logsCollectInfoCtl',
      templateUrl: 'app/mn_admin/mn_logs_collect_info.html',
      data: {
        permissions: "cluster.admin.logs.read",
        title: "Collect Information"
      }
    })
    .state('app.admin.logs.collectInfo.result', {
      url: '/result',
      templateUrl: 'app/mn_admin/mn_logs_collect_info_result.html'
    })
    .state('app.admin.logs.collectInfo.form', {
      url: '/form',
      templateUrl: 'app/mn_admin/mn_logs_collect_info_form.html'
    });
}

function mnLogsController($scope, mnHelper, mnLogsService) {
  var vm = this;
}
