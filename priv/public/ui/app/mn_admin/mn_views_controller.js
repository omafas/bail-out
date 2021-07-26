/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiSelect from "/ui/web_modules/ui-select.js";
import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";
import uiBootstrap from "/ui/web_modules/angular-ui-bootstrap.js";
import uiCodemirror from "/ui/libs/angular-ui-codemirror.js";

import ngSanitize from "/ui/web_modules/angular-sanitize.js";
import ngMessages from "/ui/web_modules/angular-messages.js";

import mnCompaction from "/ui/app/components/mn_compaction.js";
import mnHelper from "/ui/app/components/mn_helper.js";
import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnPoll from "/ui/app/components/mn_poll.js";
import mnPoolDefault from "/ui/app/components/mn_pool_default.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";

import mnFilter from "/ui/app/components/directives/mn_filter/mn_filter.js";

import mnViewsListService from "./mn_views_list_service.js";
import mnViewsEditingService from "./mn_views_editing_service.js";
import mnViewsListController from  "./mn_views_list_controller.js";
import mnViewsEditingResultController from "./mn_views_editing_result_controller.js";
import mnViewsEditingController from "./mn_views_editing_controller.js";
import mnViewsDeleteViewDialogController from "./mn_views_delete_view_dialog_controller.js";
import mnViewsDeleteDdocDialogController from "./mn_views_delete_ddoc_dialog_controller.js";
import mnViewsCreateDialogController from "./mn_views_create_dialog_controller.js";
import mnViewsCopyDialogController from "./mn_views_copy_dialog_controller.js";
import {downgradeInjectable} from '/ui/web_modules/@angular/upgrade/static.js';
import {QwDialogService} from "/_p/ui/query/angular-directives/qw.dialog.service.js";

export default "mnViews";

angular
  .module("mnViews", [
    uiSelect,
    uiRouter,
    uiBootstrap,
    uiCodemirror,
    ngSanitize,
    ngMessages,
    mnCompaction,
    mnHelper,
    mnPromiseHelper,
    mnPoll,
    mnPoolDefault,
    mnViewsListService,
    mnViewsEditingService,
    mnFilter,
    mnSpinner,
    mnMainSpinner
  ])
  .config(configure)
  .controller("mnViewsController", mnViewsController)
  .controller("mnViewsListController", mnViewsListController)
  .controller("mnViewsEditingResultController", mnViewsEditingResultController)
  .controller("mnViewsEditingController", mnViewsEditingController)
  .controller("mnViewsDeleteViewDialogController", mnViewsDeleteViewDialogController)
  .controller("mnViewsDeleteDdocDialogController", mnViewsDeleteDdocDialogController)
  .controller("mnViewsCreateDialogController", mnViewsCreateDialogController)
  .controller("mnViewsCopyDialogController", mnViewsCopyDialogController)
  .factory('qwDialogService', downgradeInjectable(QwDialogService));

function configure($stateProvider) {
  $stateProvider
    .state('app.admin.views', {
      abstract: true,
      url: '/views',
      data: {
        title: "Views",
        permissions: "cluster.bucket['.'].settings.read && cluster.bucket['.'].views.read"
      },
      views: {
        "main@app.admin": {
          templateUrl: 'app/mn_admin/mn_views.html',
          controller: 'mnViewsController as viewsCtl'
        }
      }
    })
    .state('app.admin.views.list', {
      url: "?type",
      params: {
        type: {
          value: 'development'
        }
      },
      controller: 'mnViewsListController as viewsListCtl',
      templateUrl: 'app/mn_admin/mn_views_list.html'
    })
    .state('app.admin.views.list.editing', {
      abstract: true,
      url: '/:documentId?viewId&sampleDocumentId',
      views: {
        "main@app.admin": {
          controller: 'mnViewsEditingController as viewsEditingCtl',
          templateUrl: 'app/mn_admin/mn_views_editing.html'
        }
      },
      data: {
        parent: {name: 'Views', link: 'app.admin.views.list'},
        title: "Views Editing"
      }
    })
    .state('app.admin.views.list.editing.result', {
      url: '?subset&{pageNumber:int}&viewsParams',
      params: {
        full_set: {
          value: null
        },
        pageNumber: {
          value: 0
        },
        activate: {
          value: null,
          dynamic: true
        }
      },
      controller: 'mnViewsEditingResultController as viewsEditingResultCtl',
      templateUrl: 'app/mn_admin/mn_views_editing_result.html'
    });
}

function mnViewsController($state, mnPoolDefault) {
  var vm = this;
  vm.onSelectBucket = onSelectBucket;
  vm.mnPoolDefault = mnPoolDefault.latestValue();
  vm.ddocsLoading = true;
  vm.currentBucketName = $state.params.commonBucket;

  function onSelectBucket(selectedBucket) {
    $state.go('^.list', {
      commonBucket: selectedBucket,
      commonScope: null,
      commonCollection: null
    }, {reload: true});
  }
}
