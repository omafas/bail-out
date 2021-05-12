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
import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";
import uiCodemirror from "/ui/libs/angular-ui-codemirror.js";

import mnDocumentsControlController from "./mn_documents_control_controller.js";
import mnDocumentsListController from "./mn_documents_list_controller.js";
import mnDocumentsCreateDialogController from "./mn_documents_create_dialog_controller.js";
import mnDocumentsEditingController from "./mn_documents_editing_controller.js";
import mnDocumentsDeleteDialogController from "./mn_documents_delete_dialog_controller.js";

import mnDocumentsListService from "./mn_documents_list_service.js";
import mnDocumentsEditingService from "./mn_documents_editing_service.js";

import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnFilters from "/ui/app/components/mn_filters.js";
import mnFilter from "/ui/app/components/directives/mn_filter/mn_filter.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";
import ngMessages from "/ui/web_modules/angular-messages.js";

import mnPoll from "/ui/app/components/mn_poll.js";
import mnElementCrane from "/ui/app/components/directives/mn_element_crane/mn_element_crane.js";

export default "mnDocuments";

angular
  .module("mnDocuments", [
    mnDocumentsListService,
    mnDocumentsEditingService,
    mnPromiseHelper,
    mnFilter,
    mnFilters,
    uiRouter,
    uiBootstrap,
    uiCodemirror,
    mnSpinner,
    mnMainSpinner,
    ngMessages,
    mnPoll,
    mnElementCrane
  ])
  .config(configure)
  .controller("mnDocumentsController", mnDocumentsController)
  .controller("mnDocumentsControlController", mnDocumentsControlController)
  .controller("mnDocumentsCreateDialogController", mnDocumentsCreateDialogController)
  .controller("mnDocumentsListController", mnDocumentsListController)
  .controller("mnDocumentsEditingController", mnDocumentsEditingController)
  .controller("mnDocumentsDeleteDialogController", mnDocumentsDeleteDialogController);

function configure($stateProvider) {
  $stateProvider
    .state('app.admin.documents', {
      abstract: true,
      views: {
        "main@app.admin": {
          templateUrl: 'app/mn_admin/mn_documents.html',
          controller: "mnDocumentsController as documentsCtl"
        }
      },
      url: "/documents",
      data: {
        title: "Documents",
        parent: {name: 'Buckets', link: 'app.admin.buckets'},
        permissions: "cluster.bucket['.'].settings.read && cluster.bucket['.'].data.docs.read"
      }
    })
    .state('app.admin.documents.control', {
      abstract: true,
      controller: 'mnDocumentsControlController as documentsControlCtl',
      templateUrl: 'app/mn_admin/mn_documents_control.html'
    })
    .state('app.admin.documents.control.list', {
      url: "?{pageLimit:int}&{pageNumber:int}&documentsFilter",
      params: {
        pageLimit: {
          value: 10
        },
        pageNumber: {
          value: 0
        },
        documentsFilter: null
      },
      controller: 'mnDocumentsListController as documentsListCtl',
      templateUrl: 'app/mn_admin/mn_documents_list.html'
    })
    .state('app.admin.documents.editing', {
      url: '/:documentId',
      controller: 'mnDocumentsEditingController as documentsEditingCtl',
      templateUrl: 'app/mn_admin/mn_documents_editing.html',
      data: {
        parent: {name: 'Documents', link: 'app.admin.documents.control.list'},
        title: "Documents Editing"
      }
    });
}

function mnDocumentsController($state) {
  var vm = this;

  vm.onSelectBucketName = onSelectBucketName;
  vm.currentBucketName = $state.params.commonBucket;

  function onSelectBucketName(selectedBucket) {
    $state.go('^.list', {
      commonBucket: selectedBucket,
      commonScope: null,
      commonCollection: null,
      pageNumber: 0
    }, {reload: true});
  }
}
