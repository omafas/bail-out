/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import _ from "/ui/web_modules/lodash.js";

export default mnViewsListController;

function mnViewsListController($scope, $state, $uibModal, mnViewsListService, mnViewsEditingService, mnPromiseHelper, mnCompaction, mnHelper, mnPoller, permissions) {
  var vm = this;

  vm.type = $state.params.type;
  vm.isDevModeDoc = mnViewsListService.isDevModeDoc;
  vm.getStartedCompactions = mnCompaction.getStartedCompactions;

  vm.showCreationDialog = showCreationDialog;
  vm.showMapreduceCreationDialog = showMapreduceCreationDialog;
  vm.showDdocDeletionDialog = showDdocDeletionDialog;
  vm.showViewDeletionDialog = showViewDeletionDialog;
  vm.publishDdoc = publishDdoc;
  vm.copyToDev = copyToDev;
  vm.registerCompactionAsTriggeredAndPost = registerCompactionAsTriggeredAndPost;
  vm.showPublishButton = showPublishButton;
  vm.showCreationButton = showCreationButton;
  vm.showViewCreationButtons = showViewCreationButtons;
  vm.getInitialViewsFilterParams = getInitialViewsFilterParams;
  vm.isDevelopmentViews = $state.params.type === 'development';

  activate();

  function getInitialViewsFilterParams(key, row) {
    return {
      sampleDocumentId: null,
      pageNumber: 0,
      viewId: key,
      full_set: null,
      documentId: row.doc.meta.id,
      commonBucket: $state.params.commonBucket,
      viewsParams: JSON.stringify(mnViewsEditingService.getInitialViewsFilterParams())
    };
  }
  function showViewCreationButtons() {
    return vm.ddocs && $state.params.commonBucket && vm.isDevelopmentViews && !vm.ddocs.ddocsAreInFactMissing;
  }
  function showPublishButton(row) {
    return vm.isDevelopmentViews && !isEmptyView(row);
  }
  function isEmptyView(row) {
    return !row.doc.json.views || _.isEmpty(row.doc.json.views);
  }
  function showCreationButton(row) {
    return vm.isDevelopmentViews;
  }

  function showMapreduceCreationDialog() {
    showCreationDialog(undefined, false);
  }

  function showCreationDialog(ddoc) {
    $uibModal.open({
      controller: 'mnViewsCreateDialogController as viewsCreateDialogCtl',
      templateUrl: 'app/mn_admin/mn_views_create_dialog.html',
      scope: $scope,
      resolve: {
        currentDdoc: mnHelper.wrapInFunction(ddoc),
        viewType: mnHelper.wrapInFunction("views")
      }
    });
  }
  function showDdocDeletionDialog(ddoc) {
    $uibModal.open({
      controller: 'mnViewsDeleteDdocDialogController as viewsDeleteDdocDialogCtl',
      templateUrl: 'app/mn_admin/mn_views_delete_ddoc_dialog.html',
      scope: $scope,
      resolve: {
        currentDdocName: mnHelper.wrapInFunction(ddoc.meta.id)
      }
    });
  }
  function showViewDeletionDialog(ddoc, viewName) {
    $uibModal.open({
      controller: 'mnViewsDeleteViewDialogController as viewsDeleteViewDialogCtl',
      templateUrl: 'app/mn_admin/mn_views_delete_view_dialog.html',
      scope: $scope,
      resolve: {
        currentDdocName: mnHelper.wrapInFunction(ddoc.meta.id),
        currentViewName: mnHelper.wrapInFunction(viewName)
      }
    });
  }
  function prepareToPublish(url, ddoc) {
    return function () {
      return mnPromiseHelper(vm, mnViewsListService.createDdoc(url, ddoc.json))
        .onSuccess(function () {
          $state.go('^.list', {
            type: 'production'
          });
        })
        .showGlobalSuccess("Design document published successfully!")
        .getPromise();
    };
  }
  function publishDdoc(ddoc) {
    var url = mnViewsListService.getDdocUrl($state.params.commonBucket, "_design/" + mnViewsListService.cutOffDesignPrefix(ddoc.meta.id));
    var publish = prepareToPublish(url, ddoc);
    var promise = mnViewsListService.getDdoc(url).then(function () {
      return $uibModal.open({
        windowClass: "z-index-10001",
        backdrop: 'static',
        templateUrl: 'app/mn_admin/mn_views_confirm_override_dialog.html'
      }).result.then(publish);
    }, publish);

    mnPromiseHelper(vm, promise)
      .showGlobalSpinner();
  }
  function copyToDev(ddoc) {
    $uibModal.open({
      controller: 'mnViewsCopyDialogController as viewsCopyDialogCtl',
      templateUrl: 'app/mn_admin/mn_views_copy_dialog.html',
      scope: $scope,
      resolve: {
        currentDdoc: mnHelper.wrapInFunction(ddoc)
      }
    });
  }
  function registerCompactionAsTriggeredAndPost(row) {
    mnPromiseHelper(vm, mnCompaction.registerAsTriggeredAndPost(row.controllers.compact))
      .broadcast("reloadViewsPoller");
  }
  function activate() {
    if (permissions.cluster.tasks.read) {
      new mnPoller($scope, function () {
        return mnViewsListService.getTasksOfCurrentBucket($state.params);
      })
        .subscribe("tasks", vm)
        .reloadOnScopeEvent(["reloadViewsPoller", "mnTasksDetailsChanged"])
        .cycle();
    }
    if (permissions.cluster.bucket['.'].views.read) {
      new mnPoller($scope, function () {
        var promise = mnViewsListService.getViewsListState($state.params)
        promise["finally"](function () {
          $scope.viewsCtl.ddocsLoading = false;
        });
        return promise;
      })
        .setInterval(10000)
        .subscribe("ddocs", vm)
        .reloadOnScopeEvent("reloadViewsPoller", vm, "showViewsPollerSpinner")
        .cycle();
    } else {
      $scope.viewsCtl.ddocsLoading = false;
    }
  }
}
