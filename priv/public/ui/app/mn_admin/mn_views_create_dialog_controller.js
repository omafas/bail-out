/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import _ from "/ui/web_modules/lodash.js";

export default mnViewsCreateDialogController;

function mnViewsCreateDialogController($uibModal, $state, $q, mnViewsListService, mnPromiseHelper, $uibModalInstance, currentDdoc, viewType) {
  var vm = this;
  var isViewsEditingSection = $state.includes('views.editing.result');
  vm.ddoc = {};
  vm.ddoc.name = currentDdoc && mnViewsListService.cutOffDesignPrefix(currentDdoc.meta.id);
  vm.doesDdocExist = !!currentDdoc;
  if (isViewsEditingSection) {
    vm.ddoc.view = $state.params.viewId;
  }
  vm.isCopy = isViewsEditingSection;
  vm.onSubmit = onSubmit;

  function getDdocUrl() {
    return mnViewsListService.getDdocUrl($state.params.commonBucket, '_design/dev_' + vm.ddoc.name);
  }

  function createDdoc(presentDdoc) {
    var ddoc = presentDdoc || {json: {}};
    var views = ddoc.json[viewType] || (ddoc.json[viewType] = {});
    if (vm.isCopy) {
      views[vm.ddoc.view] = currentDdoc.json[viewType][$state.params.viewId];
    } else {
      views[vm.ddoc.view] = {
        map: 'function (doc, meta) {\n  emit(meta.id, null);\n}'
      };
    }

    return mnViewsListService.createDdoc(getDdocUrl(), ddoc.json);
  }

  function onSubmit(ddocForm) {
    if (ddocForm.$invalid || vm.viewLoading) {
      return;
    }
    vm.error = false;
    var promise = mnViewsListService.getDdoc(getDdocUrl()).then(function (presentDdoc) {
      var views = presentDdoc.json[viewType] || (presentDdoc.json[viewType] = {});
      if (views[vm.ddoc.view] && !vm.isCopy) {
        return $q.reject({
          data: {
            reason: 'View with given name already exists'
          }
        });
      }
      if (_.keys(views).length >= 10) {
        return $uibModal.open({
          windowClass: "z-index-10001",
          backdrop: 'static',
          templateUrl: 'app/mn_admin/mn_views_confirm_limit_dialog.html'
        }).result.then(function () {
          return createDdoc(presentDdoc);
        }, function () {
          $uibModalInstance.close();
        });
      }
      return createDdoc(presentDdoc);
    }, function () {
      return createDdoc();
    });

    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors()
      .closeOnSuccess()
      .broadcast("reloadViewsPoller")
      .showGlobalSuccess("View created successfully!");
  };

}
