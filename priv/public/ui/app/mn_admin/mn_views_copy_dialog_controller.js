/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnViewsCopyDialogController;

function mnViewsCopyDialogController($scope, $uibModal, $state, mnViewsListService, mnPromiseHelper, $uibModalInstance, currentDdoc) {
  var vm = this;

  vm.ddoc = {};
  vm.ddoc.name = mnViewsListService.cutOffDesignPrefix(currentDdoc.meta.id);
  vm.onSubmit = onSubmit;

  function onSubmit() {
    var url = mnViewsListService.getDdocUrl($state.params.commonBucket, "_design/dev_" + vm.ddoc.name);
    var copy = prepareToCopy(url, currentDdoc);
    var promise = mnViewsListService.getDdoc(url).then(function (presentDdoc) {
      return $uibModal.open({
        windowClass: "z-index-10001",
        backdrop: 'static',
        templateUrl: 'app/mn_admin/mn_views_confirm_override_dialog.html'
      }).result.then(copy);
    }, copy);

    mnPromiseHelper(vm, promise)
      .showGlobalSpinner()
      .showGlobalSuccess("View copied successfully!");
  }
  function prepareToCopy(url, ddoc) {
    return function () {
      return mnPromiseHelper(vm, mnViewsListService.createDdoc(url, ddoc.json), $uibModalInstance)
        .closeOnSuccess()
        .onSuccess(function () {
          $state.go('^.list', {
            type: 'development'
          });
        })
        .getPromise();
    };
  }
}
