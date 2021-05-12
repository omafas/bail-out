/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnViewsDeleteViewDialogController;

function mnViewsDeleteViewDialogController($state, $uibModalInstance, mnPromiseHelper, mnViewsListService, currentDdocName, currentViewName) {
  var vm = this;
  vm.currentDdocName = currentDdocName;
  vm.currentViewName = currentViewName;
  vm.doDelete = doDelete;

  function doDelete() {
    var url = mnViewsListService.getDdocUrl($state.params.commonBucket, currentDdocName);

    var promise = mnViewsListService.getDdoc(url).then(function (presentDdoc) {
      delete presentDdoc.json['views'][currentViewName];
      return mnViewsListService.createDdoc(url, presentDdoc.json);
    });

    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .closeFinally()
      .broadcast("reloadViewsPoller")
      .showGlobalSuccess("View deleted successfully!");
  };
}
