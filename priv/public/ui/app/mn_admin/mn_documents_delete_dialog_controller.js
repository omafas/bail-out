/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnDocumentsDeleteDialogController;

function mnDocumentsDeleteDialogController(mnDocumentsEditingService, $state, documentId, $uibModalInstance, mnPromiseHelper) {
  var vm = this;
  vm.onSubmit = onSubmit;

  function onSubmit() {
    var promise = mnDocumentsEditingService.deleteDocument({
      commonBucket: $state.params.commonBucket,
      documentId: documentId
    });

    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .closeFinally()
      .broadcast("reloadDocumentsPoller")
      .showGlobalSuccess("Document deleted successfully!");
  }
}
