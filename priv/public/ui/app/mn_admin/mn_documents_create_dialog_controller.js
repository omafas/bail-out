/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnDocumentsCreateDialogController;

function mnDocumentsCreateDialogController($scope, mnDocumentsEditingService, mnPromiseHelper, $state, $uibModalInstance, doc) {
  var vm = this;
  vm.onSubmit = onSubmit;

  function onSubmit() {
    var newDocumentParams = {
      commonBucket: $state.params.commonBucket,
      documentId: vm.documentId
    };
    var promise = mnDocumentsEditingService.getDocument(newDocumentParams)
        .then(function () {
          vm.error = "Document with given ID already exists";
        }, function (resp) {
          if (resp.status == 400) {
            // Expect the REST API to tell you why this was a bad request.
            vm.error = resp.data && resp.data.reason;
          } else if (resp.status > 400 && resp.status < 500) {
            return mnPromiseHelper(vm, mnDocumentsEditingService.createDocument(newDocumentParams, doc), $uibModalInstance)
              .catchErrors(function (data) {
                vm.error = data && data.reason;
              })
              .closeOnSuccess()
              .onSuccess(function () {
                $state.go('^.^.editing', {
                  documentId: newDocumentParams.documentId
                });
              });
          } else {
            vm.error = resp.data && resp.data.reason;
          }
        });
    mnPromiseHelper($scope, promise, $uibModalInstance)
      .showGlobalSpinner()
      .showGlobalSuccess("Document created successfully!");
  }
}
