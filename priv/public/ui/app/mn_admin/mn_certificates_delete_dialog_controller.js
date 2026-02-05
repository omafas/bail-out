/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnCertificatesDeleteDialogController;

mnCertificatesDeleteDialogController.$inject = ["$scope", "$uibModalInstance", "mnCertificatesService", "mnPromiseHelper", "id"];
function mnCertificatesDeleteDialogController($scope, $uibModalInstance, mnCertificatesService, mnPromiseHelper, id) {
  var vm = this;

  vm.onSubmit = onSubmit;

  function onSubmit() {
    if ($scope.mnGlobalSpinnerFlag) {
      return;
    }

    var promise = mnCertificatesService.deletePoolsDefaultTrustedCAs(id);
    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .catchGlobalErrors()
      .closeFinally()
      .broadcast("reloadGetPoolsDefaultTrustedCAs")
      .showGlobalSuccess("Certificate deleted successfully!");
  }
}
