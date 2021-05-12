/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnBucketsDetailsDialogController;

function mnBucketsDetailsDialogController($scope, $rootScope, mnBucketsDetailsDialogService, bucketConf, autoCompactionSettings, mnPromiseHelper, $uibModalInstance, mnAlertsService) {
  var vm = this;
  if (autoCompactionSettings !== undefined) {
    bucketConf.autoCompactionDefined = !!bucketConf.autoCompactionSettings;
    vm.autoCompactionSettings = autoCompactionSettings;
  }
  vm.bucketConf = bucketConf;
  vm.validationKeeper = {};
  vm.onSubmit = onSubmit;
  vm.$uibModalInstance = $uibModalInstance;

  function onSubmit() {
    var data = mnBucketsDetailsDialogService.prepareBucketConfigForSaving(vm.bucketConf, vm.autoCompactionSettings, $scope.poolDefault, $scope.pools);
    var promise = mnBucketsDetailsDialogService.postBuckets(data, vm.bucketConf.uri);

    mnPromiseHelper(vm, promise)
      .showGlobalSpinner()
      .catchErrors(function (result) {
        if (result) {
          if (result.summaries) {
            vm.validationResult = mnBucketsDetailsDialogService.adaptValidationResult(result);
          } else {
            mnAlertsService.showAlertInPopup(result, "error");
          }
        }
      })
      .onSuccess(function (result) {
        if (!result.data) {
          $uibModalInstance.close();
          $rootScope.$broadcast("reloadBucketStats");
        }
      })
      .showGlobalSuccess("Bucket settings saved successfully!");
  };
}
