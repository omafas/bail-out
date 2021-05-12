/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnBucketsFlushDialogController;

function mnBucketsFlushDialogController($uibModalInstance, bucket, mnPromiseHelper, mnBucketsDetailsService) {
  var vm = this;
  vm.doFlush = doFlush;

  function doFlush() {
    var promise = mnBucketsDetailsService.flushBucket(bucket);
    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .closeFinally()
      .catchGlobalErrors()
      .broadcast("reloadBucketStats")
      .showGlobalSuccess("Bucket flushed successfully!");
  }
}
