/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnBucketsList;

function mnBucketsList(mnHelper) {
  var mnBucketsListDirective = {
    restrict: 'A',
    scope: {
      buckets: '=',
      rbac: "=",
      poolDefault: "=",
      adminCtl: "="
    },
    templateUrl: 'app/mn_admin/mn_buckets_list.html',
    controller: controller,
    controllerAs: "bucketsListCtl"
  };

  return mnBucketsListDirective;

  function controller() {
    var vm = this;
    mnHelper.initializeDetailsHashObserver(vm, 'openedBucket', 'app.admin.buckets');
  }
}
