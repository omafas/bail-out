/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnBuckets =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnBuckets, mn.core.MnEventableComponent);

    MnBuckets.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-buckets.html"
      })
    ];

    MnBuckets.parameters = [
      mn.services.MnHelper,
      mn.services.MnAdmin,
      mn.services.MnBuckets,
      mn.services.MnPermissions,
      ngb.NgbModal
    ];

    MnBuckets.prototype.trackByFn = trackByFn;

    return MnBuckets;

    function trackByFn(_, bucket) {
      return bucket.name;
    }

    function MnBuckets(mnHelperService, mnAdminService, mnBucketsService, mnPermissionsService, modalService) {
      mn.core.MnEventableComponent.call(this);

      this.isRebalancing = mnAdminService.stream.isRebalancing;
      this.maxBucketCount = mnAdminService.stream.maxBucketCount;
      this.onAddBucketClick = new Rx.Subject();
      this.onSortByClick = new Rx.BehaviorSubject("name");

      this.buckets =
        mnBucketsService.stream.bucketsWithTimer
        .pipe(mnHelperService.sortByStream(this.onSortByClick));

      this.maybeShowMaxBucketCountWarning =
        Rx.combineLatest(
          mnAdminService.stream.maxBucketCount, this.buckets)
        .pipe(
          Rx.operators.map(function (rv) {
            return rv[1].length >= rv[0];
          })
        );

      this.maybeShowAddBucketBuctton =
        Rx.combineLatest(
          this.maybeShowMaxBucketCountWarning,
          this.isRebalancing,
          mnPermissionsService.createPermissionStream("buckets!create")
        ).pipe(
          Rx.operators.map(function (rv) {
            return !rv[0] && !rv[1] && rv[2];
          })
        );

      this.onAddBucketClick.pipe(
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(function (rv) {
        var ref = modalService.open(mn.components.MnBucketsDialog);
        ref.componentInstance.bucket = null;
      });
    }

  })(window.rxjs);
