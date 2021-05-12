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
mn.components.MnBucketsItemDetails =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnBucketsItemDetails, mn.core.MnEventableComponent);

    MnBucketsItemDetails.annotations = [
      new ng.core.Component({
        selector: "mn-buckets-item-details",
        templateUrl: "app-new/mn-buckets-item-details.html",
        inputs: [
          "bucket"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnBucketsItemDetails.parameters = [
      mn.services.MnBuckets,
      mn.services.MnPermissions,
      mn.services.MnTasks,
      window['@uirouter/angular'].UIRouter,
      mn.services.MnAdmin,
      ngb.NgbModal
    ];

    MnBucketsItemDetails.prototype.getWarmUpTasks = getWarmUpTasks;
    MnBucketsItemDetails.prototype.getBucketRamGuageConfigParams = getBucketRamGuageConfigParams;
    MnBucketsItemDetails.prototype.getGuageConfig = getGuageConfig;

    return MnBucketsItemDetails;

    function MnBucketsItemDetails(mnBucketsService, mnPermissionsService, mnTasksService, uiRouter, mnAdminService, modalService) {
      mn.core.MnEventableComponent.call(this);
      this.editButtonClickEvent = new Rx.Subject();
      this.isRebalancing = mnAdminService.stream.isRebalancing;

      var bucketCurrentValue = this.mnOnChanges.pipe(Rx.operators.pluck("bucket", "currentValue"));
      var bucketName = bucketCurrentValue.pipe(Rx.operators.pluck("name"));

      var thisBucketCompactionTask =
          Rx.combineLatest(
            bucketName.pipe(Rx.operators.distinctUntilChanged()),
            mnTasksService.stream.tasksBucketCompaction
          ).pipe(
            Rx.operators.map(function (values) {
              return _.find(values[1], function (task) {
                return task.bucket === values[0];
              });
            })
          );

      this.editButtonClickEvent.pipe(
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(function (bucket) {
        var ref = modalService.open(mn.components.MnBucketsDialog);
        ref.componentInstance.bucket = bucket;
      });


      this.thisBucketCompactionProgress =
        thisBucketCompactionTask.pipe(
          Rx.operators.map(function (task) {
            return task ? (task.progress + "% complete") : "Not active";
          })
        );

      this.tasksRead = mnPermissionsService.createPermissionStream("tasks!read");

      this.warmUpTasks =
        mnTasksService.stream.tasksWarmingUp.pipe(
          Rx.operators.withLatestFrom(bucketCurrentValue),
          Rx.operators.map(this.getWarmUpTasks.bind(this))
        );

      this.bucketSettingsWrite =
        mnPermissionsService.createPermissionStream("settings!write", bucketName);

      this.bucketRamGuageConfig =
        bucketCurrentValue.pipe(
          Rx.operators.map(this.getBucketRamGuageConfigParams.bind(this)),
          Rx.operators.map(mnBucketsService.getBucketRamGuageConfig),
          mn.core.rxOperatorsShareReplay(1)
        );

      this.bucketRamGuageConfigTotal = this.bucketRamGuageConfig.pipe(
        Rx.operators.pluck("topRight", "value"),
        mn.core.rxOperatorsShareReplay(1)
      );

      this.bucketDiskGuageConfig =
        bucketCurrentValue.pipe(
          Rx.operators.map(this.getGuageConfig.bind(this)),
          Rx.operators.map(mnBucketsService.getGuageConfig),
          mn.core.rxOperatorsShareReplay(1)
        );

      this.bucketDiskGuageConfigTotal = this.bucketDiskGuageConfig.pipe(
        Rx.operators.pluck("topRight", "value"),
        mn.core.rxOperatorsShareReplay(1)
      );

    }

    function getWarmUpTasks(values) {
      var bucket = values[1];
      var tasks = values[0];
      if (!bucket || !tasks) {
        return;
      }
      return _.filter(tasks, function (task) {
        var isNeeded = task.bucket === bucket.name;
        if (isNeeded) {
          task.hostname = _.find(bucket.nodes, function (node) {
            return node.otpNode === task.node;
          }).hostname;
        }
        return isNeeded;
      });
    }

    function getBucketRamGuageConfigParams(details) {
      if (!details) {
        return;
      }
      return {
        total: details.basicStats.storageTotals.ram.quotaTotalPerNode * details.nodes.length,
        thisAlloc: details.quota.ram,
        otherBuckets: details.basicStats.storageTotals.ram.quotaUsedPerNode * details.nodes.length - details.quota.ram
      };
    }

    function getGuageConfig(details) {
      return {
        total: details.basicStats.storageTotals.hdd.total,
        thisBucket: details.basicStats.diskUsed,
        otherBuckets: details.basicStats.storageTotals.hdd.usedByData - details.basicStats.diskUsed,
        otherData: details.basicStats.storageTotals.hdd.used - details.basicStats.storageTotals.hdd.usedByData
      };
    }

  })(window.rxjs);
