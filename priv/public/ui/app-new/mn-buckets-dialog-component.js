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
mn.components.MnBucketsDialog =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnBucketsDialogComponent, mn.core.MnEventableComponent);

    MnBucketsDialogComponent.annotations = [
      new ng.core.Component({
        selector: "mn-buckets-dialog-component",
        templateUrl: "app-new/mn-buckets-dialog.html",
        inputs: [
          "bucket"
        ],
      })
    ];

    MnBucketsDialogComponent.parameters = [
      ngb.NgbActiveModal,
      mn.services.MnBuckets,
      mn.services.MnPools,
      mn.services.MnAdmin,
      mn.services.MnServers,
      mn.pipes.MnBytesToMB
    ];

    return MnBucketsDialogComponent;

    function MnBucketsDialogComponent(activeModal, mnBucketsService, mnPoolsService, mnAdminService, mnServersService, mnBytesToMB) {
      mn.core.MnEventableComponent.call(this);

      var bucketsDialogForm = new ng.forms.FormGroup({
        name: new ng.forms.FormControl({value: null, disabled: false}),
        ramQuotaMB: new ng.forms.FormControl(),
        bucketType: new ng.forms.FormControl({value: "membase", disabled: false}),
        replicaNumber: new ng.forms.FormControl(1),
        replicaIndex: new ng.forms.FormControl(false),
        evictionPolicy: new ng.forms.FormControl("valueOnly"),
        evictionPolicyEphemeral: new ng.forms.FormControl("noEviction"),
        maxTTL: new ng.forms.FormControl(0),
        compressionMode: new ng.forms.FormControl("passive"),
        conflictResolutionType: new ng.forms.FormControl("seqno"),
        flushEnabled: new ng.forms.FormControl(false),
        threadsNumber: new ng.forms.FormControl("3"),
        purgeInterval: new ng.forms.FormControl(1)
      });

      var bucketsDialogHelperForm = new ng.forms.FormGroup({
        replicaNumberEnabled: new ng.forms.FormControl(null),
        maxTTLEnabled: new ng.forms.FormControl(null)
      });

      this.focusFieldSubject = new Rx.BehaviorSubject(false);
      this.onSubmit = new Rx.Subject();
      this.activeModal = activeModal;
      this.bucketsDialogForm = bucketsDialogForm;
      this.bucketsDialogHelperForm = bucketsDialogHelperForm;
      this.bucketHttp = mnBucketsService.stream.bucketHttp;
      this.compatVersion55 = mnAdminService.stream.compatVersion55;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;

      var ramSummary =
          this.bucketHttp.success.pipe(
            Rx.operators.merge(this.bucketHttp.error),
            Rx.operators.pluck("summaries", "ramSummary"),
            Rx.operators.filter(Boolean)
          );

      var bucketFormChanges =
          bucketsDialogForm.valueChanges.pipe(
            Rx.operators.takeUntil(this.mnOnDestroy)
          );

      var bucketFormHelperChanges =
          bucketsDialogHelperForm.valueChanges.pipe(
            Rx.operators.takeUntil(this.mnOnDestroy)
          );


      this.bucketRamGuage =
        ramSummary.pipe(
          Rx.operators.map(mnBucketsService.getBucketRamGuageConfig),
          mn.core.rxOperatorsShareReplay(1)
        );

      this.bucketTotalRamGuage =
        this.bucketRamGuage.pipe(
          Rx.operators.map(getBucketTotalRamGuage),
          mn.core.rxOperatorsShareReplay(1)
        );

      bucketFormChanges.pipe(
        Rx.operators.map(bucketsDialogForm.getRawValue.bind(bucketsDialogForm)),
        Rx.operators.withLatestFrom(
          mnPoolsService.stream.isEnterprise,
          mnAdminService.stream.compatVersion55
        ),
        Rx.operators.debounceTime(0) //wait until all values are setted and stabilized
      ).subscribe(validateBucketForm.bind(this));

      bucketFormHelperChanges.pipe(
        Rx.operators.pluck("maxTTLEnabled")
      ).subscribe(toggleMaxTTl.bind(this));

      bucketFormHelperChanges.pipe(
        Rx.operators.pluck("replicaNumberEnabled")
      ).subscribe(toggleReplicaIndex.bind(this));

      this.mnOnInit.pipe(
        Rx.operators.withLatestFrom(
          mnAdminService.stream.getPoolsDefault,
          mnServersService.stream.serviceSpecificActiveNodes
            .pipe(Rx.operators.switchMap(R.prop("kv")))
        ),
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(setInitialValues.bind(this));

      this.onSubmit.pipe(
        Rx.operators.map(bucketsDialogForm.getRawValue.bind(bucketsDialogForm)),
        Rx.operators.withLatestFrom(
          mnPoolsService.stream.isEnterprise,
          mnAdminService.stream.compatVersion55
        ),
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(saveAutoCompaction.bind(this));

      this.onSubmit.pipe(
        Rx.operators.switchMap(getFirstSuccess.bind(this))
      ).subscribe(function () {
        activeModal.dismiss();
        mnBucketsService.stream.updateBucketsPoller.next();
      });

      function getFirstSuccess() {
        return this.bucketHttp.success.pipe(
          Rx.operators.first()
        );
      }

      function toggleMaxTTl(isMaxTTLEnabled) {
        if (!isMaxTTLEnabled) {
          bucketsDialogForm.get("maxTTL").setValue(0);
        }
        bucketsDialogForm.get("maxTTL")
        [isMaxTTLEnabled ? "enable" : "disable"]({onlySelf: true});
      }

      function threadsEvictionWarning(fieldName) {
        var initValue = bucketsDialogForm.get(fieldName).value;
        this[fieldName + "Warning"] =
          bucketFormChanges.pipe(
            Rx.operators.pluck(fieldName),
            Rx.operators.map(function (value) {
              return (value != initValue) ?
                ('Changing ' + (fieldName === 'evictionPolicy' ?
                                'eviction policy' :
                                'bucket priority')  +
                 ' will restart the bucket. This will lead to closing all' +
                 ' open connections and some downtime') : "";
            })
          );
      }

      function toggleReplicaIndex(replicaNumberEnabled) {
        var values = {};

        if (replicaNumberEnabled) {
          values["replicaNumber"] = (bucketsDialogForm.get("replicaNumber").value || 1);
        } else {
          values["replicaNumber"] = 0;
          values["replicaIndex"] = false;
        }
        bucketsDialogForm.patchValue(values);
        bucketsDialogForm.get("replicaIndex")
        [!(replicaNumberEnabled && this.isNew) ? "disable" : "enable"]({onlySelf: true});
      }

      function getBucketTotalRamGuage(bucketGuage) {
        return (bucketGuage.bottomLeft.name === "overcommitted") ?
          bucketGuage.topLeft.value : bucketGuage.topRight.value;
      }

      function saveAutoCompaction(values) {
        this.bucketHttp.post([
          mnBucketsService.prepareBucketConfigForSaving(
            values[0], values[1], values[2], this.isNew
          ), false, this.bucket && this.bucket.uri
        ]);
      }

      function validateBucketForm(values) {
        this.bucketHttp.post([
          mnBucketsService.prepareBucketConfigForSaving(
            values[0], values[1], values[2], this.isNew
          ), true, this.bucket && this.bucket.uri
        ]);
      }

      function getInitialRamQuotaMB(values) {
        var ram = values[1].storageTotals.ram;
        return mnBytesToMB.transform((ram.quotaTotal - ram.quotaUsed) / values[2].length);
      }

      function setInitialValues(values) {
        var bucket;
        this.isNew = !this.bucket;

        if (this.isNew) {
          this.focusFieldSubject.next("name");
          bucketsDialogForm.get("ramQuotaMB").setValue(getInitialRamQuotaMB(values));
        } else {

          bucket = _.clone(this.bucket, true);
          bucket.ramQuotaMB = mnBytesToMB.transform(bucket.quota.ram);
          bucket.evictionPolicyEphemeral = bucket.evictionPolicy;
          bucket.threadsNumber = bucket.threadsNumber.toString();
          bucket.flushEnabled = !!bucket.controllers.flush;
          bucket.replicaIndex = !!bucket.replicaIndex;
          bucketsDialogForm.patchValue(bucket);

          bucketsDialogForm.get("conflictResolutionType").disable({onlySelf: true});
          bucketsDialogForm.get("name").disable({onlySelf: true});
          bucketsDialogForm.get("bucketType").disable({onlySelf: true});
          bucketsDialogForm.get("evictionPolicyEphemeral").disable({onlySelf: true});

          this.focusFieldSubject.next("ramQuotaMB");
        }

        bucketsDialogHelperForm
          .patchValue({
            replicaNumberEnabled: bucketsDialogForm.get("replicaNumber").value != 0,
            maxTTLEnabled: bucketsDialogForm.get("maxTTL").value != 0
          });

        if (!this.isNew) {
          (['threadsNumber','evictionPolicy']).forEach(threadsEvictionWarning.bind(this));
        }
      }
    }
  })(window.rxjs);
