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
mn.components.MnXDCRAddReplication =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnXDCRAddReplication, mn.core.MnEventableComponent);

    MnXDCRAddReplication.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-xdcr-add-replication.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnXDCRAddReplication.parameters = [
      mn.services.MnForm,
      mn.services.MnPools,
      mn.services.MnXDCR,
      mn.services.MnBuckets,
      mn.services.MnAdmin,
      mn.services.MnAlerts,
      mn.services.MnTasks,
      ngb.NgbActiveModal
    ];

    return MnXDCRAddReplication;

    function MnXDCRAddReplication(mnFormService, mnPoolsService, mnXDCRService, mnBucketsService, mnAdminService, mnAlertsService, mnTasksService, activeModal) {
      mn.core.MnEventableComponent.call(this);

      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.activeModal = activeModal;
      this.bucketsMembaseEphemeral = mnBucketsService.stream.bucketsMembaseEphemeral;
      this.getSettingsReplications = mnXDCRService.stream.getSettingsReplications;
      this.remoteClusters = mnXDCRService.stream.getRemoteClustersFiltered;
      this.postCreateReplication = mnXDCRService.stream.postCreateReplication;
      this.postSettingsReplicationsValidation =
        mnXDCRService.stream.postSettingsReplicationsValidation;

      this.showAdvancedSettingsClick = new Rx.Subject();
      this.showAdvancedSettings =
        this.showAdvancedSettingsClick.pipe(Rx.operators.scan(R.not, false),
                                            mn.core.rxOperatorsShareReplay(1));

      this.form = mnFormService.create(this)
        .setFormGroup({fromBucket: null,
                       toCluster: null,
                       toBucket: null,
                       type: "xmem",
                       compressionType: null,
                       sourceNozzlePerNode: null,
                       targetNozzlePerNode: null,
                       checkpointInterval: null,
                       workerBatchSize: null,
                       docBatchSizeKb: null,
                       failureRestartInterval: null,
                       optimisticReplicationThreshold: null,
                       statsInterval: null,
                       networkUsageLimit: null,
                       logLevel: null})
        .setPackPipe(Rx.pipe(
          Rx.operators.withLatestFrom(this.isEnterprise, mnAdminService.stream.compatVersion55),
          Rx.operators.map(mnXDCRService.prepareReplicationSettigns.bind(this))))
        .setSource(this.getSettingsReplications)
        .setPostRequest(this.postCreateReplication)
        .setValidation(this.postSettingsReplicationsValidation)
        .clearErrors()
        .success(function (data) {
          activeModal.close();
          mnTasksService.stream.updateTasks.next();
          data = JSON.parse(data);
          if (!!(data.warnings && data.warnings.length)) {
            data.warnings.forEach(function (message) {
              mnAlertsService.stream.alert.next({
                message: message,
                type: "warning"
              });
            });
          } else {
            mnAlertsService.success("Replication created successfully!")();
          }
        });


    }

  })(window.rxjs);
