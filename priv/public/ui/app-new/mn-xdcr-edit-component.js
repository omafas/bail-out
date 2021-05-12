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
mn.components.MnXDCREdit =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnXDCREdit, mn.core.MnEventableComponent);

    MnXDCREdit.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-xdcr-edit.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush,
        inputs: [
          "replication"
        ]
      })
    ];

    MnXDCREdit.parameters = [
      ngb.NgbActiveModal,
      mn.services.MnXDCR,
      mn.services.MnForm,
      mn.services.MnTasks,
      mn.services.MnPools,
      mn.services.MnAdmin
    ];

    MnXDCREdit.prototype.ngOnInit = ngOnInit;

    return MnXDCREdit;

    function ngOnInit() {
      this.form = this.mnFormService.create(this)
        .setFormGroup({compressionType: null,
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
          Rx.operators.withLatestFrom(this.isEnterprise, this.compatVersion55),
          Rx.operators.map(this.prepareReplicationSettigns.bind(this)),
          Rx.operators.map(function (data) {
            return [this.replication.id, data];
          }.bind(this))))
        .setUnpackPipe(Rx.operators.map(function (source) {
          return Object.assign({}, source[0], source[1]);
        }))
        .setSource(Rx.combineLatest(
          this.getSettingsReplications,
          this.createGetSettingsReplicationsPipe(this.replication.id)
        ))
        .setPostRequest(this.postSettingsReplications)
        .setValidation(this.postSettingsReplicationsValidation)
        .successMessage("Settings saved successfully!")
        .clearErrors()
        .success(function () {
          this.activeModal.close();
        }.bind(this));
    }

    function MnXDCREdit(activeModal, mnXDCRService, mnFormService, mnTasksService, mnPoolsService, mnAdminService) {
      mn.core.MnEventableComponent.call(this);

      this.activeModal = activeModal;
      this.mnFormService = mnFormService;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.compatVersion55 = mnAdminService.stream.compatVersion55;
      this.prepareReplicationSettigns = mnXDCRService.prepareReplicationSettigns;
      this.getSettingsReplications = mnXDCRService.stream.getSettingsReplications
      this.postSettingsReplicationsValidation =
        mnXDCRService.stream.postSettingsReplicationsValidation;
      this.postSettingsReplications =
        mnXDCRService.stream.postSettingsReplications;
      this.createGetSettingsReplicationsPipe =
        mnXDCRService.createGetSettingsReplicationsPipe.bind(mnXDCRService);
    }

  })(window.rxjs);
