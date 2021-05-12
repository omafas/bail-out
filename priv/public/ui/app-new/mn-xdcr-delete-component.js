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
mn.components.MnXDCRDelete =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnXDCRDelete, mn.core.MnEventableComponent);

    MnXDCRDelete.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-xdcr-delete.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush,
        inputs: [
          "replication"
        ]
      })
    ];

    MnXDCRDelete.parameters = [
      ngb.NgbActiveModal,
      mn.services.MnXDCR,
      mn.services.MnForm,
      mn.services.MnTasks
    ];

    return MnXDCRDelete;

    function MnXDCRDelete(activeModal, mnXDCRService, mnFormServices, mnTasksService) {
      mn.core.MnEventableComponent.call(this);
      this.activeModal = activeModal;

      this.form = mnFormServices.create(this)
        .setPackPipe(Rx.operators.map(function () {
          return this.replication.id;
        }.bind(this)))
        .setPostRequest(mnXDCRService.stream.deleteCancelXDCR)
        .successMessage("Replication deleted successfully!")
        .success(function () {
          activeModal.close();
          mnTasksService.stream.updateTasks.next();
        });
    }

  })(window.rxjs);
