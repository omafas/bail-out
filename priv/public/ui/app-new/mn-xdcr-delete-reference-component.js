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
mn.components.MnXDCRDeleteReference =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnXDCRDeleteReference, mn.core.MnEventableComponent);

    MnXDCRDeleteReference.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-xdcr-delete-reference.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush,
        inputs: [
          "reference"
        ]
      })
    ];

    MnXDCRDeleteReference.parameters = [
      ngb.NgbActiveModal,
      mn.services.MnXDCR,
      mn.services.MnForm
    ];

    return MnXDCRDeleteReference;

    function MnXDCRDeleteReference(activeModal, mnXDCRService, mnFormService) {
      mn.core.MnEventableComponent.call(this);

      this.form = mnFormService.create(this)
        .setPackPipe(Rx.operators.map(function () {
          return this.reference.name
        }.bind(this)))
        .setPostRequest(mnXDCRService.stream.deleteRemoteClusters)
        .successMessage("Replication deleted successfully!")
        .success(function () {
          activeModal.close();
          mnXDCRService.stream.updateRemoteClusters.next();
        });

      this.activeModal = activeModal;
    }

  })(window.rxjs);
