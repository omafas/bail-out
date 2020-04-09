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
