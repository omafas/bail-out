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
