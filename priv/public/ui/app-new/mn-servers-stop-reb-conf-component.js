var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnServersStopRebalanceDialog =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnServersStopRebalanceDialog, mn.core.MnEventableComponent);

    MnServersStopRebalanceDialog.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-servers-stop-reb-conf.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnServersStopRebalanceDialog.parameters = [
      ngb.NgbActiveModal
    ];

    return MnServersStopRebalanceDialog;

    function MnServersStopRebalanceDialog(activeModal) {
      mn.core.MnEventableComponent.call(this);
      this.activeModal = activeModal;
    }

  })(window.rxjs);
