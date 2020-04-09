var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnServersFailoverConfirmationDialog =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnServersFailoverConfirmationDialog, mn.core.MnEventableComponent);

    MnServersFailoverConfirmationDialog.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-servers-failover-conf.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnServersFailoverConfirmationDialog.parameters = [
      ngb.NgbActiveModal
    ];

    return MnServersFailoverConfirmationDialog;

    function MnServersFailoverConfirmationDialog(activeModal) {
      mn.core.MnEventableComponent.call(this);
      this.activeModal = activeModal;
    }

  })(window.rxjs);
