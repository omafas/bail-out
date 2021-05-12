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
mn.components.MnSessionTimeoutDialog =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnSessionTimeoutDialog, mn.core.MnEventableComponent);

    MnSessionTimeoutDialog.annotations = [
      new ng.core.Component({
        selector: "mn-session-timeout-dialog-component",
        templateUrl: "app-new/mn-session-timeout-dialog.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnSessionTimeoutDialog.parameters = [
      ngb.NgbActiveModal
    ];

    return MnSessionTimeoutDialog;

    function MnSessionTimeoutDialog(activeModal) {
      mn.core.MnEventableComponent.call(this);
      this.activeModal = activeModal;

      var time = (Number(localStorage.getItem("uiSessionTimeout")) - 30000) / 1000;


      this.formGroup = new ng.forms.FormGroup({});

      this.time =
        Rx.interval(1000)
        .pipe(
          Rx.operators.scan(function(acc) {
            return --acc;
          }, time),
          Rx.operators.startWith(time));
    }
  })(window.rxjs);
