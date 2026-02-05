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
