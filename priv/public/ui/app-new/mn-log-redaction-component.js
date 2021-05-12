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
mn.components.MnLogRedaction =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnLogRedaction, mn.core.MnEventableComponent);

    MnLogRedaction.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-log-redaction.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnLogRedaction.parameters = [
      mn.services.MnSecurity,
      mn.services.MnPermissions,
      mn.services.MnAlerts
    ];

    MnLogRedaction.prototype.maybeDisableField = maybeDisableField;
    MnLogRedaction.prototype.setInitialValue = setInitialValue;
    MnLogRedaction.prototype.getValue = getValue;

    return MnLogRedaction;

    function MnLogRedaction(mnSecurityService, mnPermissionsService, mnAlertsService) {
      mn.core.MnEventableComponent.call(this);

      var postLogRedaction = mnSecurityService.stream.postLogRedaction;

      this.securityWrite = mnPermissionsService.createPermissionStream("admin.security!write");
      this.onSubmit = new Rx.Subject();
      this.logRedactionForm = new ng.forms.FormGroup({
        logRedactionLevel: new ng.forms.FormControl(null)
      });

      mnSecurityService.stream.getLogRedaction
        .pipe(Rx.operators.first())
        .subscribe(this.setInitialValue.bind(this));

      this.securityWrite
        .pipe(Rx.operators.first())
        .subscribe(this.maybeDisableField.bind(this));

      postLogRedaction.success
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(mnAlertsService.success("Settings saved successfully!"))

      this.onSubmit.pipe(
        Rx.operators.map(this.getValue.bind(this)),
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(postLogRedaction.post.bind(postLogRedaction));
    }

    function getValue() {
      return {logRedactionLevel: this.logRedactionForm.get("logRedactionLevel").value};
    }

    function maybeDisableField(value) {
      this.logRedactionForm.get("logRedactionLevel")[value ? "enable" : "disable"]({onlySelf: true});
    }

    function setInitialValue(value) {
      this.logRedactionForm.patchValue(value);
    }

  })(window.rxjs);
