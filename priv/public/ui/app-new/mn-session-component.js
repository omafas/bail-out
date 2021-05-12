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
mn.components.MnSession =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnSession, mn.core.MnEventableComponent);

    MnSession.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-session.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnSession.parameters = [
      mn.services.MnSecurity,
      mn.services.MnPermissions,
      mn.services.MnAlerts,
      mn.services.MnAdmin
    ];

    MnSession.prototype.setInitFormValue = setInitFormValue;
    MnSession.prototype.maybeDisableField = maybeDisableField;
    MnSession.prototype.getValue = getValue;

    return MnSession;

    function MnSession(mnSecurityService, mnPermissionsService, mnAlertsService, mnAdminService) {
      mn.core.MnEventableComponent.call(this);

      this.onSubmit = new Rx.Subject();
      this.sessionForm = new ng.forms.FormGroup({
        uiSessionTimeout: new ng.forms.FormControl()
      });

      mnAdminService.stream.uiSessionTimeout
        .pipe(Rx.operators.first(),
              Rx.operators.map(function (v) {
                return (Number(v) / 60) || 0;
              }))
        .subscribe(this.setInitFormValue.bind(this))

      this.postSession = mnSecurityService.stream.postSession;
      this.securityWrite = mnPermissionsService.createPermissionStream("admin.security!write");

      this.securityWrite
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.maybeDisableField.bind(this));

      this.postSession.success
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(mnAlertsService.success("Settings saved successfully!"))

      this.onSubmit.pipe(
        Rx.operators.tap(this.postSession.clearError.bind(this.postSession)),
        Rx.operators.map(this.getValue.bind(this)),
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(this.postSession.post.bind(this.postSession));
    }

    function setInitFormValue(v) {
      this.sessionForm.patchValue({uiSessionTimeout: v});
    }

    function maybeDisableField(v) {
      this.sessionForm.get("uiSessionTimeout")[v ? "enable": "disable"]({onlySelf: true});
    }

    function getValue() {
      return {uiSessionTimeout: this.sessionForm.get("uiSessionTimeout").value * 60};
    }

  })(window.rxjs);
