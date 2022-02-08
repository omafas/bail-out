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
mn.components.MnGeneralSettings =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnGeneralSettings, mn.core.MnEventableComponent);

    MnGeneralSettings.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-general-settings.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnGeneralSettings.parameters = [
      mn.services.MnPermissions,
      mn.services.MnAdmin,
      mn.services.MnSettings,
      mn.services.MnPools
    ];

    return MnGeneralSettings;

    function MnGeneralSettings(mnPermissionsService, mnAdminService, mnSettingsService, mnPoolsService) {
      mn.core.MnEventableComponent.call(this);

      this.formGroup = new ng.forms.FormGroup({
        clusterName: new ng.forms.FormControl(),
        sendStats: new ng.forms.FormControl(),
        services: new ng.forms.FormGroup({
          field: new ng.forms.FormGroup({
            kv: new ng.forms.FormControl(),
            index: new ng.forms.FormControl(),
            fts: new ng.forms.FormControl(),
            cbas: new ng.forms.FormControl(),
            eventing: new ng.forms.FormControl()
          })
        }),
        autoFailover: new ng.forms.FormGroup({
          enabled: new ng.forms.FormControl(),
          timeout: new ng.forms.FormControl(),
          maxCount: new ng.forms.FormControl(),
          failoverOnDataDiskIssues: new ng.forms.FormGroup({
            enabled: new ng.forms.FormControl(),
            timePeriod: new ng.forms.FormControl(),
          }),
          failoverServerGroup: new ng.forms.FormControl()
        }),
        autoReprovision: new ng.forms.FormGroup({
          count: new ng.forms.FormControl(),
          enabled: new ng.forms.FormControl(),
          max_nodes: new ng.forms.FormControl()
        })
      });

      this.onToggleClick = new Rx.Subject();
      this.toggleSection =
        this.onToggleClick
        .pipe(Rx.operators.scan(R.not, false),
              mn.core.rxOperatorsShareReplay(1));

      this.prettyVersion = mnAdminService.stream.prettyVersion;
      this.getStats = mnSettingsService.stream.getStats;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.compatVersion55 = mnAdminService.stream.compatVersion55;
      this.poolsWrite = mnPermissionsService.createPermissionStream("pools!write");
      this.settingsIndexesWrite =
        mnPermissionsService.createPermissionStream("settings.indexes!write");
      this.settingsIndexesRead =
        mnPermissionsService.createPermissionStream("settings.indexes!read");

      this.memoryQuotasFirst =
        mnAdminService.stream.memoryQuotas.pipe(Rx.operators.first());

      mnAdminService.stream.clusterName
        .pipe(Rx.operators.first())
        .subscribe(function (v) {
          this.formGroup.get("clusterName").setValue(v, {emitEvent: false});
        }.bind(this));

      mnSettingsService.stream.getStats
        .pipe(Rx.operators.first())
        .subscribe(function (v) {
          this.formGroup.get("sendStats").setValue(v.sendStats, {emitEvent: false});
        }.bind(this));

      mnSettingsService.stream.getAutoFailover
        .pipe(Rx.operators.first())
        .subscribe(function (v) {
          this.formGroup.get("autoFailover").patchValue(v, {emitEvent: false});
        }.bind(this));

      mnSettingsService.stream.getAutoReprovision
        .pipe(Rx.operators.first())
        .subscribe(function (v) {
          this.formGroup.get("autoReprovision").setValue(v, {emitEvent: false});
        }.bind(this));

      this.getPhoneHome = mnSettingsService.stream.getPhoneHome
    }

  })(window.rxjs);
