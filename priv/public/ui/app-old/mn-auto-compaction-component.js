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
mn.components.MnAutoCompaction =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnAutoCompaction, mn.core.MnEventableComponent);

    MnAutoCompaction.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-auto-compaction.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnAutoCompaction.parameters = [
      mn.services.MnSettings,
      mn.services.MnPermissions,
      mn.services.MnForm,
      mn.services.MnHelper
    ];

    MnAutoCompaction.prototype.getValue = getValue;

    return MnAutoCompaction;

    function MnAutoCompaction(mnSettingsService, mnPermissionsService, mnFormService, mnHelperService) {
      mn.core.MnEventableComponent.call(this);

      this.settingsWrite = mnPermissionsService.createPermissionStream("settings!write");
      this.postAutoCompaction = mnSettingsService.stream.postAutoCompaction;
      this.postAutoCompactionValidation = mnSettingsService.stream.postAutoCompactionValidation;

      this.form = mnFormService.create(this);
      this.form
        .setFormGroup({
          allowedTimePeriod: this.form.builder.group({
            abortOutside: false,
            fromHour: null,
            fromMinute: null,
            toHour: null,
            toMinute: null
          }),
          databaseFragmentationThreshold: this.form.builder.group({
            percentage: null,
            size: null
          }),
          viewFragmentationThreshold: this.form.builder.group({
            percentage: null,
            size: null
          }),
          indexFragmentationThreshold: this.form.builder.group({
            percentage: null
          }),
          indexCircularCompaction: this.form.builder.group({
            daysOfWeek: this.form.builder.group(
              mnHelperService.daysOfWeek.reduce(function (acc, day) {
                acc[day] = false;
                return acc;
              }, {})
            ),
            interval: this.form.builder.group({
              abortOutside: null,
              fromHour: null,
              fromMinute: null,
              toHour: null,
              toMinute: null,
            })
          }),
          indexCompactionMode: null,
          parallelDBAndViewCompaction: null,
          purgeInterval: null
        })
        .setPackPipe(Rx.operators.map(this.getValue.bind(this)))
        .setSource(mnSettingsService.stream.getAutoCompactionFirst)
        .setPostRequest(this.postAutoCompaction)
        .setValidation(this.postAutoCompactionValidation, this.securityWrite)
        .clearErrors()
        .successMessage("Settings saved successfully!");


      this.formError = Rx
        .merge(
          this.postAutoCompaction.error,
          this.postAutoCompactionValidation.error
        ).pipe(Rx.operators.startWith({}),
               Rx.operators.map(function (e) {
                 return e && e.errors ? e.errors : {};
               }));
    }

    function getValue() {
      var v = JSON.parse(JSON.stringify(this.form.group.value));
      var icc = v.indexCircularCompaction;
      if (icc) {
        icc.daysOfWeek = Object
          .keys(icc.daysOfWeek)
          .reduce(function (acc, day) {
            if (icc.daysOfWeek[day]) {
              acc.push(day);
            }
            return acc;
          }, [])
          .join(",");
      }
      return v;
    }

  })(window.rxjs);
