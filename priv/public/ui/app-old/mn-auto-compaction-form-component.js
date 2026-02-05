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
mn.components.MnAutoCompactionForm =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnAutoCompactionForm, mn.core.MnEventableComponent);

    MnAutoCompactionForm.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-auto-compaction-form.html",
        selector: "mn-auto-compaction-form",
        inputs: [
          "group",
          "error"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnAutoCompactionForm.parameters = [
      mn.services.MnHelper,
      mn.services.MnBuckets,
      mn.services.MnPermissions,
      mn.services.MnPools,
      mn.services.MnWizard,
      mn.services.MnSettings
    ];

    MnAutoCompactionForm.prototype.ngOnInit = ngOnInit;
    MnAutoCompactionForm.prototype.setInitFormValue = setInitFormValue;
    MnAutoCompactionForm.prototype.maybeDisableField = maybeDisableField;
    MnAutoCompactionForm.prototype.maybeDisableCircularField = maybeDisableCircularField;
    MnAutoCompactionForm.prototype.getShorten = getShorten;
    MnAutoCompactionForm.prototype.getInitFormGroupHelper = getInitFormGroupHelper;


    return MnAutoCompactionForm;

    function MnAutoCompactionForm(mnHelperService, mnBucketsService, mnPermissionsService, mnPoolsService, mnWizardService, mnSettingsService) {
      mn.core.MnEventableComponent.call(this);

      this.formGroupHelper = new ng.forms.FormGroup({
        viewSize: new ng.forms.FormControl(),
        viewPercentage: new ng.forms.FormControl(),
        dataSize: new ng.forms.FormControl(),
        dataPercentage: new ng.forms.FormControl(),
        allowedTimePeriod: new ng.forms.FormControl()
      });

      this.mnSettingsService = mnSettingsService;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.getIndexes = mnWizardService.stream.getIndexes;
      this.daysOfWeek = mnHelperService.daysOfWeek;

      this.settingsWrite =
        mnPermissionsService.createPermissionStream("settings!write");

      this.settingsIndexesRead =
        mnPermissionsService.createPermissionStream("settings.indexes!read");
    }

    function ngOnInit() {
      var initData = this.mnSettingsService.stream.getAutoCompactionFirst
          .pipe(Rx.operators.map(this.getShorten.bind(this)),
                Rx.operators.map(this.getInitFormGroupHelper.bind(this)),
                Rx.operators.takeUntil(this.mnOnDestroy));

      initData
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.setInitFormValue.bind(this));

      this.periodError =
        this.error.pipe(Rx.operators.pluck("allowedTimePeriod"));
      this.intervalError =
        this.error.pipe(Rx.operators.pluck("indexCircularCompaction", "interval"));

      Rx.combineLatest(
        this.group.get("indexCompactionMode").valueChanges,
        this.settingsWrite
      )
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.maybeDisableCircularField.bind(this));

      Rx.combineLatest(
        Rx.merge(this.formGroupHelper.valueChanges, initData),
        this.settingsWrite
      )
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.maybeDisableField.bind(this));
    }

    function getShorten(v) {
      return {
        data: v.databaseFragmentationThreshold,
        view: v.viewFragmentationThreshold,
        period: v.allowedTimePeriod,
        undfd: "undefined"
      };
    }

    function getInitFormGroupHelper(v) {
      return {
        viewSize: v.view.size !== v.undfd,
        dataSize: v.data.size !== v.undfd,
        viewPercentage: v.view.percentage !== v.undfd,
        dataPercentage: v.data.percentage !== v.undfd,
        allowedTimePeriod: !!v.period
      };
    }

    function setInitFormValue(v) {
      this.formGroupHelper.patchValue(v, {emitEvent: false});
    }

    function maybeDisableCircularField(v) {
      var event = {emitEvent: true};
      this.group.get("indexCircularCompaction")
      [(v[0] == "circular" && v[1]) ? "enable" : "disable"](event);

      this.group.get("indexFragmentationThreshold")
      [(v[0] == "full" && v[1]) ? "enable" : "disable"](event);
    }

    function maybeDisableField(v) {
      var data = "databaseFragmentationThreshold";
      var view = "viewFragmentationThreshold";
      var period = "allowedTimePeriod";
      var action = function (v) {return v ? "enable": "disable";};
      var event = {emitEvent: false};
      var dataPercentage = v[0].dataPercentage && v[1];
      var dataSize = v[0].dataSize && v[1];
      var viewPercentage = v[0].viewPercentage && v[1];
      var viewSize = v[0].viewSize && v[1];
      var allowedPeriod = v[0].allowedTimePeriod && v[1];

      this.group.get(data + ".percentage")[action(dataPercentage)](event);
      this.group.get(data + ".size")[action(dataSize)](event);
      this.group.get(view + ".percentage")[action(viewPercentage)](event);
      this.group.get(view + ".size")[action(viewSize)](event);
      this.group.get(period)[action(allowedPeriod)](event);

      if (!dataPercentage && !dataSize && !viewPercentage && !viewSize) {
        this.formGroupHelper.get("allowedTimePeriod").disable(event);
        if (v[0].allowedTimePeriod) {
          this.formGroupHelper.get("allowedTimePeriod").setValue(false, event);
        }
      } else {
        this.formGroupHelper.get("allowedTimePeriod").enable(event);
      }

      //trigger validation once
      this.group.updateValueAndValidity({onlySelf: true, emitEvent: true});
    }
  })(window.rxjs);
