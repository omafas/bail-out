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
mn.components.MnStorageMode =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnStorageMode, mn.core.MnEventableComponent);

    MnStorageMode.annotations = [
      new ng.core.Component({
        selector: "mn-storage-mode",
        templateUrl: "app-new/mn-storage-mode.html",
        inputs: [
          "control",
          "indexFlagChanges",
          "permissionsIndexWrite"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnStorageMode.parameters = [
      mn.services.MnWizard,
      mn.services.MnPools
    ];

    MnStorageMode.prototype.ngOnInit = ngOnInit;
    MnStorageMode.prototype.doDisableControl = doDisableControl;

    return MnStorageMode;

    function MnStorageMode(mnWizardService, mnPoolsService) {
      mn.core.MnEventableComponent.call(this);
      this.indexesHttp = mnWizardService.stream.indexesHttp;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
    }

    function ngOnInit() {
      var isNotEnterprise =
          this.isEnterprise.pipe(Rx.operators.map(mn.helper.invert));

      var isFirstValueForestDB =
          this.control.valueChanges.pipe(
            Rx.operators.first(),
            Rx.operators.map(function (v) {
              return v === 'forestdb';
            })
          );

      this.showForestDB =
        Rx.combineLatest(
          isNotEnterprise,
          isFirstValueForestDB
        )
        .pipe(Rx.operators.map(_.curry(_.some)(_, Boolean)));

      this.showPlasma = this.isEnterprise;

      Rx.combineLatest(
        isNotEnterprise,
        (this.indexFlagChanges || Rx.of(true)).pipe(Rx.operators.map(mn.helper.invert)),
        (this.permissionsIndexWrite || Rx.of(true)).pipe(Rx.operators.map(mn.helper.invert))
      ).pipe(
        Rx.operators.map(_.curry(_.some)(_, Boolean)),
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(this.doDisableControl.bind(this));
    }

    function doDisableControl(value) {
      this.control[value ? "disable" : "enable"]();
    }

  })(window.rxjs);
