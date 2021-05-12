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
mn.components.MnPathField =
  (function (Rx) {
    "use strict";

    MnPathField.annotations = [
      new ng.core.Component({
        selector: "mn-path-field",
        templateUrl: "app-new/mn-path-field.html",
        inputs: [
          "control",
          "controlName"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnPathField.parameters = [
      mn.services.MnWizard
    ];

    MnPathField.prototype.ngOnInit = ngOnInit;

    return MnPathField;

    function ngOnInit() {
      this.lookUpPath = this.createLookUpStream(this.control.valueChanges);
      setTimeout(function () {
        //trigger storageGroup.valueChanges for lookUpIndexPath,lookUpDBPath
        this.control.setValue(this.control.value);
      }.bind(this), 0);
    }

    function MnPathField(mnWizardService) {
      this.focusFieldSubject = new Rx.BehaviorSubject(true);
      this.createLookUpStream = mnWizardService.createLookUpStream.bind(mnWizardService);
    }
  })(window.rxjs);
