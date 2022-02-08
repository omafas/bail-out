/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {BehaviorSubject} from 'rxjs';

import {MnWizardService} from './mn.wizard.service.js';

export {MnPathFieldComponent};

class MnPathFieldComponent {
  static get annotations() { return [
    new Component({
      selector: "mn-path-field",
      templateUrl: "app/mn.path.field.html",
      inputs: [
        "control",
        "controlName"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnWizardService
  ]}

  ngOnInit() {
    this.lookUpPath = this.createLookUpStream(this.control.valueChanges);
    setTimeout(function () {
      //trigger storageGroup.valueChanges for lookUpIndexPath,lookUpDBPath
      this.control.setValue(this.control.value);
    }.bind(this), 0);
  }

  constructor(mnWizardService) {
    this.focusFieldSubject = new BehaviorSubject(true);
    this.createLookUpStream = mnWizardService.createLookUpStream.bind(mnWizardService);
  }
}
