/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Component, ChangeDetectionStrategy } from '@angular/core';
import { MnLifeCycleHooksToStream } from './mn.core.js';

import { MnHelperService } from './mn.helper.service.js';


export { MnViewsFilterComponent };

class MnViewsFilterComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-views-filter",
      templateUrl: "app/mn.views.filter.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: ['mnForm', 'defaultFormGroup']
    })
  ]}

  static get parameters() { return [
    MnHelperService
  ]}

  constructor(mnHelperService) {
    super();

    this.caret = mnHelperService.createToggle();
  }

  onReset() {
    return this.mnForm.group.setValue(this.defaultFormGroup);
  }
}
