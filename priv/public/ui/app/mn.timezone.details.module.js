/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/


import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';

import {MnSharedModule} from './mn.shared.module.js';
import {MnTimezoneDetailsComponent} from './mn.timezone.details.component.js';

export {MnTimezoneDetailsModule}

class MnTimezoneDetailsModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
        MnTimezoneDetailsComponent
      ],
      declarations: [
        MnTimezoneDetailsComponent
      ],
      imports: [
        CommonModule,
        MnSharedModule,
        NgbModule,
      ],
      exports: [
        MnTimezoneDetailsComponent
      ]
    })
  ]}
}
