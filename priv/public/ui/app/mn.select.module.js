/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { MnSelectComponent } from './mn.select.component.js';
import { NgModule } from '/ui/web_modules/@angular/core.js';
import { CommonModule } from '/ui/web_modules/@angular/common.js';
import { MnSharedModule } from './mn.shared.module.js';
import { NgbModule } from '/ui/web_modules/@ng-bootstrap/ng-bootstrap.js';
import { MnInputFilterModule } from './mn.input.filter.module.js';

export { MnSelectModule }

class MnSelectModule {
  static get annotations() { return [
    new NgModule({
      imports: [
        CommonModule,
        MnSharedModule,
        NgbModule,
        MnInputFilterModule
      ],
      declarations: [
        MnSelectComponent
      ],
      exports: [
        MnSelectComponent
      ]
    })
  ]}
}
