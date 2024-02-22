/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {ReactiveFormsModule} from '@angular/forms';

import {MnInputFilterComponent} from './mn.input.filter.component.js';
import {MnSharedModule} from './mn.shared.module.js';

export {MnInputFilterModule}

class MnInputFilterModule {
  static get annotations() { return [
    new NgModule({
      imports: [
        ReactiveFormsModule,
        CommonModule,
        MnSharedModule
      ],
      declarations: [
        MnInputFilterComponent
      ],
      exports: [
        MnInputFilterComponent
      ]
    })
  ]}
}
