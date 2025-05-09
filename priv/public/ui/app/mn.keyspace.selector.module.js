/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {ReactiveFormsModule} from '@angular/forms';
import {MnSharedModule} from './mn.shared.module.js';

import {MnInputFilterModule} from './mn.input.filter.module.js';
import {MnKeyspaceSelectorComponent} from './mn.keyspace.selector.component.js';

export {MnKeyspaceSelectorModule};

class MnKeyspaceSelectorModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
        MnKeyspaceSelectorComponent
      ],
      declarations: [
        MnKeyspaceSelectorComponent
      ],
      exports: [
        MnKeyspaceSelectorComponent
      ],
      imports: [
        CommonModule,
        MnInputFilterModule,
        ReactiveFormsModule,
        MnSharedModule
      ],
    })
  ]}
}
