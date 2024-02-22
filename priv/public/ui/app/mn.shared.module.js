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

import {MnFocusDirective} from './mn.focus.directive.js';
import {MnSpinnerDirective} from './mn.spinner.directive.js';
import {MnMainSpinnerDirective} from './ajs.upgraded.components.js';

export {MnSharedModule}

class MnSharedModule {
  static get annotations() { return [
    new NgModule({
      imports: [
        ReactiveFormsModule,
        CommonModule,
        // ngb.NgbModule,
      ],
      declarations: [
        MnFocusDirective,
        MnMainSpinnerDirective,
        MnSpinnerDirective
        // mn.components.MnAutoCompactionForm,
        // mn.components.MnPeriod,
        // mn.components.MnServicesConfig,
        // mn.components.MnSearch,
        // mn.components.MnSearchField
      ],
      exports: [
        MnFocusDirective,
        MnMainSpinnerDirective,
        MnSpinnerDirective,
        CommonModule
        // mn.components.MnServicesConfig,
        // mn.components.MnAutoCompactionForm,
        // mn.components.MnSearch,
        // mn.components.MnSearchField
      ]
    })
  ]}
}
