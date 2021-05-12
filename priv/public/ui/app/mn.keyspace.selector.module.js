/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { NgModule } from '/ui/web_modules/@angular/core.js';
import { CommonModule } from '/ui/web_modules/@angular/common.js';
import { ReactiveFormsModule } from '/ui/web_modules/@angular/forms.js';
import { MnInputFilterModule } from './mn.input.filter.module.js';
import { MnCollectionsServiceModule } from './mn.collections.service.js';

import { MnKeyspaceSelectorComponent } from "/ui/app/mn.keyspace.selector.component.js";
import { MnFormService } from "./mn.form.service.js";

export { MnKeyspaceSelectorModule };

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
        MnCollectionsServiceModule
      ],
      providers: [
        MnFormService
      ]
    })
  ]}
}
