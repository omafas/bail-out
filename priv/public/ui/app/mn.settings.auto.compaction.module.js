/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '@angular/core';
import {UIRouterModule} from '@uirouter/angular';
import {ReactiveFormsModule} from '@angular/forms';

import {MnSharedModule} from './mn.shared.module.js';
import {MnSettingsAutoCompactionComponent} from './mn.settings.auto.compaction.component.js';
import {MnSettingsAutoCompactionFormModule} from './mn.settings.auto.compaction.form.module.js'
import {MnElementCraneModule} from "./mn.element.crane.js";

const autoCompactionState = {
  url: "/autoCompaction",
  name: "app.admin.settings.autoCompaction",
  component: MnSettingsAutoCompactionComponent,
  data: {
    permissions: "cluster.settings.autocompaction.read"
  }
};

export { MnSettingsAutoCompactionModule };

class MnSettingsAutoCompactionModule {
  static get annotations() { return [
    new NgModule({
      declarations: [
        MnSettingsAutoCompactionComponent
      ],
      imports: [
        MnSharedModule,
        UIRouterModule.forChild({ states: [autoCompactionState] }),
        ReactiveFormsModule,
        MnElementCraneModule,
        MnSettingsAutoCompactionFormModule
      ]
    })
  ]}
}
