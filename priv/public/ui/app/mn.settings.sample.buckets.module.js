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
import {MnSettingsSampleBucketsComponent} from './mn.settings.sample.buckets.component.js';
import {MnElementCraneModule} from "./mn.element.crane.js";
import {MnPipesModule} from "./mn.pipes.module.js";


const sampleBucketsState = {
  url: "/sampleBuckets",
  name: "app.admin.settings.sampleBuckets",
  component: MnSettingsSampleBucketsComponent,
  data: {
    permissions: "cluster.settings.read"
  }
};

export {MnSettingsSampleBucketsModule};

class MnSettingsSampleBucketsModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [],
      declarations: [
        MnSettingsSampleBucketsComponent
      ],
      imports: [
        MnSharedModule,
        MnElementCraneModule,
        UIRouterModule.forChild({ states: [sampleBucketsState] }),
        ReactiveFormsModule,
        MnPipesModule
      ]
    })
  ]}
}
