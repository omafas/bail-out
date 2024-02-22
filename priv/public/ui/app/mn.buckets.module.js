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
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';
import {ReactiveFormsModule} from '@angular/forms';

import {MnSharedModule} from './mn.shared.module.js';
import {MnBarUsageModule} from './mn.bar.usage.module.js';
import {MnSelectModule} from './mn.select.module.js';
import {MnWarmupProgressModule} from './mn.warmup.progress.module.js';
import {MnBucketsComponent} from './mn.buckets.component.js';
import {MnInputFilterModule} from './mn.input.filter.module.js';
import {MnPipesModule} from './mn.pipes.module.js';
import {MnElementCraneModule} from './mn.element.crane.js';
import {MnSettingsAutoCompactionFormModule} from './mn.settings.auto.compaction.form.module.js';
import {MnBucketItemComponent} from './mn.bucket.item.component.js';
import {MnBucketItemDetailsComponent} from './mn.bucket.item.details.component.js';

import {MnBucketDialogComponent} from './mn.bucket.dialog.component.js';
import {MnBucketDeleteDialogComponent} from './mn.bucket.delete.dialog.component.js';
import {MnBucketFlushDialogComponent} from './mn.bucket.flush.dialog.component.js';
import {MnBucketFullDialogComponent} from './mn.bucket.full.dialog.component.js';

let bucketsState = {
  url: "/buckets?openedBuckets",
  name: "app.admin.buckets",
  data: {
    permissions: "cluster.bucket['.'].settings.read",
    title: "Buckets"
  },
  params: {
    openedBuckets: {
      value: [],
      array: true,
      dynamic: true
    }
  },
  views: {
    "main@app.admin": {
      component: MnBucketsComponent
    }
  }
};

export {MnBucketsModule};

class MnBucketsModule {
  static get annotations() {
    return [
      new NgModule({
        entryComponents: [
          MnBucketDialogComponent,
          MnBucketDeleteDialogComponent,
          MnBucketFlushDialogComponent,
          MnBucketFullDialogComponent
        ],
        declarations: [
          MnBucketsComponent,
          MnBucketItemComponent,
          MnBucketItemDetailsComponent,
          MnBucketDialogComponent,
          MnBucketDeleteDialogComponent,
          MnBucketFlushDialogComponent,
          MnBucketFullDialogComponent
        ],
        imports: [
          MnSharedModule,
          NgbModule,
          MnInputFilterModule,
          MnPipesModule,
          MnBarUsageModule,
          MnSelectModule,
          MnWarmupProgressModule,
          MnElementCraneModule,
          MnSettingsAutoCompactionFormModule,
          ReactiveFormsModule,
          UIRouterModule.forChild({states: [bucketsState]})
        ]
      })
    ]
  }
}
