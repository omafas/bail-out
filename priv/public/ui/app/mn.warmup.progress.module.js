/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '@angular/core';

import {MnSharedModule} from './mn.shared.module.js';
import {MnPipesModule} from './mn.pipes.module.js';
import {MnWarmupProgressComponent} from './mn.warmup.progress.component.js';

export {MnWarmupProgressModule}

class MnWarmupProgressModule {
  static get annotations() { return [
    new NgModule({
      imports: [
        MnSharedModule,
        MnPipesModule
      ],
      declarations: [
        MnWarmupProgressComponent
      ],
      exports: [
        MnWarmupProgressComponent
      ]
    })
  ]}
}
