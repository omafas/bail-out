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

import {MnInputFilterModule} from './mn.input.filter.module.js';
import {MnPipesModule} from './mn.pipes.module.js';
import {MnSelectableNodesComponent} from './mn.selectable.nodes.component.js';
import {MnSelectableNodeItemComponent} from './mn.selectable.node.item.component.js';

import {MnSharedModule} from './mn.shared.module.js';

export {MnSelectableNodesModule}

class MnSelectableNodesModule {
  static get annotations() { return [
    new NgModule({
      imports: [
        CommonModule,
        ReactiveFormsModule,
        MnInputFilterModule,
        MnPipesModule,
        MnSharedModule
      ],
      declarations: [
        MnSelectableNodesComponent,
        MnSelectableNodeItemComponent
      ],
      exports: [
        MnSelectableNodesComponent
      ]
    })
  ]}
}
