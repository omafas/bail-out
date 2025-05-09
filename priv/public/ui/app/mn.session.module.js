/*
Copyright 2020-Present Couchbase, Inc.

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
import {MnSessionComponent} from './mn.session.component.js';

let sessionState = {
  url: '/session',
  name: "app.admin.security.session",
  component: MnSessionComponent,
  data: {
    permissions: "cluster.admin.security.read"
  }
};

export {MnSessionModule};

class MnSessionModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
      ],
      declarations: [
        MnSessionComponent
      ],
      imports: [
        ReactiveFormsModule,
        MnSharedModule,
        UIRouterModule.forChild({ states: [sessionState] })
      ]
    })
  ]}
}
