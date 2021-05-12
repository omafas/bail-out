/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '/ui/web_modules/@angular/core.js';
import {UIRouterModule} from "/ui/web_modules/@uirouter/angular.js";
import {MnSharedModule} from './mn.shared.module.js';
import {ReactiveFormsModule} from '/ui/web_modules/@angular/forms.js';
import {NgbModule} from '/ui/web_modules/@ng-bootstrap/ng-bootstrap.js';

import {MnSessionComponent} from './mn.session.component.js';
import {MnSessionServiceModule} from './mn.session.service.js';

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
        MnSessionServiceModule,
        MnSharedModule,
        UIRouterModule.forChild({ states: [sessionState] })
      ],
      providers: []
    })
  ]}
}
