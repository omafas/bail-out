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
import {MnSelectModule} from './mn.select.module.js';
import {MnSecurityOtherComponent} from './mn.security.other.component.js';

let securityOtherState = {
  url: '/other',
  name: "app.admin.security.other",
  component: MnSecurityOtherComponent
};

export {MnSecurityOtherModule};

class MnSecurityOtherModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
      ],
      declarations: [
        MnSecurityOtherComponent
      ],
      imports: [
        MnSharedModule,
        NgbModule,
        ReactiveFormsModule,
        MnSelectModule,
        UIRouterModule.forChild({states: [securityOtherState]})
      ]
    })
  ]}
}
