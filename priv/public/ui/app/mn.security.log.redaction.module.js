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
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';

import {MnSharedModule} from './mn.shared.module.js';
import {MnSecurityLogRedactionComponent} from './mn.security.log.redaction.component.js';

let logRedactionState = {
  url: '/redaction',
  name: "app.admin.security.redaction",
  component: MnSecurityLogRedactionComponent,
  data: {
    compat: "atLeast55",
    enterprise: true
  }
};

export {MnSecurityLogRedactionModule};

class MnSecurityLogRedactionModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
      ],
      declarations: [
        MnSecurityLogRedactionComponent
      ],
      imports: [
        NgbModule,
        ReactiveFormsModule,
        MnSharedModule,
        UIRouterModule.forChild({ states: [logRedactionState] })
      ]
    })
  ]}
}
