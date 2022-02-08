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
import {MnPipesModule} from './mn.pipes.module.js';
import {MnSelectModule} from './mn.select.module.js';
import {MnSecurityAuditComponent} from './mn.security.audit.component.js';
import {MnSecurityAuditItemComponent} from './mn.security.audit.item.component.js';

let auditState = {
  url: '/audit',
  name: "app.admin.security.audit",
  data: {
    enterprise: true
  },
  component: MnSecurityAuditComponent
};

export {MnSecurityAuditModule};

class MnSecurityAuditModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
      ],
      declarations: [
        MnSecurityAuditComponent,
        MnSecurityAuditItemComponent
      ],
      imports: [
        MnPipesModule,
        MnSelectModule,
        ReactiveFormsModule,
        MnSharedModule,
        NgbModule,
        UIRouterModule.forChild({ states: [auditState] })
      ]
    })
  ]}
}
