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
import {ReactiveFormsModule} from '/ui/web_modules/@angular/forms.js';
import {NgbModule} from '/ui/web_modules/@ng-bootstrap/ng-bootstrap.js';
import {MnSharedModule} from './mn.shared.module.js';

import {MnPipesModule} from './mn.pipes.module.js';
import {MnSelectModule} from './mn.select.module.js';

import {MnSecurityAuditComponent} from './mn.security.audit.component.js';
import {MnSecurityAuditItemComponent} from './mn.security.audit.item.component.js';
import {MnSecurityService} from './mn.security.service.js';

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
      ],
      providers: [
        MnSecurityService
      ]
    })
  ]}
}
