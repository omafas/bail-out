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

import {MnSecurityLogRedactionComponent} from './mn.security.log.redaction.component.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnBucketsService} from './mn.buckets.service.js';
import {MnFormService} from './mn.form.service.js';

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
      ],
      providers: [
        MnSecurityService,
        MnBucketsService
      ]
    })
  ]}
}
