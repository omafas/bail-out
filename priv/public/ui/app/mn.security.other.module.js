/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '../web_modules/@angular/core.js';
import {UIRouterModule} from "../web_modules/@uirouter/angular.js";
import {MnSharedModule} from './mn.shared.module.js';
import {NgbModule} from '../web_modules/@ng-bootstrap/ng-bootstrap.js';
import {ReactiveFormsModule} from '../web_modules/@angular/forms.js';
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
      ],
      providers: []
    })
  ]}
}
