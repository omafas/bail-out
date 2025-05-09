/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '@angular/core';
import {CommonModule, Location} from '@angular/common';
import {UIRouterModule} from '@uirouter/angular';
import {ReactiveFormsModule, Validators} from '@angular/forms';

import {MnAuthComponent} from './mn.auth.component.js';
import {MnAuthChangePasswordComponent} from './mn.auth.change.password.component.js';
import {MnSharedModule} from './mn.shared.module.js';

let authState = {
  name: "app.auth",
  params: {
    samlErrorMsgId: {
      value: null,
      squash: true,
      dynamic: true
    }
  },
  component: MnAuthComponent
};

let authChangePasswordState = {
  name: "app.authChangePassword",
  component: MnAuthChangePasswordComponent,
  params: {
    auth: {
      value: null,
      dynamic: true
    }
  },
};

export { MnAuthModule };

class MnAuthModule {
  static get annotations() { return [
    new NgModule({
      declarations: [
        MnAuthComponent,
        MnAuthChangePasswordComponent
      ],
      imports: [
        CommonModule,
        ReactiveFormsModule,
        MnSharedModule,
        UIRouterModule.forChild({ states: [authState, authChangePasswordState] })
      ],
      entryComponents: [
        MnAuthComponent,
        MnAuthChangePasswordComponent
      ],
      providers: [
        Validators,
        Location
      ]
    })
  ]}
}
