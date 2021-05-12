/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { NgModule } from '../web_modules/@angular/core.js';
import { MnAuthComponent } from './mn.auth.component.js';
import { MnAuthService } from './mn.auth.service.js';
import { BrowserModule } from '../web_modules/@angular/platform-browser.js';
import { ReactiveFormsModule, Validators } from '../web_modules/@angular/forms.js';
import { Location } from '../web_modules/@angular/common.js';
import { MnSharedModule } from './mn.shared.module.js';

export { MnAuthModule };

class MnAuthModule {
  static annotations = [
    new NgModule({
      declarations: [
        MnAuthComponent
      ],
      imports: [
        BrowserModule,
        ReactiveFormsModule,
        MnSharedModule
      ],
      entryComponents: [
        MnAuthComponent
      ],
      providers: [
        MnAuthService,
        Validators,
        Location
      ]
    })
  ]
}
