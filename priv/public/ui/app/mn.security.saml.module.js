/*
Copyright 2023-Present Couchbase, Inc.

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
import {ClipboardModule} from 'ngx-clipboard';

import {MnSharedModule} from './mn.shared.module.js';
import {MnPipesModule} from './mn.pipes.module.js';
import {MnSelectModule} from './mn.select.module.js';
import {MnSecuritySamlComponent} from './mn.security.saml.component.js';

import { MnFileReaderDirective } from "./ajs.upgraded.components.js";

let samlState = {
  url: '/saml',
  name: "app.admin.security.saml",
  data: {
    permissions: "cluster.admin.security.external.read",
    enterprise: true,
    compat: "atLeast76"
  },
  component: MnSecuritySamlComponent
};

export {MnSecuritySamlModule};

class MnSecuritySamlModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
      ],
      declarations: [
        MnFileReaderDirective,

        MnSecuritySamlComponent,
      ],
      imports: [
        MnPipesModule,
        MnSelectModule,
        ReactiveFormsModule,
        MnSharedModule,
        NgbModule,
        ClipboardModule,
        UIRouterModule.forChild({ states: [samlState] })
      ]
    })
  ]}
}
