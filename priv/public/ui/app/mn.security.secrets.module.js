/*
Copyright 2024-Present Couchbase, Inc.

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
import {DatePipe} from '@angular/common';

import {MnSharedModule} from './mn.shared.module.js';
import {MnSelectModule} from './mn.select.module.js';
import {MnSecuritySecretsComponent} from './mn.security.secrets.component.js';
import {MnSecuritySecretsAddDialogComponent} from './mn.security.secrets.add.dialog.component.js';
import {MnSecuritySecretsDeleteDialogComponent} from './mn.security.secrets.delete.dialog.component.js';
import {MnSecuritySecretsDeleteKeyDialogComponent} from './mn.security.secrets.delete.key.dialog.component.js';
import {MnSecuritySecretsEncryptionDialogComponent} from './mn.security.secrets.encryption.dialog.component.js';

import {MnSecuritySecretsItemComponent} from './mn.security.secrets.item.component.js';
import {MnSecuritySecretsItemDetailsComponent} from './mn.security.secrets.item.details.component.js';
import {MnInputFilterModule} from './mn.input.filter.module.js';
import {MnPipesModule} from './mn.pipes.module.js';
import {MnElementCraneModule} from './mn.element.crane.js';
import {MnSecuritySecretsReencryptConfirmationComponent} from './mn.security.secrets.reencrypt.confirmation.component.js';
import {MnEncryptionStatusModule} from "./mn.encryption.status.module.js";
import {MnTimezoneDetailsModule} from './mn.timezone.details.module.js';

let secretsState = {
  url: "/secrets?openedSecrets",
  name: "app.admin.security.secrets",
  params: {
    openedSecrets: {
      value: [],
      array: true,
      dynamic: true
    }
  },
  data: {
    permissions: "cluster.admin.security.read",
    title: "Encryption at Rest",
    enterprise: true
  },
  component: MnSecuritySecretsComponent
};

export {MnSecuritySecretsModule};

class MnSecuritySecretsModule {
  static get annotations() {
    return [
      new NgModule({
        entryComponents: [
          MnSecuritySecretsAddDialogComponent,
          MnSecuritySecretsDeleteDialogComponent,
          MnSecuritySecretsDeleteKeyDialogComponent,
          MnSecuritySecretsEncryptionDialogComponent,
        ],
        declarations: [
          MnSecuritySecretsAddDialogComponent,
          MnSecuritySecretsDeleteDialogComponent,
          MnSecuritySecretsDeleteKeyDialogComponent,
          MnSecuritySecretsEncryptionDialogComponent,
          MnSecuritySecretsComponent,
          MnSecuritySecretsItemDetailsComponent,
          MnSecuritySecretsItemComponent,
        ],
        imports: [
          MnSharedModule,
          NgbModule,
          MnInputFilterModule,
          MnPipesModule,
          MnSelectModule,
          MnElementCraneModule,
          ReactiveFormsModule,
          MnEncryptionStatusModule,
          MnTimezoneDetailsModule,
          UIRouterModule.forChild({states: [secretsState]})
        ],
        providers: [
          DatePipe
        ]
      })
    ]
  }
}
