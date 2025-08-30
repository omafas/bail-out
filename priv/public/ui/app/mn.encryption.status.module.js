/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/


import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';

import {MnEncryptionStatusComponent} from './mn.encryption.status.component.js';
import {MnEncryptionForceConfirmationComponent} from './mn.encryption.force.confirmation.component.js';

export {MnEncryptionStatusModule}

class MnEncryptionStatusModule {
  static get annotations() { return [
    new NgModule({
      imports: [
        CommonModule,
        NgbModule,
      ],
      declarations: [
        MnEncryptionStatusComponent,
        MnEncryptionForceConfirmationComponent
      ],
      entryComponents: [
        MnEncryptionForceConfirmationComponent
      ],
      exports: [
        MnEncryptionStatusComponent
      ]
    })
  ]}
}
