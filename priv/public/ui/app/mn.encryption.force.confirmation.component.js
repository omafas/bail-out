/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in
the file licenses/APL2.txt.
*/

import { Component, ChangeDetectionStrategy } from '@angular/core';
import { MnLifeCycleHooksToStream } from './mn.core.js';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import template from "./mn.encryption.force.confirmation.html";

export { MnEncryptionForceConfirmationComponent };

class MnEncryptionForceConfirmationComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "itemType",
        "bucketName",
        "isEncryptionEnabled"
      ]
    })
  ]}

  static get parameters() { return [
    NgbActiveModal
  ]}

  constructor(activeModal) {
    super();
    this.activeModal = activeModal;
  }

  confirm() {
    this.activeModal.close(true);
  }

  cancel() {
    this.activeModal.close(false);
  }
}
