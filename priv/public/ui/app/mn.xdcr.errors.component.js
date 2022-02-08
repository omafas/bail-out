/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';

import {MnFormService} from './mn.form.service.js';

export {MnXDCRErrorsComponent};

class MnXDCRErrorsComponent {
  static get annotations() { return [
    new Component({
      templateUrl: "app/mn.xdcr.errors.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "errors"
      ]
    })
  ]}

  static get parameters() { return [
    NgbActiveModal,
    MnFormService
  ]}

  constructor(activeModal, mnFormService) {
    this.activeModal = activeModal;
    this.form = mnFormService.create(this)
      .setFormGroup({})
      .hasNoPostRequest()
  }
}
