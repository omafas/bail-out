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
import {map} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnXDCRService} from "./mn.xdcr.service.js";
import {MnFormService} from "./mn.form.service.js";
import template from "./mn.xdcr.delete.ref.html";

export {MnXDCRDeleteRefComponent};

class MnXDCRDeleteRefComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ]
    })
  ]}

  static get parameters() { return [
    NgbActiveModal,
    MnXDCRService,
    MnFormService
  ]}

  constructor(activeModal, mnXDCRService, mnFormService) {
    super();

    this.form = mnFormService.create(this)
      .setPackPipe(map(() => this.item.name))
      .setPostRequest(mnXDCRService.stream.deleteRemoteClusters)
      .successMessage("Replication deleted successfully!")
      .showGlobalSpinner()
      .success(() => {
        activeModal.close();
        mnXDCRService.stream.updateRemoteClusters.next();
      });

    this.activeModal = activeModal;
    this.deleteRemoteClusters = mnXDCRService.stream.deleteRemoteClusters;
  }
}
