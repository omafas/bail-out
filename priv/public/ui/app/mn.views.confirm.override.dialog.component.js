/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Component, ChangeDetectionStrategy } from '@angular/core';
import { MnLifeCycleHooksToStream } from './mn.core.js';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { map } from 'rxjs/operators';
import { pipe } from 'rxjs';

import { MnFormService } from './mn.form.service.js';
import { MnViewsListService } from './mn.views.list.service.js';
import { UIRouter } from '@uirouter/angular';
import template from "./mn.views.confirm.override.dialog.html";

export { MnViewsConfirmOverrideDialogComponent };

class MnViewsConfirmOverrideDialogComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    NgbActiveModal,
    MnViewsListService,
    MnFormService,
    UIRouter
  ]}

  constructor(activeModal, mnViewsListService, mnFormService, uiRouter) {
    super();
    this.activeModal = activeModal;

    this.form = mnFormService.create(this)
      .setFormGroup({})
      .setPackPipe(pipe(map(this.getDdocData.bind(this))))
      .setPostRequest(mnViewsListService.stream.postDdoc)
      .showGlobalSpinner()
      .success(() => {
        activeModal.close();
        uiRouter.stateService.go('.', { type: 'production' });
      });
  }

  getDdocData() {
    return { url: this.ddocUrl, json: this.json };
  }
}
