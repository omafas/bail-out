/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Component, ChangeDetectionStrategy } from '@angular/core';
import { Validators } from '@angular/forms';
import { MnLifeCycleHooksToStream } from './mn.core.js';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UIRouter } from '@uirouter/angular';
import { map } from 'rxjs/operators';
import { pipe } from 'rxjs';

import { MnFormService } from './mn.form.service.js';
import { MnViewsListService } from './mn.views.list.service.js';
import template from "./mn.views.copy.dialog.html";

export { MnViewsCopyDialogComponent };

class MnViewsCopyDialogComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    NgbActiveModal,
    MnViewsListService,
    UIRouter
  ]}

  constructor(mnFormService, activeModal, mnViewsListService, uiRouter) {
    super();

    this.activeModal = activeModal;
    this.mnViewsListService = mnViewsListService;

    this.commonBucket = uiRouter.globals.params.commonBucket;

    this.form = mnFormService.create(this)
      .setFormGroup({ ddocName: [null, [Validators.required]] })
      .setPackPipe(pipe(map(this.getDdocData.bind(this))))
      .setPostRequest(mnViewsListService.stream.postDdoc)
      .showGlobalSpinner()
      .success(() => {
        this.activeModal.close();
        this.mnViewsListService.stream.updateDdocsPoller.next();
        uiRouter.stateService.reload('app.admin.views');
      });
  }

  getDdocUrl() {
    let name = this.form.group.get('ddocName').value;

    return this.mnViewsListService.getDdocUrl([this.commonBucket, name, '_design/dev_']);
  }

  getDdocData() {
    return { url: this.getDdocUrl(), json: this.json };
  }
}
