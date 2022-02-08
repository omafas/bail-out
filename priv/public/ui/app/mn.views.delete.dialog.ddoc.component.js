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
import { UIRouter } from '@uirouter/angular';
import { map } from 'rxjs/operators';
import { pipe } from 'rxjs';

import { MnFormService } from './mn.form.service.js';
import { MnViewsListService } from './mn.views.list.service.js';

export { MnViewsDeleteDialogDdocComponent };

class MnViewsDeleteDialogDdocComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "app/mn.views.delete.dialog.ddoc.html",
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
    this.commonBucket = uiRouter.globals.params.commonBucket;

    this.form = mnFormService.create(this)
      .setFormGroup({})
      .setPackPipe(pipe(map(this.getDdocUrl.bind(this))))
      .setPostRequest(mnViewsListService.stream.deleteDdoc)
      .showGlobalSpinner()
      .successMessage("Ddoc Deleted successfully!")
      .success(() => {
        activeModal.close();
        mnViewsListService.stream.updateDdocsPoller.next();
        uiRouter.stateService.reload();
      });
  }

  getDdocUrl() {
    return '/couchBase/' + encodeURIComponent(this.commonBucket) + '/' + this.ddocName;
  }
}
