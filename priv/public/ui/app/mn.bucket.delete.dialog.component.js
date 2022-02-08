/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';
import {map} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from './mn.form.service.js';
import {MnBucketsService} from './mn.buckets.service.js';

export {MnBucketDeleteDialogComponent};

class MnBucketDeleteDialogComponent extends MnLifeCycleHooksToStream {

  static get annotations() { return [
    new Component({
      templateUrl: "app/mn.bucket.delete.dialog.html",
      inputs: [
        'bucket'
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    NgbActiveModal,
    MnFormService,
    MnBucketsService
  ]}

  constructor(activeModal, mnFormService, mnBucketsService, uiRouter) {
    super();

    this.activeModal = activeModal;
    this.uiRouter = uiRouter;

    this.form = mnFormService.create(this)
      .setFormGroup({})
      .setPackPipe(map(() => this.bucket))
      .setPostRequest(mnBucketsService.stream.deleteBucket)
      .showGlobalSpinner()
      .successMessage('Bucket deleted successfully!')
      .success(() => activeModal.dismiss());
  }
}
