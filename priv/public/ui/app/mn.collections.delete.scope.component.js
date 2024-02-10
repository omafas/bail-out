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
import {MnFormService} from "./mn.form.service.js";
import {MnCollectionsService} from './mn.collections.service.js';
import template from "./mn.collections.delete.scope.html";

export {MnCollectionsDeleteScopeComponent}

class MnCollectionsDeleteScopeComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    NgbActiveModal,
    MnCollectionsService,
    MnFormService
  ]}

  constructor(activeModal, mnCollectionsService, mnFormService) {
    super();
    this.activeModal = activeModal;
    this.form = mnFormService.create(this);

    this.form
      .setFormGroup({})
      .setPackPipe(map(() => [this.bucketName, this.scopeName]))
      .setPostRequest(mnCollectionsService.stream.deleteScopeHttp)
      .showGlobalSpinner()
      .success(() => {
        mnCollectionsService.stream.updateManifest.next();
        activeModal.close();
      });

  }
}
