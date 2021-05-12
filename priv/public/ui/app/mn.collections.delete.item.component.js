/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {NgbActiveModal} from '/ui/web_modules/@ng-bootstrap/ng-bootstrap.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {takeUntil} from '/ui/web_modules/rxjs/operators.js';
import {map} from "/ui/web_modules/rxjs/operators.js";

import {MnFormService} from "./mn.form.service.js";
import {MnCollectionsService} from './mn.collections.service.js';

export {MnCollectionsDeleteItemComponent}

class MnCollectionsDeleteItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "app/mn.collections.delete.item.html",
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
      .setPackPipe(map(() => [this.bucketName, this.scopeName, this.collectionName]))
      .setPostRequest(mnCollectionsService.stream.deleteCollectionHttp)
      .showGlobalSpinner()
      .success(() => {
        mnCollectionsService.stream.updateManifest.next();
        activeModal.close();
      });

  }
}
