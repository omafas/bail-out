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
import {withLatestFrom, map} from 'rxjs/operators';
import {pipe} from 'rxjs';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from './mn.form.service.js';
import {MnCollectionsService} from './mn.collections.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import template from "./mn.collections.add.item.html";

export {MnCollectionsAddItemComponent}

class MnCollectionsAddItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    NgbActiveModal,
    MnCollectionsService,
    MnFormService,
    MnPoolsService
  ]}

  constructor(activeModal, mnCollectionsService, mnFormService, mnPoolsService) {
    super();
    this.activeModal = activeModal;
    this.form = mnFormService.create(this);
    this.addCollectionHttp = mnCollectionsService.stream.addCollectionHttp;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;

    this.form
      .setFormGroup({name: "", maxTTL: 0})
      .setPackPipe(pipe(
        withLatestFrom(this.isEnterprise),
        map(this.prepareDataForSending.bind(this))))
      .setPostRequest(this.addCollectionHttp)
      .showGlobalSpinner()
      .success(() => {
        mnCollectionsService.stream.updateManifest.next();
        activeModal.close()
      });
  }

  prepareDataForSending([, isEnterprise]) {
    let dataForSending = [this.bucketName, this.scopeName, this.form.group.value.name];
    if (isEnterprise) {
      dataForSending.push(this.form.group.value.maxTTL);
    }

    return dataForSending;
  }
}
