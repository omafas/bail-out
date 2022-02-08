/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Component, ChangeDetectionStrategy } from '@angular/core';
import { FormBuilder, Validators } from '@angular/forms';
import { MnLifeCycleHooksToStream } from './mn.core.js';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UIRouter } from '@uirouter/angular';
import { map } from 'rxjs/operators';
import { pipe } from 'rxjs';

import { MnFormService } from './mn.form.service.js';
import { MnViewsListService } from './mn.views.list.service.js';
import template from "./mn.views.create.dialog.html";

export { MnViewsCreateDialogComponent };

class MnViewsCreateDialogComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    NgbActiveModal,
    FormBuilder,
    MnViewsListService,
    UIRouter
  ]}

  constructor(mnFormService, activeModal, formBuilder, mnViewsListService, uiRouter) {
    super();

    this.activeModal = activeModal;
    this.mnViewsListService = mnViewsListService;
    this.postDdoc = mnViewsListService.stream.postDdoc;

    this.commonBucket = uiRouter.globals.params.commonBucket;

    this.isViewsEditingSection =
      uiRouter.globals.current.name.includes('views.editing.result');

    this.form = mnFormService.create(this)
      .setFormGroup({
        ddoc: formBuilder.group({
          name: [null, [Validators.required, this.nameEqaulityValidator()]],
          view : [null, [Validators.required, this.viewEqaulityValidator()]]
        })
      })
      .setPackPipe(pipe(map(this.getDdocData.bind(this))))
      .setPostRequest(this.postDdoc)
      .showGlobalSpinner()
      .success(() => {
        this.activeModal.close();
        this.mnViewsListService.stream.updateDdocsPoller.next();
        uiRouter.stateService.reload('app.admin.views');
      });
  }

  ngOnInit() {
    if (this.ddocName) {
      this.form.group.get('ddoc.name').disable();
      this.form.group.get('ddoc').patchValue({
        name: this.mnViewsListService.removeDesignPrefix(this.ddocName)
      });
    }
  }

  getDdocUrl() {
    let name = this.form.group.get('ddoc.name').value;

    return this.mnViewsListService.getDdocUrl([this.commonBucket, name, '_design/dev_']);
  }

  getDdocData() {
    let url = this.getDdocUrl();
    let json = { views: this.views || {} };

    let viewObject = json.views[this.form.group.get('ddoc.view').value] = {
      map: 'function (doc, meta) {\n  emit(meta.id, null);\n}'
    };

    if (this.mapJson) {
      viewObject.map = this.mapJson;
    }

    if (this.reduceJson) {
      viewObject.reduce = this.reduceJson;
    }

    return { url, json };
  }

  nameEqaulityValidator() {
    return (control) => {
      let value = control.value;

      if (!this.ddocs) {
        return null;
      }

      let ddocNames = this.ddocs.map(ddoc => this.mnViewsListService.removeDesignPrefix(ddoc.doc.meta.id));

      return ddocNames.includes(value) ? { alreadyExists : true } : null;
    }
  }

  viewEqaulityValidator() {
    return (control) => {
      let value = control.value;

      if (!this.views) {
        return null;
      }

      return Object.keys(this.views).includes(value) ? { alreadyExists : true } : null;
    }
  }
}
