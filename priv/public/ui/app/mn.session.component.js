/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {map, takeUntil} from 'rxjs/operators';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnFormService} from './mn.form.service.js';
import {MnAdminService} from './mn.admin.service.js';
import template from "./mn.session.html";

export {MnSessionComponent};

class MnSessionComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnSecurityService,
    MnFormService,
    MnPermissions,
    MnAdminService
  ]}

  constructor(mnSecurityService, mnFormService, mnPermissions, mnAdminService) {
    super();

    this.postSession = mnSecurityService.stream.postSession;

    this.form = mnFormService.create(this);

    this.form
      .setFormGroup({uiSessionTimeout: ""})
      .setUnpackPipe(map(v => ({uiSessionTimeout: (Number(v) / 60) || 0})))
      .setSource(mnAdminService.stream.uiSessionTimeout)
      .setPackPipe(map(this.getValue.bind(this)))
      .setPostRequest(this.postSession)
      .successMessage("Settings saved successfully!")
      .clearErrors()
      .showGlobalSpinner();

    this.form.group.disable();

    this.isDisabled =
      mnAdminService.stream.uiSessionTimeout
      .pipe(map(() => mnPermissions.export.cluster.admin.security.write));

    this.isDisabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this));
  }

  maybeDisableField(v) {
    this.form.group[v ? "enable": "disable"]();
  }

  getValue() {
    let timeout = this.form.group.get("uiSessionTimeout").value;
    return {uiSessionTimeout: timeout ? (timeout * 60) : ""};
  }
}
