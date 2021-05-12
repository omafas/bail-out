/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {pluck, map, takeUntil} from '/ui/web_modules/rxjs/operators.js';
import {MnPermissions} from '/ui/app/ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnFormService} from './mn.form.service.js';

export {MnSecurityLogRedactionComponent};

class MnSecurityLogRedactionComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "/ui/app/mn.security.log.redaction.html",
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnSecurityService,
    MnFormService,
    MnPermissions
  ]}

  constructor(mnSecurityService, mnFormService, mnPermissions) {
    super();

    this.postLogRedaction = mnSecurityService.stream.postLogRedaction;

    this.form = mnFormService.create(this);

    this.form
      .setFormGroup({logRedactionLevel: ""})
      .setSource(mnSecurityService.stream.getLogRedaction)
      .setPackPipe(map(this.getValue.bind(this)))
      .setPostRequest(this.postLogRedaction)
      .successMessage("Settings saved successfully!")
      .clearErrors()
      .showGlobalSpinner();

    this.form.group.disable();

    this.isDisabled =
      mnSecurityService.stream.getLogRedaction
      .pipe(map(v => mnPermissions.export.cluster.admin.security.write));

    this.isDisabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this));
  }

  maybeDisableField(value) {
    this.form.group[value ? "enable" : "disable"]();
  }

  getValue() {
    return {logRedactionLevel: this.form.group.get("logRedactionLevel").value};
  }
}
