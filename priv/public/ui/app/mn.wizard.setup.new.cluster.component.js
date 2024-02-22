/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {UIRouter} from '@uirouter/angular';
import {BehaviorSubject, pipe} from 'rxjs';
import {filter, map} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnWizardService} from './mn.wizard.service.js';
import {MnFormService} from "./mn.form.service.js";
import template from "./mn.wizard.setup.new.cluster.html";

export {MnWizardSetupNewClusterComponent};

class MnWizardSetupNewClusterComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnHelperService,
    MnWizardService,
    MnFormService,
    UIRouter
  ]}

  constructor(mnHelperService, mnWizardService, mnFormService, uiRouter) {
    super();

    this.focusFieldSubject = new BehaviorSubject("clusterName");
    this.uiRouter = uiRouter;
    this.postSettingsWebHttp = mnWizardService.stream.postSettingsWebHttp;

    this.form = mnFormService.create(this);
    this.form
      .setPackPipe(pipe(
        filter(this.canSubmit.bind(this)),
        map(this.getValues.bind(this))))
      .setFormGroup(mnWizardService.wizardForm.newCluster)
      .setPostRequest(this.postSettingsWebHttp)
      .showGlobalSpinner()
      .success(this.onSuccess.bind(this));

    this.form.group.setValidators([mnHelperService.validateEqual("user.password",
                                                                 "user.passwordVerify",
                                                                 "passwordMismatch")]);
  }

  getValues() {
    return [this.form.group.value.user, true];
  }

  canSubmit() {
    return !this.form.group.invalid;
  }

  onSuccess() {
    this.uiRouter.stateService.go('app.wizard.termsAndConditions', null, {location: false});
  }
}
