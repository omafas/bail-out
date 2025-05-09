/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/
import {Component, ChangeDetectionStrategy} from '@angular/core';
import {Validators} from '@angular/forms';
import {BehaviorSubject} from 'rxjs';
import {map} from 'rxjs/operators';
import {UIRouter} from '@uirouter/angular';

import {MnHelperService} from './mn.helper.service.js';
import {MnAuthService} from './mn.auth.service.js';
import {MnFormService} from './mn.form.service.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import template from "./mn.auth.change.password.html";

export {MnAuthChangePasswordComponent};

class MnAuthChangePasswordComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnHelperService,
    MnFormService,
    MnAuthService,
    UIRouter
  ]}

  constructor(mnHelperService, mnFormService, mnAuthService, uiRouter) {
    super();
    this.focusFieldSubject = new BehaviorSubject(true);
    let auth = '';

    if (uiRouter.stateService.params.auth) {
      //extract basic auth and remove it from state;
      auth = uiRouter.stateService.params.auth;
      uiRouter.stateService.go('app.authChangePassword', {auth: null}, {location: false});
    }

    this.postChangePassword = mnAuthService.stream.postChangePassword;

    this.form = mnFormService.create(this)
      .setFormGroup({
        password: ['', [Validators.required, Validators.minLength(6)]],
        confirmPassword: ['', Validators.required]
      })
      .setPackPipe(map(() => ([
        this.form.group.value,
        auth
      ])))
      .setPostRequest(this.postChangePassword)
      .showGlobalSpinner()
      .success(() => {
        uiRouter.stateService.go('app.auth', null, {location: false});
      });

    this.form.group.setValidators([
      mnHelperService.validateEqual("password", "confirmPassword", "passwordMismatch")]);
  }
}
