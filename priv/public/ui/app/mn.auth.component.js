/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {Validators} from '@angular/forms';
import {BehaviorSubject} from 'rxjs';
import {UIRouter} from '@uirouter/angular';

import {MnAuthService} from './mn.auth.service.js';
import {MnFormService} from './mn.form.service.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnPools, $rootScope} from './ajs.upgraded.providers.js';
import template from "./mn.auth.html";

export {MnAuthComponent};

class MnAuthComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    MnAuthService,
    UIRouter,
    MnPools,
    $rootScope
  ]}

  constructor(mnFormService, mnAuthService, uiRouter, mnPools, $rootScope) {
    super();
    this.focusFieldSubject = new BehaviorSubject(true);

    this.postUILogin = mnAuthService.stream.postUILogin;

    this.form = mnFormService.create(this)
      .setFormGroup({
        user: ['', Validators.required],
        password: ['', Validators.required]})
      .setPostRequest(this.postUILogin)
      .showGlobalSpinner()
      .success(() => {
        $rootScope.mnGlobalSpinnerFlag = true;
        mnPools.clearCache();
        uiRouter.urlRouter.sync();
      });
  }
}
