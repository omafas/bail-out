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
import {BehaviorSubject, NEVER} from 'rxjs';
import {pluck, switchMap, distinctUntilChanged, shareReplay, map} from 'rxjs/operators';
import {UIRouter} from '@uirouter/angular';


import {MnAuthService} from './mn.auth.service.js';
import {MnFormService} from './mn.form.service.js';
import {MnAdminService} from './mn.admin.service.js';
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
    MnAdminService,
    UIRouter,
    MnPools,
    $rootScope,
  ]}

  constructor(mnFormService, mnAuthService, mnAdminService, uiRouter, mnPools, $rootScope) {
    super();
    this.focusFieldSubject = new BehaviorSubject(true);

    this.postUILogin = mnAuthService.stream.postUILogin;
    this.postUISAMLLogin = mnAuthService.stream.postUISAMLLogin;
    this.getAuthMethods = mnAuthService.stream.getAuthMethods;

    this.samlsError = uiRouter.globals.params$
      .pipe(pluck("samlErrorMsgId"),
            distinctUntilChanged(),
            switchMap((id) => id ? mnAdminService.getSamlError(id) : NEVER),
            shareReplay({refCount: true, bufferSize: 1}));

    this.form = mnFormService.create(this)
      .setFormGroup({
        user: ['', Validators.required],
        password: ['', Validators.required]})
      .setPackPipe(map(() => ([
        this.form.group.value,
        false
      ])))
      .setPostRequest(this.postUILogin)
      .showGlobalSpinner()
      .success(() => {
        $rootScope.mnGlobalSpinnerFlag = true;
        mnPools.clearCache();
        uiRouter.urlRouter.sync();
      });

      this.certAuth = mnFormService.create(this)
        .setFormGroup({})
        .setPackPipe(map(() => ([null, true])))
        .setPostRequest(this.postUILogin)
        .hasNoHandler()
  }
}
