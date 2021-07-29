/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '../web_modules/@angular/core.js';
import {UIRouter} from '../web_modules/@uirouter/angular.js';
import {combineLatest} from '../web_modules/rxjs.js';
import {map, takeUntil, withLatestFrom} from '../web_modules/rxjs/operators.js';
import {FormBuilder} from '../web_modules/@angular/forms.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from './mn.form.service.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnHttpGroupRequest} from './mn.http.request.js';
import {MnPermissions} from './ajs.upgraded.providers.js';

export {MnSecurityOtherComponent};

class MnSecurityOtherComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: new URL('./mn.security.other.html', import.meta.url).pathname,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    FormBuilder,
    MnSecurityService,
    UIRouter,
    MnPermissions,
    MnAdminService,
    MnPoolsService
  ]}

  constructor(mnFormService, formBuilder, mnSecurityService, uiRouter,
              mnPermissions, mnAdminService, mnPoolsService) {
    super();

    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    let compatVersion55 = mnAdminService.stream.compatVersion55;
    this.isEnterpriseAnd55 = combineLatest(this.isEnterprise, compatVersion55)
      .pipe(map(([isEnterprise, compatVersion55]) => isEnterprise && compatVersion55));

    this.mnPermissions = mnPermissions.stream;

    this.postSettingsSecurity = mnSecurityService.stream.postSettingsSecurity;
    this.postLogRedactionRequest = mnSecurityService.stream.postLogRedaction;

    let uiSessionTimeout = mnAdminService.stream.uiSessionTimeout;
    let shouldGetLogRedaction = mnSecurityService.stream.shouldGetLogRedaction;
    let shouldGetClusterEncryption = mnSecurityService.stream.shouldGetClusterEncryption;
    let otherSettings = mnSecurityService.stream.otherSettings;

    this.form = mnFormService.create(this)
      .setSource(otherSettings)
      .setFormGroup({
        logRedactionLevel: formBuilder.group({
          logRedactionLevel: null
        }),
        settingsSecurity: formBuilder.group({
          uiSessionTimeout: null,
          clusterEncryptionLevel: null
        })
      })
      .setPackPipe(map(this.prepareData.bind(this)))
      .setPostRequest(new MnHttpGroupRequest({
        logRedactionLevel: this.postLogRedactionRequest,
        settingsSecurity: this.postSettingsSecurity
      }).addSuccess().addError())
      .setReset(uiRouter.stateService.reload)
      .successMessage("Settings saved successfully!");

    shouldGetLogRedaction
      .pipe(withLatestFrom(mnPermissions.stream),
            map(([logRedaction, permissions]) => permissions.cluster.admin.security.write),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this, 'logRedactionLevel.logRedactionLevel'));

    uiSessionTimeout
      .pipe(withLatestFrom(mnPermissions.stream),
            map(([uiSessionTimeout, permissions]) => permissions.cluster.admin.security.write),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this, 'settingsSecurity.uiSessionTimeout'));

    shouldGetClusterEncryption
      .pipe(withLatestFrom(mnPermissions.stream),
            map(([clusterEncryptionLevel, permissions]) => clusterEncryptionLevel && permissions.cluster.admin.security.write),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this, 'settingsSecurity.clusterEncryptionLevel'));
  }

  prepareSessionData() {
    let timeout = this.form.group.get("settingsSecurity.uiSessionTimeout").value;
    return timeout ? (timeout * 60) : "";
  }

  prepareData() {
    let formValue = this.form.group.value;
    let result = new Map();

    if (formValue.logRedactionLevel.logRedactionLevel !== null) {
      result.set('logRedactionLevel', formValue.logRedactionLevel);
    }

    let securityValue = {
      uiSessionTimeout: this.prepareSessionData()
    };

    let encryptionLevel = formValue.settingsSecurity.clusterEncryptionLevel;
    if (encryptionLevel) {
      securityValue.clusterEncryptionLevel = encryptionLevel;
    }
    result.set('settingsSecurity', securityValue);

    return result;
  }

  maybeDisableField(field, value) {
    this.form.group.get(field)[value ? "enable" : "disable"]();
  }
}
