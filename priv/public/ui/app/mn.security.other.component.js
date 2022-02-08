/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {UIRouter} from '@uirouter/angular';
import {FormBuilder} from '@angular/forms';
import {combineLatest} from 'rxjs';
import {map, takeUntil} from 'rxjs/operators';
import {all} from 'ramda';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from './mn.form.service.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnHttpGroupRequest} from './mn.http.request.js';

export {MnSecurityOtherComponent};

class MnSecurityOtherComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: 'app/mn.security.other.html',
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

    let isEnterprise = mnPoolsService.stream.isEnterprise;
    let compatVersion55 = mnAdminService.stream.compatVersion55;
    let mnPermissionsStream = mnPermissions.stream;
    let postSettingsSecurity = mnSecurityService.stream.postSettingsSecurity;
    let postLogRedactionRequest = mnSecurityService.stream.postLogRedaction;
    let prepareOtherSettingsFormValues = mnSecurityService.stream.prepareOtherSettingsFormValues;
    this.majorMinorVersion = mnAdminService.stream.majorMinorVersion;
    this.isClusterEncryptionEnabled = mnAdminService.stream.isClusterEncryptionEnabled;

    this.form = mnFormService.create(this)
      .setFormGroup({
        logRedactionLevel: formBuilder.group({
          logRedactionLevel: null
        }),
        settingsSecurity: formBuilder.group({
          uiSessionTimeout: null,
          clusterEncryptionLevel: null
        })
      })
      .setSource(prepareOtherSettingsFormValues)
      .setPackPipe(map(this.packData.bind(this)))
      .setPostRequest(new MnHttpGroupRequest({
        logRedactionLevel: postLogRedactionRequest,
        settingsSecurity: postSettingsSecurity
      }).addSuccess().addError())
      .setReset(uiRouter.stateService.reload)
      .successMessage("Settings saved successfully!");

    mnPermissionsStream
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe((permissions) => {
        let write = permissions.cluster.admin.security.write;
        this.maybeDisableField('settingsSecurity.uiSessionTimeout', write);
        this.maybeDisableField('logRedactionLevel.logRedactionLevel', write);
      });

    combineLatest(mnPermissionsStream, this.isClusterEncryptionEnabled)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(([permissions, enabled]) => {
        let write = permissions.cluster.admin.security.write;
        this.maybeDisableField('settingsSecurity.clusterEncryptionLevel', enabled && write);
      });

    this.mnPermissions = mnPermissionsStream;
    this.isEnterprise = isEnterprise;
    this.postLogRedactionRequest = postLogRedactionRequest;
    this.postSettingsSecurity = postSettingsSecurity;
    this.isEnterpriseAnd55 = combineLatest(isEnterprise, compatVersion55).pipe(map(all(Boolean)));
  }

  packData() {
    let formValue = this.form.group.value;
    let result = new Map();

    let timeout = formValue.settingsSecurity.uiSessionTimeout;
    let redaction = formValue.logRedactionLevel.logRedactionLevel;
    let encryptionLevel = formValue.settingsSecurity.clusterEncryptionLevel;

    let securityValue = {};
    securityValue.uiSessionTimeout = timeout ? (timeout * 60) : "";
    if (encryptionLevel) {
      securityValue.clusterEncryptionLevel = encryptionLevel;
    }

    result.set('settingsSecurity', securityValue);
    if (redaction !== null) {
      result.set('logRedactionLevel', formValue.logRedactionLevel);
    }

    return result;
  }

  maybeDisableField(field, value) {
    this.form.group.get(field)[value ? "enable" : "disable"]();
  }
}
