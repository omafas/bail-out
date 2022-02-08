/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {UIRouter} from '@uirouter/angular';
import {Component, ChangeDetectionStrategy} from '@angular/core';
import {BehaviorSubject, pipe} from 'rxjs';
import {filter, map, switchMap, combineLatest} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnWizardService} from './mn.wizard.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnFormService} from "./mn.form.service.js";
import {MnAuthService} from "./mn.auth.service.js";
import {MnAdminService} from "./mn.admin.service.js";
import {MnPools} from "./ajs.upgraded.providers.js";
import {clone} from 'ramda';

export {MnWizardTermsAndConditionsComponent};

class MnWizardTermsAndConditionsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "app/mn.wizard.terms.and.conditions.html",
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnAdminService,
    MnWizardService,
    MnPoolsService,
    MnFormService,
    MnAuthService,
    UIRouter,
    MnPools
  ]}

  constructor(mnAdminService, mnWizardService, mnPoolsService, mnFormService, mnAuthService, uiRouter, mnPools) {
    super();

    this.focusFieldSubject = new BehaviorSubject(true);
    this.form = mnFormService.create(this);
    this.defaultForm = mnFormService.create(this);

    mnWizardService.wizardForm.termsAndConditions.get("agree").setValue(false);

    this.uiRouter = uiRouter;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.wizardForm = mnWizardService.wizardForm;
    this.initialValues = mnWizardService.initialValues;
    this.majorMinorVersion = mnAdminService.stream.majorMinorVersion;

    this.groupHttp = mnWizardService.stream.groupHttp;
    this.secondGroupHttp = mnWizardService.stream.secondGroupHttp;
    this.servicesHttp = mnWizardService.stream.servicesHttp;
    this.mnWizardService = mnWizardService;

    this.license = mnPoolsService.stream.isEnterprise
      .pipe(switchMap(this.getLicense.bind(this)));

    this.termsHref = mnPoolsService.stream.isEnterprise
      .pipe(map(this.getTermsAndCond.bind(this)));

    this.form
      .setFormGroup(mnWizardService.wizardForm.termsAndConditions)
      .setPackPipe(filter(this.isValid.bind(this)))
      .hasNoPostRequest()
      .success(this.onSuccess.bind(this));

    this.defaultForm
      .setPackPipe(pipe(
        filter(this.isValid.bind(this)),
        combineLatest(mnPoolsService.stream.mnServices, mnPoolsService.stream.isEnterprise),
        map(this.getClusterInitConfig.bind(this))))
      .setPostRequest(mnWizardService.stream.postClusterInitHttp)
      .setPackPipe(map(mnWizardService.getUserCreds.bind(mnWizardService)))
      .setPostRequest(mnAuthService.stream.postUILogin)
      .clearErrors()
      .showGlobalSpinner()
      .success(() => {
        mnPools.clearCache();
        uiRouter.urlRouter.sync();
      });
  }

  onSuccess() {
    this.uiRouter.stateService.go('app.wizard.clusterConfiguration', null, {location: false});
  }

  isValid() {
    return !this.form.group.invalid;
  }

  getLicense(isEnterprise) {
    return isEnterprise ?
      this.mnWizardService.getEELicense():
      this.mnWizardService.getCELicense();
  }

  getTermsAndCond(isEnterprise) {
    return isEnterprise ?
      'https://www.couchbase.com/LA03012021' :
      'https://www.couchbase.com/community-license-agreement04272021';
  }

  getClusterInitConfig([, services, isEnterprise]) {
    let userData = clone(this.wizardForm.newCluster.value.user);
    delete userData.passwordVerify;
    userData.port = "SAME";
    return Object.assign({
        hostname: this.initialValues.hostname,
        services: services.join(","),
        sendStats: this.wizardForm.termsAndConditions.get("enableStats").value,
        clusterName: this.wizardForm.newCluster.get("clusterName").value,
        setDefaultMemQuotas : true,
        indexerStorageMode: isEnterprise ? "plasma" : "forestdb"
      }, userData)
  }
}
