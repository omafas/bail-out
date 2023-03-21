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
import {map, withLatestFrom, first, filter, startWith} from 'rxjs/operators';
import {pipe, combineLatest} from 'rxjs';
import {clone} from 'ramda';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnWizardService} from './mn.wizard.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnFormService} from './mn.form.service.js';
import {MnAuthService} from './mn.auth.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnPools, $rootScope} from './ajs.upgraded.providers.js';
import template from "./mn.wizard.new.cluster.config.html";

export {MnWizardNewClusterConfigComponent};

class MnWizardNewClusterConfigComponent extends MnLifeCycleHooksToStream {

  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    MnWizardService,
    MnAdminService,
    MnPoolsService,
    // MnAppService,
    MnAuthService,
    UIRouter,
    MnPools,
    $rootScope
  ]}

  constructor(mnFormService, mnWizardService, mnAdminService, mnPoolsService, mnAuthService, uiRouter, mnPools, $rootScope) {
    super();

    this.postClusterInitHttp = mnWizardService.stream.postClusterInitHttp;
    this.majorMinorVersion = mnAdminService.stream.majorMinorVersion;

    this.wizardForm = mnWizardService.wizardForm;
    this.newClusterConfigForm = mnWizardService.wizardForm.newClusterConfig;
    this.getServicesValues = mnWizardService.getServicesValues.bind(mnWizardService);

    this.totalRAMMegs = mnWizardService.stream.totalRAMMegs;
    this.maxRAMMegs = mnWizardService.stream.maxRAMMegs;
    this.memoryQuotasFirst = mnWizardService.stream.memoryQuotasFirst;

    this.servicesHttp = mnWizardService.stream.servicesHttp;
    this.groupHttp = mnWizardService.stream.groupHttp;

    this.isEnterprise = mnPoolsService.stream.isEnterprise;

    let postPoolsDefaultErrors =
        mnAdminService.stream.postPoolsDefaultValidation.error
        .pipe(map((error) => error && !!Object.keys(error.errors).length),
              startWith(false));

    let hostConfigField =
        this.wizardForm.newClusterConfig.get("clusterStorage.hostConfig");

    this.isButtonDisabled =
      combineLatest(
        postPoolsDefaultErrors,
        hostConfigField.statusChanges.pipe(map(v => v == "INVALID"))
      ).pipe(map(([err, invalid]) => err || invalid));


    mnWizardService.stream.getSelfConfig
      .pipe(first())
      .subscribe(v => mnWizardService.setSelfConfig(v));

    this.form = mnFormService.create(this);

    mnPoolsService.stream.isEnterprise
      .pipe(first())
      .subscribe(() => {
        this.form
          .setPackPipe(pipe(
            filter(() => hostConfigField.valid),
            withLatestFrom(mnPoolsService.stream.isEnterprise),
            map(this.getClusterInitConfig.bind(this))
          ))
          .setPostRequest(mnWizardService.stream.postClusterInitHttp);

        /** We need to check for err === 0 for certificate specific errors
         *  when using TLS. Certificates are regenerated during the middle
         *  of postClusterInitHttp, which causes postUILogin to error or
         *  more specifically, timeout.
        */
        this.form
          .setPackPipe(map(mnWizardService.getUserCreds.bind(mnWizardService)))
          .setPostRequest(mnAuthService.stream.postUILogin)
          .clearErrors()
          .showGlobalSpinner()
          .error(err => {
            if (err === 0) {
              window.location.reload();
            }
          })
          .success(() => {
            $rootScope.mnGlobalSpinnerFlag = true;
            mnPools.clearCache();
            uiRouter.urlRouter.sync();
          });
      });
  }

  getAddressFamily(addressFamilyUI) {
    switch(addressFamilyUI) {
      case "inet":
      case "inetOnly": return "ipv4";
      case "inet6":
      case "inet6Only": return "ipv6";
      default: return "ipv4";
    }
  }

  getAddressFamilyOnly(addressFamilyUI) {
    switch(addressFamilyUI) {
      case "inet":
      case "inet6": return false;
      case "inetOnly":
      case "inet6Only": return true;
      default: return false;
    }
  }

  getHostConfig() {
    let clusterStore = this.wizardForm.newClusterConfig.get("clusterStorage");

    return {
      afamily: this.getAddressFamily(clusterStore.get("hostConfig.addressFamilyUI").value),
      afamilyOnly: this.getAddressFamilyOnly(clusterStore.get("hostConfig.addressFamilyUI").value),
      nodeEncryption: clusterStore.get("hostConfig.nodeEncryption").value ? 'on' : 'off'
    };
  }

  getIndexesConfig() {
    var rv = {};
    if (this.wizardForm.newClusterConfig.get("services.flag").value.index) {
        rv.indexerStorageMode = this.wizardForm.newClusterConfig.get("storageMode").value;
    }
    return rv;
  }

  getClusterInitConfig([, isEnterprise]) {
    let rv = {};
    let nodeStorage = this.wizardForm.newClusterConfig.get("clusterStorage");
    rv.hostname = nodeStorage.get("hostname").value;
    rv.dataPath = nodeStorage.get("storage.path").value;
    rv.indexPath = nodeStorage.get("storage.index_path").value;
    rv.eventingPath = nodeStorage.get("storage.eventing_path").value;
    rv.sendStats = this.wizardForm.termsAndConditions.get("enableStats").value;
    let services = this.wizardForm.newClusterConfig.get("services.flag");
    rv.services = this.getServicesValues(services).join(",");
    let userData = clone(this.wizardForm.newCluster.value.user);
    delete userData.passwordVerify;
    userData.port = "SAME";

    let hostConfigRv = {};

    if (isEnterprise) {
      rv.analyticsPath = nodeStorage.get("storage.cbas_path").value;
      rv.javaHome = this.wizardForm.newClusterConfig.get("javaPath").value;
      hostConfigRv = this.getHostConfig.bind(this)();
    }

    let poolsDefaultRv = this.getPoolsDefaultValues.bind(this)(isEnterprise);
    let indexesRv = this.getIndexesConfig.bind(this)();

    return Object.assign(rv, poolsDefaultRv, hostConfigRv, indexesRv, userData);
  }

  getPoolsDefaultValues(isEnterprise) {
    var services = [
      ["memoryQuota", "kv"],
      ["indexMemoryQuota", "index"],
      ["ftsMemoryQuota", "fts"]
    ];
    if (isEnterprise) {
      services.push(["eventingMemoryQuota", "eventing"]);
      services.push(["cbasMemoryQuota", "cbas"]);
    }
    return services.reduce(this.getPoolsDefaultValue.bind(this), {
      clusterName: this.wizardForm.newCluster.get("clusterName").value
    });
  }

  getPoolsDefaultValue(result, names) {
    var service = this.wizardForm.newClusterConfig.get("services.flag." + names[1]);
    if (service && service.value) {
      result[names[0]] =
        this.wizardForm.newClusterConfig.get("services.field." + names[1]).value;
    }
    return result;
  }
}
