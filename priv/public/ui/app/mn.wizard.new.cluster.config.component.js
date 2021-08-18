/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {UIRouter} from '/ui/web_modules/@uirouter/angular.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {map, withLatestFrom, first} from '/ui/web_modules/rxjs/operators.js';
import {pipe} from '/ui/web_modules/rxjs.js';
import {MnWizardService} from './mn.wizard.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnFormService} from "./mn.form.service.js";
import {MnAuthService} from "./mn.auth.service.js";
import {MnAdminService} from "./mn.admin.service.js";
import {MnHttpGroupRequest} from "./mn.http.request.js";
import {MnPools, $rootScope} from "./ajs.upgraded.providers.js";

export {MnWizardNewClusterConfigComponent};

class MnWizardNewClusterConfigComponent extends MnLifeCycleHooksToStream {

  static get annotations() { return [
    new Component({
      templateUrl: "/ui/app/mn.wizard.new.cluster.config.html",
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

    this.isButtonDisabled =
      mnAdminService.stream.postPoolsDefaultValidation.error
      .pipe(map((error) => error && !!Object.keys(error.errors).length));

    mnWizardService.stream.getSelfConfig
      .pipe(first())
      .subscribe(v => mnWizardService.setSelfConfig(v));

    this.form = mnFormService.create(this);

    mnPoolsService.stream.isEnterprise
      .pipe(first())
      .subscribe(isEnterprise => {
        this.form
          .setPackPipe(pipe(
            withLatestFrom(mnPoolsService.stream.isEnterprise),
            map(this.getNodeInitConfig.bind(this))
          ))
          .setPostRequest(mnWizardService.stream.postNodeInitHttp)
          .setPackPipe(pipe(
            withLatestFrom(mnPoolsService.stream.isEnterprise),
            map(this.getPoolsDefaultConfig.bind(this))
          ))
          .setPostRequest(mnAdminService.stream.postPoolsDefault)
          .setPackPipe(map(this.getStatsConfig.bind(this)))
          .setPostRequest(mnWizardService.stream.statsHttp)
          .setPackPipe(map(this.getServicesHttpConfig.bind(this)))
          .setPostRequest(mnWizardService.stream.servicesHttp);

        if (isEnterprise) {
          this.form
            .setPackPipe(map(this.getHostConfig.bind(this)))
            .setPostRequest(mnWizardService.stream.enableExternalListenerHttp)
            .setPackPipe(map(this.getHostConfig.bind(this)))
            .setPostRequest(mnWizardService.stream.setupNetConfigHttp)
            .setPostRequest(mnWizardService.stream.disableUnusedExternalListenersHttp)
        }

        this.form
          .setPackPipe(map(this.getIndexesConfig.bind(this)))
          .setPostRequest(new MnHttpGroupRequest({
            indexesHttp: mnWizardService.stream.indexesHttp
          }).addSuccess().addError())
          .setPackPipe(map(this.getAuthConfig.bind(this)))
          .setPostRequest(mnWizardService.stream.authHttp)
          .setPackPipe(map(mnWizardService.getUserCreds.bind(mnWizardService)))
          .setPostRequest(mnAuthService.stream.postUILogin)
          .clearErrors()
          .showGlobalSpinner()
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

  getAuthConfig() {
    return [this.wizardForm.newCluster.value.user, false];
  }

  getIndexesConfig() {
    var rv = new Map();
    if (this.wizardForm.newClusterConfig.get("services.flag").value.index) {
      rv.set("indexesHttp", {
        storageMode: this.wizardForm.newClusterConfig.get("storageMode").value
      });
    }
    return rv;
  }

  getNodeInitConfig([_, isEnterprise]) {
    let rv = {};
    let nodeStorage = this.wizardForm.newClusterConfig.get("clusterStorage");
    let addressFamilyUI = nodeStorage.get("hostConfig.addressFamilyUI").value;
    rv.hostname = nodeStorage.get("hostname").value;
    rv.dataPath = nodeStorage.get("storage.path").value;
    rv.indexPath = nodeStorage.get("storage.index_path").value;
    rv.eventingPath = nodeStorage.get("storage.eventing_path").value;
    rv.javaHome = nodeStorage.get("storage.java_home").value;

    if (isEnterprise) {
      rv.afamily = this.getAddressFamily(addressFamilyUI);
      rv.analyticsPath = nodeStorage.get("storage.cbas_path").value;
    }
    return rv;
  }

  getStatsConfig() {
    return this.wizardForm.termsAndConditions.get("enableStats").value;
  }

  getServicesHttpConfig() {
    let services = this.wizardForm.newClusterConfig.get("services.flag");
    return {services: this.getServicesValues(services).join(",")};
  }

  getPoolsDefaultConfig(isEnterprise) {
    return this.getPoolsDefaultValues.bind(this)(isEnterprise[1]);
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
