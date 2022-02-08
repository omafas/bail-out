/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/


import {FormGroup, FormControl, Validators, FormArray} from '@angular/forms';
import {Injectable} from '@angular/core';
import {HttpClient, HttpParams} from '@angular/common/http';
import _ from 'lodash';
import {BehaviorSubject, combineLatest} from 'rxjs';
import {switchMap, shareReplay, map, pluck} from 'rxjs/operators';

import {MnHelperService} from './mn.helper.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnHttpRequest} from './mn.http.request.js';

import {singletonGuard} from './mn.core.js';

export {MnWizardService};

function ipvOnlyValidator() {
  return (control) => {
    let value = control.value;
    delete control.warnings;
    if (value && value.includes("Only")) {
      let mnLocation = MnHelperService.mnLocation();
      let isValueV6 = value.includes("inet6");
      if (mnLocation.kind) {
        let isKindV6 = mnLocation.kind.includes("ipv6");
        return (isKindV6 && isValueV6) || (!isKindV6 && !isValueV6) ? null : getIpvOnlyError(isKindV6, isValueV6);
      } else {
        control.warnings = getIpvOnlyError(null, isValueV6);
        return null;
      }
    } else {
      return null;
    }
  };
}

function getIpvOnlyError(isKindV6, isValueV6) {
  return {
    ipvOnly: {
      kind: isKindV6 ? 6 : 4,
      value: isValueV6 ? 6 : 4
    }
  };
}



var clusterStorage = new FormGroup({
  hostname: new FormControl(null, [Validators.required]),
  hostConfig: new FormGroup({
    addressFamilyUI: new FormControl(null, [ipvOnlyValidator()]),
    nodeEncryption: new FormControl()
  }),
  storage: new FormGroup({
    path: new FormControl(null),
    index_path: new FormControl(null),
    eventing_path: new FormControl(null),
    java_home: new FormControl(null),
    cbas_path: new FormArray([new FormControl()])
  })
});

var wizardForm = {
  newCluster: new FormGroup({
    clusterName: new FormControl(null, [Validators.required]),
    user: new FormGroup({
      username: new FormControl("Administrator", [Validators.required]),
      password: new FormControl(null, [Validators.required, Validators.minLength(6)]),
      passwordVerify: new FormControl()
    })
  }),
  newClusterConfig: new FormGroup({
    clusterStorage: clusterStorage,
    services: new FormGroup({
      // flag
      // field
    }),
    javaPath: new FormControl(),
    storageMode: new FormControl(null)
  }),
  termsAndConditions: new FormGroup({
    agree: new FormControl(false, [Validators.required]),
    enableStats: new FormControl(true)
  }),
  joinCluster: new FormGroup({
    clusterAdmin: new FormGroup({
      hostname: new FormControl(null, [Validators.required]),
      user: new FormControl("Administrator", [Validators.required]),
      password: new FormControl('', [Validators.required])
    }),
    services: new FormGroup({
      // flag
    }),
    clusterStorage: clusterStorage
  })
};

class MnWizardService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnAdminService,
    MnHelperService,
    MnPoolsService
  ]}

  constructor(http, mnAdminService, mnHelperService, mnPoolsService) {
    singletonGuard(MnWizardService);
    this.http = http;
    this.wizardForm = wizardForm;
    this.IEC = mnHelperService.IEC;

    this.stream = {};
    this.initialValues = {
      hostname: null,
      storageMode: null,
      clusterStorage: null,
      implementationVersion: null
    };

    this.stream.joinClusterHttp =
      new MnHttpRequest(this.postJoinCluster.bind(this))
      .addSuccess()
      .addLoading()
      .addError();

    this.stream.postNodeInitHttp =
      new MnHttpRequest(this.postNodeInit.bind(this))
      .addSuccess()
      .addError();

    this.stream.postClusterInitHttp =
      new MnHttpRequest(this.postClusterInit.bind(this))
      .addSuccess()
      .addError();

    this.stream.diskStorageHttp =
      new MnHttpRequest(this.postDiskStorage.bind(this))
      .addSuccess()
      .addError();

    this.stream.enableExternalListenerHttp =
      new MnHttpRequest(this.postEnableExternalListener.bind(this))
      .addSuccess()
      .addError();

    this.stream.setupNetConfigHttp =
      new MnHttpRequest(this.postSetupNetConfig.bind(this))
      .addSuccess()
      .addError();

    this.stream.disableUnusedExternalListenersHttp =
      new MnHttpRequest(this.postDisableUnusedExternalListeners.bind(this))
      .addSuccess()
      .addError();

    this.stream.hostnameHttp =
      new MnHttpRequest(this.postHostname.bind(this))
      .addSuccess()
      .addError();

    this.stream.postSettingsWebHttp =
      new MnHttpRequest(this.postSettingsWeb.bind(this))
      .addSuccess()
      .addError();

    this.stream.querySettingsHttp =
      new MnHttpRequest(this.postQuerySettings.bind(this))
      .addSuccess()
      .addError();

    this.stream.indexesHttp =
      new MnHttpRequest(this.postIndexes.bind(this))
      .addSuccess()
      .addError();

    this.stream.servicesHttp =
      new MnHttpRequest(this.postServices.bind(this))
      .addSuccess()
      .addError();

    this.stream.statsHttp =
      new MnHttpRequest(this.postStats.bind(this))
      .addSuccess()
      .addError();

    this.stream.getSelfConfig =
      (new BehaviorSubject()).pipe(switchMap(this.getSelfConfig.bind(this)),
                                   shareReplay({refCount: true, bufferSize: 1}));


    this.stream.memoryQuotasFirst =
      combineLatest(
        this.stream.getSelfConfig,
        mnPoolsService.stream.quotaServices
      )
      .pipe(map(mnPoolsService.pluckMemoryQuotas.bind(mnPoolsService)));

    this.stream.getIndexes =
      (new BehaviorSubject()).pipe(switchMap(this.getIndexes.bind(this)),
                                   shareReplay({refCount: true, bufferSize: 1}));

    this.stream.preprocessPath =
      this.stream.getSelfConfig.pipe(map(this.chooseOSPathPreprocessor.bind(this)));

    this.stream.availableHddStorage =
      this.stream.getSelfConfig.pipe(
        pluck("availableStorage", "hdd"),
        map((hdd) => hdd.sort((a, b) => b.path.length - a.path.length)));

    this.stream.initHddStorage =
      this.stream.getSelfConfig.pipe(
        pluck("storage", "hdd", 0),
        map(function (rv) {
          rv.cbas_path = rv.cbas_dirs;
          delete rv.cbas_dirs;
          return rv;
        })
      );

    this.stream.totalRAMMegs =
      this.stream.getSelfConfig.pipe(
        map((nodeConfig) => Math.floor(nodeConfig.storageTotals.ram.total / this.IEC.Mi)));

    this.stream.maxRAMMegs =
      this.stream.totalRAMMegs.pipe(map(mnHelperService.calculateMaxMemorySize));
  }

  getServicesValues(servicesGroup) {
    return Object
      .keys(servicesGroup.controls)
      .filter((serviceName) => Boolean(servicesGroup.get(serviceName).value));
    //   reduce(function (result, serviceName) {
    //   var service = servicesGroup.get(serviceName);
    //   if (service && service.value) {
    //     result.push(serviceName);
    //   }
    //   return result;
    // }, []);
  }

  getAddressFamilyUI(config) {
    return config.addressFamily + (config.addressFamilyOnly ? "Only" : "");
  }

  setSelfConfig(selfConfig) {
    var hostname = selfConfig.configuredHostname;

    if (!hostname) {
      let maybeCbLocal = selfConfig['otpNode'].split('@')[1];
      if (!maybeCbLocal || (maybeCbLocal == "cb.local")) {
        hostname = selfConfig.addressFamily == "inet6" ? "::1" : "127.0.0.1";
      } else {
        hostname = maybeCbLocal;
      }
    } else {
      //remove port from host
      let parsed = new URL("http://" + selfConfig.configuredHostname);
      //remove ipv6 brackets
      hostname = parsed.hostname.replace(/[[\]']+/g, '');
    }

    wizardForm.newClusterConfig.get("clusterStorage.hostname").setValue(hostname);
    wizardForm.joinCluster.get("clusterStorage.hostname").setValue(hostname);
    wizardForm.newClusterConfig.get("clusterStorage.hostConfig").patchValue({
      addressFamilyUI: this.getAddressFamilyUI(selfConfig),
      nodeEncryption: selfConfig.nodeEncryption
    });
    this.initialValues.hostname = hostname;
  }

  getUserCreds() {
    var data = _.clone(this.wizardForm.newCluster.value.user);
    data.user = data.username
    delete data.passwordVerify;
    delete data.username;
    return data;
  }

  createLookUpStream(subject) {
    return combineLatest(
      this.stream.availableHddStorage,
      this.stream.preprocessPath,
      subject
    ).pipe(
      map(this.lookupPathResource.bind(this)),
      map(this.updateTotal.bind(this)));
  }

  updateTotal(pathResource) {
    return Math.floor(
      pathResource.sizeKBytes * (100 - pathResource.usagePercent) / 100 / this.IEC.Mi
    ) + ' GiB';
  }

  lookupPathResource(rv) {
    var notFound = {path: "/", sizeKBytes: 0, usagePercent: 0};
    if (!rv[2]) {
      return notFound;
    } else {
      return _.detect(rv[0], function (info) {
        var preproc = rv[1](info.path);
        return rv[1](rv[2]).substring(0, preproc.length) == preproc;
      }) || notFound;
    }
  }

  chooseOSPathPreprocessor(config) {
    return (
      (config.os === 'windows') ||
        (config.os === 'win64') ||
        (config.os === 'win32')
    ) ? this.preprocessPathForWindows.bind(this) : this.preprocessPathStandard.bind(this);
  }

  preprocessPathStandard(p) {
    if (p.charAt(p.length-1) != '/') {
      p += '/';
    }
    return p;
  }

  preprocessPathForWindows(p) {
    p = p.replace(/\\/g, '/');
    if ((/^[A-Z]:\//).exec(p)) { // if we're using uppercase drive letter downcase it
      p = String.fromCharCode(p.charCodeAt(0) + 0x20) + p.slice(1);
    }
    return this.preprocessPathStandard(p);
  }

  getQuerySettings() {
    return this.http.get("/settings/querySettings");
  }

  getCELicense() {
    return this.http.get("CE_license_agreement.txt", {responseType: 'text'});
  }

  getEELicense() {
    return this.http.get("EE_subscription_license_agreement.txt", {responseType: 'text'});
  }

  getSelfConfig() {
    return this.http.get('/nodes/self');
  }

  postStats(sendStats) {
    return this.http.post('/settings/stats', {sendStats: sendStats});
  }

  postServices(data) {
    return this.http.post('/node/controller/setupServices', data);
  }



  postNodeInit(data) {
    return this.http.post('/nodeInit', data);
  }

  postClusterInit(data) {
    return this.http.post('/clusterInit', data);
  }

  postDiskStorage(config) {
    return this.http.post('/nodes/self/controller/settings', config);
  }

  postEnableExternalListener(data) {
    return this.http.post('/node/controller/enableExternalListener',  data);
  }

  postSetupNetConfig(data) {
    return this.http.post('/node/controller/setupNetConfig', data);
  }

  postDisableUnusedExternalListeners() {
    return this.http.post("/node/controller/disableUnusedExternalListeners");
  }

  postHostname(hostname) {
    return this.http.post('/node/controller/rename', {hostname: hostname});
  }



  postQuerySettings(data) {
    return this.http.post("/settings/querySettings", data);
  }

  postIndexes(data) {
    return this.http.post('/settings/indexes', data);
  }

  getIndexes() {
    return this.http.get('/settings/indexes');
  }

  postSettingsWeb(user) {
    var data = _.clone(user[0]);
    delete data.passwordVerify;
    data.port = "SAME";
    return this.http.post('/settings/web', data, {
      params: new HttpParams().set("just_validate", user[1] ? 1 : 0)
    });
  }

  postJoinCluster(clusterMember) {
    return this.http.post('/node/controller/doJoinCluster', clusterMember)
  }

}
