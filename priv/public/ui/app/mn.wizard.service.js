/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {FormGroup, FormControl, Validators, FormArray} from "/ui/web_modules/@angular/forms.js";
import {Injectable} from "/ui/web_modules/@angular/core.js";
import {HttpClient, HttpParams} from '/ui/web_modules/@angular/common/http.js';
import _ from '/ui/web_modules/lodash.js';
import {BehaviorSubject, Subject, combineLatest} from '/ui/web_modules/rxjs.js';
import {switchMap, shareReplay, withLatestFrom, first, map, pluck} from '/ui/web_modules/rxjs/operators.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnHttpRequest} from './mn.http.request.js';

export {MnWizardService};

var clusterStorage = new FormGroup({
  hostname: new FormControl(null, [Validators.required]),
  hostConfig: new FormGroup({
    afamily: new FormControl(),
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

    this.stream.authHttp =
      new MnHttpRequest(this.postAuth.bind(this))
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
      .pipe(map(mnHelperService.pluckMemoryQuotas.bind(mnHelperService)));

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
      hostname = parsed.hostname.replace(/[\[\]']+/g, '');
    }

    wizardForm.newClusterConfig.get("clusterStorage.hostname").setValue(hostname);
    wizardForm.joinCluster.get("clusterStorage.hostname").setValue(hostname);
    wizardForm.newClusterConfig.get("clusterStorage.hostConfig").patchValue({
      afamily: selfConfig.addressFamily == "inet6",
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

  postAuth(user) {
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
