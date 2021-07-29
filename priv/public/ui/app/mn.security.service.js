/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from "../web_modules/@angular/core.js";
import {HttpClient, HttpParams, HttpHeaders} from '../web_modules/@angular/common/http.js';
import {BehaviorSubject, combineLatest, of} from '../web_modules/rxjs.js';
import {switchMap, shareReplay, map, pluck} from '../web_modules/rxjs/operators.js';
import {MnHttpRequest} from './mn.http.request.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MN_HTTP_REQUEST_RESTRICTED} from './constants/constants.js';

export {MnSecurityService};

class MnSecurityService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnAdminService,
    MnPoolsService
  ]}

  constructor(http, mnAdminService, mnPoolsService) {
    this.http = http;

    let isEnterprise = mnPoolsService.stream.isEnterprise;
    let compatVersion55 = mnAdminService.stream.compatVersion55;

    this.stream = {};

    this.stream.getSaslauthdAuth =
      (new BehaviorSubject()).pipe(
        switchMap(this.getSaslauthdAuth.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getCertificate =
      (new BehaviorSubject()).pipe(
        switchMap(this.getCertificate.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));

    let getLogRedaction =
      (new BehaviorSubject()).pipe(
        switchMap(this.getLogRedaction.bind(this)));
    this.stream.shouldGetLogRedaction = combineLatest(isEnterprise, compatVersion55)
      .pipe(switchMap(([isEnterprise, compatVersion55]) =>
                        isEnterprise && compatVersion55 ? getLogRedaction : of(MN_HTTP_REQUEST_RESTRICTED)),
                        shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getClientCertAuth =
      (new BehaviorSubject()).pipe(
        switchMap(this.getClientCertAuth.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getAudit =
      (new BehaviorSubject()).pipe(
        switchMap(this.getAudit.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getAuditDescriptors =
      (new BehaviorSubject()).pipe(
        switchMap(this.getAuditDescriptors.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getAuditNonFilterableDescriptors =
      (new BehaviorSubject()).pipe(
        switchMap(this.getAuditNonFilterableDescriptors.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getSettingsSecurity = (new BehaviorSubject())
      .pipe(switchMap(this.getSettingsSecurity.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    let getClusterEncryption = this.stream.getSettingsSecurity
      .pipe(pluck('clusterEncryptionLevel'));
    this.stream.shouldGetClusterEncryption = isEnterprise
    .pipe(switchMap(isEnterprise => isEnterprise ? getClusterEncryption : of(MN_HTTP_REQUEST_RESTRICTED)),
      shareReplay({refCount: true, bufferSize: 1}));

    this.stream.postLogRedaction =
      new MnHttpRequest(this.postLogRedaction.bind(this))
      .addSuccess()
      .addError();

    this.stream.postSettingsSecurity =
      new MnHttpRequest(this.postSettingsSecurity.bind(this))
      .addSuccess()
      .addError();

    this.stream.postClientCertAuth =
      new MnHttpRequest(this.postClientCertAuth.bind(this))
      .addSuccess()
      .addError();

    this.stream.postAuditValidation =
      new MnHttpRequest(this.postAudit(true).bind(this))
      .addSuccess()
      .addError();

    this.stream.postAudit =
      new MnHttpRequest(this.postAudit(false).bind(this))
      .addSuccess()
      .addError();

    this.stream.otherSettings = combineLatest(
      mnAdminService.stream.uiSessionTimeout.pipe(map(this.unpackSession.bind(this))),
      this.stream.shouldGetLogRedaction.pipe(map(this.unpackLogRedaction.bind(this))),
      this.stream.shouldGetClusterEncryption.pipe(map(this.unpackClusterEncryption.bind(this)))
    ).pipe(map(this.getOtherSettings.bind(this)));
  }

  postAudit(validate) {
    return (data) => {
      return this.http.post("/settings/audit", data, {
        params: new HttpParams().set("just_validate", validate ? 1 : 0)
      });
    }
  }

  postSettingsSecurity(data) {
    return this.http.post("/settings/security", data);
  }

  getSettingsSecurity() {
    return this.http.get("/settings/security");
  }

  getAuditDescriptors() {
    return this.http.get("/settings/audit/descriptors");
  }

  getAuditNonFilterableDescriptors() {
    return this.http
      .get("/settings/audit/nonFilterableDescriptors")
      .pipe(map(data => data.map((desc) => {
        desc.nonFilterable = true;
        return desc;
      })));
  }

  getAudit() {
    return this.http.get("/settings/audit");
  }

  getSaslauthdAuth() {
    return this.http.get("/settings/saslauthdAuth");
  }

  getClientCertAuth() {
    return this.http.get("/settings/clientCertAuth");
  }

  postClientCertAuth(data) {
    return this.http.post("/settings/clientCertAuth", data[0], {
      headers: new HttpHeaders().set("isNotForm", data[1])
    });
  }

  getLogRedaction() {
    return this.http.get("/settings/logRedaction");
  }

  postLogRedaction(data) {
    return this.http.post("/settings/logRedaction", data);
  }

  getCertificate() {
    return this.http.get("/pools/default/certificate", {
      params: new HttpParams().set("extended", true)
    });
  }

  unpackSession(session) {
    return (session !== MN_HTTP_REQUEST_RESTRICTED) ? (Number(session) / 60) || 0 : null;
  }

  unpackLogRedaction(redaction) {
    return (redaction !== MN_HTTP_REQUEST_RESTRICTED) ?
      redaction['logRedactionLevel'] : null;
  }

  unpackClusterEncryption(encryption) {
    return (encryption !== MN_HTTP_REQUEST_RESTRICTED) ? (encryption || null) : null;
  }

  getOtherSettings([session, logRedaction, clusterEncryption]) {
    return {
      logRedactionLevel: {
        logRedactionLevel: logRedaction
      },
      settingsSecurity: {
        uiSessionTimeout: session,
        clusterEncryptionLevel: clusterEncryption
      }
    }
  }
}
