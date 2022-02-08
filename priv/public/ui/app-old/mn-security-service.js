/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnSecurity = (function (Rx) {
  "use strict";

  MnSecurityService.annotations = [
    new ng.core.Injectable()
  ];

  MnSecurityService.parameters = [
    ng.common.http.HttpClient
  ];

  MnSecurityService.prototype.getSaslauthdAuth = getSaslauthdAuth;
  MnSecurityService.prototype.getCertificate = getCertificate;
  MnSecurityService.prototype.getLogRedaction = getLogRedaction;
  MnSecurityService.prototype.postLogRedaction = postLogRedaction;
  MnSecurityService.prototype.getClientCertAuth = getClientCertAuth;
  MnSecurityService.prototype.postClientCertAuth = postClientCertAuth;
  MnSecurityService.prototype.getAuditDescriptors = getAuditDescriptors;
  MnSecurityService.prototype.getAudit = getAudit;
  MnSecurityService.prototype.postAudit = postAudit;
  MnSecurityService.prototype.postSession = postSession;

  return MnSecurityService;

  function MnSecurityService(http) {
    this.http = http;

    this.stream = {};

    this.stream.getSaslauthdAuth =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getSaslauthdAuth.bind(this)),
        mn.core.rxOperatorsShareReplay(1));

    this.stream.getCertificate =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getCertificate.bind(this)),
        mn.core.rxOperatorsShareReplay(1));

    this.stream.getLogRedaction =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getLogRedaction.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getClientCertAuth =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getClientCertAuth.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.postLogRedaction =
      new mn.core.MnPostHttp(this.postLogRedaction.bind(this))
      .addSuccess()
      .addError();

    this.stream.postSession =
      new mn.core.MnPostHttp(this.postSession.bind(this))
      .addSuccess()
      .addError();

    this.stream.postClientCertAuth =
      new mn.core.MnPostHttp(this.postClientCertAuth.bind(this))
      .addSuccess()
      .addError();

    this.stream.getAudit =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getAudit.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getAuditDescriptors =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getAuditDescriptors.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.postAuditValidation =
      new mn.core.MnPostHttp(this.postAudit(true).bind(this))
      .addSuccess()
      .addError();

    this.stream.postAudit =
      new mn.core.MnPostHttp(this.postAudit(false).bind(this))
      .addSuccess()
      .addError();
  }

  function postAudit(validate) {
    return function (data) {
      return this.http.post("/settings/audit", data, {
        params: new ng.common.http.HttpParams().set("just_validate", validate ? 1 : 0)
      });
    }.bind(this)
  }

  function postSession(data) {
    return this.http.post("/settings/security", data);
  }

  function getAuditDescriptors() {
    return this.http.get("/settings/audit/descriptors");
  }

  function getAudit() {
    return this.http.get("/settings/audit");
  }

  function getSaslauthdAuth() {
    return this.http.get("/settings/saslauthdAuth");
  }

  function getClientCertAuth() {
    return this.http.get("/settings/clientCertAuth");
  }

  function postClientCertAuth(data) {
    return this.http.post("/settings/clientCertAuth", data[0], {
      headers: new ng.common.http.HttpHeaders().set("isNotForm", data[1])
    });
  }

  function getLogRedaction() {
    return this.http.get("/settings/logRedaction");
  }

  function postLogRedaction(data) {
    return this.http.post("/settings/logRedaction", data);
  }

  function getCertificate() {
    return this.http.get("/pools/default/certificate", {
      params: new ng.common.http.HttpParams().set("extended", true)
    });
  }

})(window.rxjs);
