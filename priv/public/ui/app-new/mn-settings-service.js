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
mn.services.MnSettings = (function (Rx) {
  "use strict";

  MnSettingsService.annotations = [
    new ng.core.Injectable()
  ];

  MnSettingsService.parameters = [
    ng.common.http.HttpClient,
    mn.services.MnPools,
    mn.services.MnAdmin
  ];

  MnSettingsService.prototype.postTestEmail = postTestEmail;
  MnSettingsService.prototype.postAlerts = postAlerts;
  MnSettingsService.prototype.getAlerts = getAlerts;
  MnSettingsService.prototype.getAutoCompaction = getAutoCompaction;
  MnSettingsService.prototype.postAutoCompaction = postAutoCompaction;
  MnSettingsService.prototype.getStats = getStats;
  MnSettingsService.prototype.getPhoneHome = getPhoneHome;
  MnSettingsService.prototype.getAutoFailover = getAutoFailover;
  MnSettingsService.prototype.getAutoReprovision = getAutoReprovision;
  MnSettingsService.prototype.postAutoFailoverReset = postAutoFailoverReset;
  MnSettingsService.prototype.postAutoReprovisionReset = postAutoReprovisionReset;

  return MnSettingsService;

  function MnSettingsService(http, mnPoolsService, mnAdminService) {
    this.http = http;
    this.stream = {};

    this.stream.getAlerts =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getAlerts.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getAutoCompaction =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getAutoCompaction.bind(this)),
        Rx.operators.map(function (v) {
          var ac = v.autoCompactionSettings;
          ac.indexCircularCompaction.daysOfWeek =
            ac.indexCircularCompaction.daysOfWeek.split(",").reduce(function (acc, day) {
              acc[day] = true;
              return acc;
            }, {});
          ac.purgeInterval = v.purgeInterval;
          return ac;
        }),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getAutoCompactionFirst =
      this.stream.getAutoCompaction.pipe(Rx.operators.first());

    this.stream.getStats =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getStats.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getAutoFailover =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getAutoFailover.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getAutoReprovision =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getAutoReprovision.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getPhoneHome =
      mnPoolsService.stream.getSuccess
      .pipe(Rx.operators.withLatestFrom(mnAdminService.stream.implementationVersion),
            Rx.operators.switchMap(this.getPhoneHome.bind(this)),
            mn.core.rxOperatorsShareReplay(1)
           );

    this.stream.postAutoCompaction =
      new mn.core.MnPostHttp(this.postAutoCompaction(false).bind(this))
      .addSuccess()
      .addError();

    this.stream.postAutoCompactionValidation =
      new mn.core.MnPostHttp(this.postAutoCompaction(true).bind(this))
      .addSuccess()
      .addError();

    this.stream.postTestEmail =
      new mn.core.MnPostHttp(this.postTestEmail.bind(this))
      .addSuccess()
      .addError();

    this.stream.postAlerts =
      new mn.core.MnPostHttp(this.postAlerts(false).bind(this))
      .addSuccess()
      .addError();

    this.stream.postAlertsValidation =
      new mn.core.MnPostHttp(this.postAlerts(true).bind(this))
      .addSuccess()
      .addError();

    this.stream.postAutoFailoverReset =
      new mn.core.MnPostHttp(this.postAutoFailoverReset.bind(this))
      .addSuccess()
      .addError();

    this.stream.postAutoReprovisionReset =
      new mn.core.MnPostHttp(this.postAutoReprovisionReset.bind(this))
      .addSuccess()
      .addError();
  }

  function getPhoneHome(data) {
    return this.http.jsonp('https://ph.couchbase.net/v2' +
                           '?launchID=' + data[0].launchID +
                           '&version=' + data[1], "callback");
  }

  function getStats() {
    return this.http.get('/settings/stats');
  }
  function postAlerts(validate) {
    return function (data) {
      return this.http.post("/settings/alerts", data, {
        params: new ng.common.http.HttpParams().set("just_validate", validate ? 1 : 0)
      });
    }.bind(this);
  }

  function postAutoCompaction(validate) {
    return function (data) {
      return this.http.post("/controller/setAutoCompaction", data, {
        params: new ng.common.http.HttpParams().set("just_validate", validate ? 1 : 0)
      });
    }
  }

  function postTestEmail(data) {
    return this.http.post("/settings/alerts/testEmail", data);
  }

  function postAutoFailoverReset() {
    return this.http.post("/settings/autoFailover/resetCount");
  }

  function postAutoReprovisionReset() {
    return this.http.post("/settings/autoReprovision/resetCount");
  }

  function getAlerts() {
    return this.http.get("/settings/alerts");
  }

  function getAutoReprovision() {
    return this.http.get("/settings/autoReprovision");
  }

  function getAutoFailover() {
    return this.http.get("/settings/autoFailover");
  }

  function getAutoCompaction(data) {
    return this.http.get("/settings/autoCompaction");
  }

})(window.rxjs);
