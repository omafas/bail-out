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
mn.services.MnAuth = (function () {
  "use strict";

  //TODO chech that the streams do not contain privat info after logout
  MnAuthService.annotations = [
    new ng.core.Injectable()
  ];

  MnAuthService.parameters = [
    ng.common.http.HttpClient
  ];

  MnAuthService.prototype.postUILogin = postUILogin;
  MnAuthService.prototype.postUILogout = postUILogout;
  MnAuthService.prototype.whoami = whoami;

  return MnAuthService;

  function MnAuthService(http) {
    this.http = http;
    this.stream = {};

    this.stream.postUILogin =
      new mn.core.MnPostHttp(this.postUILogin.bind(this))
      .addSuccess()
      .addError();

    this.stream.postUILogout =
      new mn.core.MnPostHttp(this.postUILogout.bind(this));
  }

  function whoami() {
    return this.http.get('/whoami');
  }

  function postUILogin(user) {
    return this.http.post('/uilogin', user || {});
    // should be moved into app.admin alerts
    // we should say something like you are using cached vesrion, reload the tab
    // return that.mnPoolsService
    //   .get$
    //   .map(function (cachedPools, newPools) {

    // if (cachedPools.implementationVersion !== newPools.implementationVersion) {
    //   return {ok: false, status: 410};
    // } else {
    //   return resp;
    // }
    // });
  }

  function postUILogout() {
    return this.http.post("/uilogout");
    // .do(function () {
    // $uibModalStack.dismissAll("uilogout");
    // $state.go('app.auth');
    // $window.localStorage.removeItem('mn_xdcr_regex');
    // $window.localStorage.removeItem('mn_xdcr_testKeys');
    // });
  }
})();
