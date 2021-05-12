/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnApp = (function (Rx) {
  "use strict";

  MnApp.annotations = [
    new ng.core.Injectable()
  ];

  return MnApp;

  function MnApp() {
    this.stream = {};
    this.stream.loading = new Rx.BehaviorSubject(false);
    this.stream.httpResponse = new Rx.Subject();
    this.stream.pageNotFound = new Rx.Subject();
    this.stream.appError = new Rx.Subject();
    this.stream.http401 =
      this.stream.httpResponse.pipe(
        Rx.operators.filter(function (rv) {
          //rejection.config.url !== "/controller/changePassword"
          //$injector.get('mnLostConnectionService').getState().isActivated
          return (rv instanceof ng.common.http.HttpErrorResponse) &&
            (rv.status === 401) && !rv.headers.get("ignore-401");
        })
      );
  }
})(window.rxjs);
