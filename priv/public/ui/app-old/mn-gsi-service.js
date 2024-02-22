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
mn.services.MnGSI = (function (Rx) {
  "use strict";

  MnGSI.annotations = [
    new ng.core.Injectable()
  ];

  MnGSI.parameters = [
    ng.common.http.HttpClient
  ];

  MnGSI.prototype.getIndexStatus = getIndexStatus;

  return MnGSI;

  function MnGSI(http) {
    this.http = http;
    this.stream = {};

    this.stream.getIndexStatus =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getIndexStatus.bind(this)),
        mn.core.rxOperatorsShareReplay(1));
  }

  function getIndexStatus() {
    return this.http.get("/indexStatus");
  }

})(window.rxjs);
