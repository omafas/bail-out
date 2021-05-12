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
mn.services.MnPools = (function (Rx) {
  "use strict";

  var launchID =  (new Date()).valueOf() + '-' + ((Math.random() * 65536) >> 0);

  MnPoolsService.annotations = [
    new ng.core.Injectable()
  ];

  MnPoolsService.parameters = [
    ng.common.http.HttpClient,
    mn.pipes.MnParseVersion
  ];

  MnPoolsService.prototype.get = get;

  return MnPoolsService;

  function MnPoolsService(http, mnParseVersionPipe) {
    this.http = http;
    this.stream = {};

    this.stream.getSuccess =
      (new Rx.BehaviorSubject())
      .pipe(
        Rx.operators.switchMap(this.get.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.isEnterprise =
      this.stream.getSuccess.pipe(Rx.operators.pluck("isEnterprise"),
                                  Rx.operators.distinctUntilChanged());

    this.stream.implementationVersion =
      this.stream.getSuccess.pipe(Rx.operators.pluck("implementationVersion"));

    this.stream.majorMinorVersion =
      this.stream.implementationVersion.pipe(
        Rx.operators.map(mnParseVersionPipe.transform.bind(mnParseVersionPipe)),
        Rx.operators.map(function (rv) {
          return rv[0].split('.').splice(0,2).join('.');
        })
      );

    this.stream.mnServices =
      this.stream.isEnterprise
      .pipe(Rx.operators.map(function (isEnterprise) {
        return isEnterprise ?
          ["kv", "index", "fts", "n1ql", "eventing", "cbas"] :
          ["kv", "index", "fts", "n1ql"];
      }), mn.core.rxOperatorsShareReplay(1));

    this.stream.quotaServices =
      this.stream.isEnterprise
      .pipe(Rx.operators.map(function (isEnterprise) {
        return isEnterprise ?
          ["kv", "index", "fts", "eventing", "cbas"] :
          ["kv", "index", "fts"];
      }), mn.core.rxOperatorsShareReplay(1));
  }

  function get(mnHttpParams) {
    return this.http.get('/pools').pipe(
      Rx.operators.map(function (pools) {
        pools.isInitialized = !!pools.pools.length;
        pools.launchID = pools.uuid + '-' + launchID;
        return pools;
      })
    );
  }
})(window.rxjs);
