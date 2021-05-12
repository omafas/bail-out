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
mn.services.MnGroups = (function (Rx) {
  "use strict";

  MnGroupsService.annotations = [
    new ng.core.Injectable()
  ];

  MnGroupsService.parameters = [
    ng.common.http.HttpClient
  ];

  MnGroupsService.prototype.getServerGroups = getServerGroups;

  return MnGroupsService;

  function MnGroupsService(http) {
    this.http = http;

    this.stream = {};

    this.stream.getServerGroups =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getServerGroups.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

  }

  function getServerGroups() {
    return this.http.get("/pools/default/serverGroups");
  }

})(window.rxjs);
