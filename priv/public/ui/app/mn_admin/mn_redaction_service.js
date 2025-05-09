/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";

export default "mnLogRedactionService";

angular
  .module("mnLogRedactionService", [])
  .factory("mnLogRedactionService", ["$http", mnRedactionFactory]);

function mnRedactionFactory($http) {
  var mnLogRedactionService = {
    get: get,
    post: post
  };

  return mnLogRedactionService;

  function get() {
    return $http.get("/settings/logRedaction").then(function (resp) {
      return resp.data;
    });
  }

  function post(data) {
    return $http.post("/settings/logRedaction", data);
  }
}
