/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default "mnInternalSettingsService";

angular
  .module("mnInternalSettingsService", [])
  .factory("mnInternalSettingsService", mnInternalSettingsFactory);

function mnInternalSettingsFactory($http) {
  var mnInternalSettingsService = {
    getState: getState,
    save: save
  };

  return mnInternalSettingsService;

  function save(data) {
    return $http({
      method: "POST",
      url: "/internalSettings",
      data: data
    });
  }

  function getState() {
    return $http({
      method: "GET",
      url: "/internalSettings"
    }).then(function (resp) {
      return resp.data;
    })
  }
}
