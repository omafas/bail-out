/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";

export default 'mnSettingsAutoFailoverService';

angular
  .module('mnSettingsAutoFailoverService', [])
  .factory('mnSettingsAutoFailoverService', ["$http", mnSettingsAutoFailoverServiceFactory]);

function mnSettingsAutoFailoverServiceFactory($http) {
  var mnSettingsAutoFailoverService = {
    resetAutoFailOverCount: resetAutoFailOverCount,
    resetAutoReprovisionCount: resetAutoReprovisionCount,
    getAutoFailoverSettings: getAutoFailoverSettings,
    saveAutoFailoverSettings: saveAutoFailoverSettings,
    getAutoReprovisionSettings: getAutoReprovisionSettings,
    postAutoReprovisionSettings: postAutoReprovisionSettings
  };

  return mnSettingsAutoFailoverService;

  function resetAutoFailOverCount(mnHttpParams) {
    return $http({
      method: 'POST',
      url: '/settings/autoFailover/resetCount',
      mnHttp: mnHttpParams
    });
  }
  function getAutoFailoverSettings() {
    return $http({
      method: 'GET',
      url: "/settings/autoFailover"
    }).then(function (resp) {
      return resp.data;
    });
  }
  function saveAutoFailoverSettings(autoFailoverSettings, params) {
    return $http({
      method: 'POST',
      url: "/settings/autoFailover",
      data: autoFailoverSettings,
      params: params
    });
  }
  function getAutoReprovisionSettings() {
    return $http({
      method: 'GET',
      url: "/settings/autoReprovision"
    });
  }
  function postAutoReprovisionSettings(settings, params) {
    return $http({
      method: 'POST',
      url: "/settings/autoReprovision",
      data: settings,
      params: params
    });
  }
  function resetAutoReprovisionCount(mnHttpParams) {
    return $http({
      method: 'POST',
      url: "/settings/autoReprovision/resetCount",
      mnHttp: mnHttpParams
    });
  }
}
