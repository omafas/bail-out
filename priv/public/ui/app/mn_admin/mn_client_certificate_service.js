/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import _ from "/ui/web_modules/lodash.js";
import mnPoolDefault from "/ui/app/components/mn_pool_default.js";

export default 'mnClientCertificateService';

angular
  .module("mnClientCertificateService", [mnPoolDefault])
  .factory("mnClientCertificateService", mnClientCertificateFactory);

function mnClientCertificateFactory($http, mnPoolDefault) {
  var mnClientCertificateService = {
    getClientCertificateSettings: getClientCertificateSettings,
    postClientCertificateSettings: postClientCertificateSettings
  };

  return mnClientCertificateService;

  function getClientCertificateSettings() {
    return $http({
      method: 'GET',
      url: '/settings/clientCertAuth',
    }).then(function (resp) {
      return resp.data;
    });
  }

  function postClientCertificateSettings(data){
    var settings = _.clone(data);
    if (settings.state == 'disable') {
      settings.prefixes = settings.prefixes.filter(function (pref) {
        return !_.isEqual(pref, {delimiter: '', prefix: '', path: ''});
      });
    }
    if (!mnPoolDefault.export.compat.atLeast51) {
      (['delimiter', 'prefix', 'path']).forEach(function (key) {
        if (settings.prefixes[0] && settings.prefixes[0][key]) {
          settings[key] = settings.prefixes[0][key];
        }
      });
      delete settings.prefixes;
    }
    return $http({
      method: 'POST',
      url: '/settings/clientCertAuth',
      mnHttp: {
        isNotForm: mnPoolDefault.export.compat.atLeast51
      },
      data: settings,
    }).then(function (resp) {
      return resp.data;
    });
  }
}
