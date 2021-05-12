/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";
import mnPools from "/ui/app/components/mn_pools.js";
import _ from "/ui/web_modules/lodash.js";

export default 'mnAuthService';

angular
  .module('mnAuthService', [mnPools, uiRouter])
  .factory('mnAuthService', mnAuthServiceFactory);

function mnAuthServiceFactory(mnPools, $http, $uibModalStack, $window, $q) {
  var mnAuthService = {
    login: login,
    logout: _.once(logout),
    whoami: whoami,
    canUseCertForAuth: canUseCertForAuth
  };

  return mnAuthService;

  function whoami() {
    return $http({
      method: 'GET',
      cache: true,
      url: '/whoami'
    }).then(function (resp) {
      return resp.data;
    });
  }

  function canUseCertForAuth() {
    return $http({
      method: 'GET',
      url: '/_ui/canUseCertForAuth'
    }).then(function (r) {
      return r.data;
    });
  }

  function login(user, useCertForAuth) {
    var config = {
      method: 'POST',
      url: '/uilogin'
    }

    if (useCertForAuth) {
      config.params = {
        use_cert_for_auth: 1
      };
    } else {
      user = user || {};
      config.data = {
        user: user.username,
        password: user.password
      };
    }

    return $http(config).then(function (resp) {
      return mnPools.get().then(function (cachedPools) {
        mnPools.clearCache();
        return mnPools.get().then(function (newPools) {
          if (cachedPools.implementationVersion !== newPools.implementationVersion) {
            return $q.reject({status: 410});
          } else {
            return resp;
          }
        });
      }).then(function (resp) {
        localStorage.setItem("mnLogIn",
                             Number(localStorage.getItem("mnLogIn") || "0") + 1);
        return resp;
      })
    });
  }

  function logout() {
    $uibModalStack.dismissAll("uilogout");
    return $http({
      method: 'POST',
      url: "/uilogout"
    }).then(function () {
      $window.location.reload();
    }, function () {
      $window.location.reload();
    });
  }
}
