/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import mnLostConnectionService from "./mn_lost_connection_service.js";

export default 'mnLostConnection';

angular
  .module('mnLostConnection', [mnLostConnectionService])
  .config(["$httpProvider", mnLostConnectionConfig])
  .controller("mnLostConnectionController", ["mnLostConnectionService", "$window", mnLostConnectionController]);

function mnLostConnectionController(mnLostConnectionService, $window) {
  var vm = this;
  vm.lostConnectionAt = $window.location.host;
  vm.state = mnLostConnectionService.getState();
  vm.retryNow = mnLostConnectionService.resendQueries;
}

function mnLostConnectionConfig($httpProvider) {
  $httpProvider.interceptors.push(['$q', '$injector', interceptorOfErrConnectionRefused]);
}

function interceptorOfErrConnectionRefused($q, $injector) {
  var wantedUrls = {};

  return {
    responseError: function (rejection) {
      if (rejection.status <= 0 && (rejection.xhrStatus == "error")) {
        //rejection caused not by us (e.g. net::ERR_CONNECTION_REFUSED)
        wantedUrls[rejection.config.url] = true;
        $injector
          .get("mnLostConnectionService")
          .activate();
      } else {
        if (rejection.config && wantedUrls[rejection.config.url]) { //in order to avoid cached queries
          wantedUrls = {};
          $injector
            .get("mnLostConnectionService")
            .deactivate();
        }
      }
      return $q.reject(rejection);
    },
    response: function (resp) {
      if (wantedUrls[resp.config.url]) {
        wantedUrls = {};
        $injector
          .get("mnLostConnectionService")
          .deactivate();
      }
      return resp;
    }
  };
}
