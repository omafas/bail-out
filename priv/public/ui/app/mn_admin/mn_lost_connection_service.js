/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";
import uiBootstrap from "/ui/web_modules/angular-ui-bootstrap.js";

export default "mnLostConnectionService";

angular
  .module("mnLostConnectionService", [
    uiRouter,
    uiBootstrap
  ])
  .factory("mnLostConnectionService", mnLostConnectionFactory);

function mnLostConnectionFactory($interval, $uibModalStack, $window, $state) {
  var state = {
    isActive: false,
    isReload: false
  };
  var mnLostConnectionService = {
    activate: activate,
    deactivate: deactivate,
    getState: getState,
    resendQueries: resendQueries
  };
  return mnLostConnectionService;

  function activate() {
    if (state.isActive) {
      return;
    }
    state.isActive = true;
    resetTimer();
    runTimer();
  }

  function runTimer() {
    state.interval = $interval(function () {
      state.repeatAt -= 1;
      if (state.repeatAt <= 0) {
        $uibModalStack.dismissAll();
        resendQueries();
      }
    }, 1000);
  }

  function resetTimer() {
    $interval.cancel(state.interval);
    state.interval = null;
    state.repeatAt = 60;
  }

  function resendQueries() {
    $state.reload().then(deactivate, function () {
      resetTimer();
      runTimer();
    });
  }

  function deactivate() {
    if (state.isReload) {
      return;
    }
    state.isReload = true;
    $interval.cancel(state.interval);
    $window.location.reload(true);// completely reinitialize application after lost of connection
  }

  function getState() {
    return state;
  }
}
