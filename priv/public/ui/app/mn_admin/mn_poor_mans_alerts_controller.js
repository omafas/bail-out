/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";

import mnSpinner from "../components/directives/mn_spinner.js";
import mnPromiseHelper from "../components/mn_promise_helper.js";
import mnPoorMansAlertsService from "./mn_poor_mans_alerts_service.js";

export default "mnPoorMansAlerts";

angular
  .module("mnPoorMansAlerts", [
    mnSpinner,
    mnPromiseHelper,
    mnPoorMansAlertsService
  ])
  .controller("mnPoorMansAlertsController", ["mnPromiseHelper", "mnPoorMansAlertsService", "alertsSilenceURL", "alerts", "$uibModalInstance", mnPoorMansAlertsController]);

function mnPoorMansAlertsController(mnPromiseHelper, mnPoorMansAlertsService, alertsSilenceURL, alerts, $uibModalInstance) {
  var vm = this;

  vm.alerts = alerts;
  vm.onClose = onClose;

  function onClose() {
    mnPromiseHelper(vm, mnPoorMansAlertsService.postAlertsSilenceURL(alertsSilenceURL), $uibModalInstance)
      .showGlobalSpinner()
      .closeOnSuccess();
  }
}
