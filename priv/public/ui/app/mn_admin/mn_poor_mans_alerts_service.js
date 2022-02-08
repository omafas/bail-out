/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import _ from "lodash";
import uiRouter from "@uirouter/angularjs";
import uiBootstrap from "angular-ui-bootstrap";

import mnHelper from "../components/mn_helper.js";

export default "mnPoorMansAlertsService";

angular
  .module("mnPoorMansAlertsService", [
    uiRouter,
    uiBootstrap,
    mnHelper
  ])
  .factory("mnPoorMansAlertsService", ["$http", "$state", "$uibModal", "mnHelper", "$timeout", mnPoorMansAlertsFactory]);

function mnPoorMansAlertsFactory($http, $state, $uibModal, mnHelper, $timeout) {
  var alerts = [];
  var modal;
  var modalDeferId;

  var mnPoorMansAlerts = {
    maybeShowAlerts: maybeShowAlerts,
    postAlertsSilenceURL: postAlertsSilenceURL
  };

  return mnPoorMansAlerts;

  function postAlertsSilenceURL(alertsSilenceURL) {
    return $http({
      method: "POST",
      url: alertsSilenceURL,
      timeout: 5000,
      data: ''
    });
  }

  function maybeShowAlerts(poolDefault) {
    if ($state.params.disablePoorMansAlerts) {
      return;
    }
    alerts = poolDefault.alerts.filter(a => !a.disableUIPopUp);
    if (!alerts.length) {
      return;
    }
    if (modalDeferId) {
      $timeout.cancel(modalDeferId);
    }
    if (modal) {
      modal.dismiss();
      modal = null;
      modalDeferId = $timeout(function () { //we need this in order to allow uibModal close backdrop
        modal = doShowAlerts(poolDefault.alertsSilenceURL, alerts);
      }, 0);
    } else {
      modal = doShowAlerts(poolDefault.alertsSilenceURL, alerts);
    }
  }

  function doShowAlerts(alertsSilenceURL, alerts) {
    return $uibModal.open({
      templateUrl: "app/mn_admin/mn_poor_mans_alerts.html",
      controller: "mnPoorMansAlertsController as poorMansCtl",
      resolve: {
        alertsSilenceURL: mnHelper.wrapInFunction(alertsSilenceURL),
        alerts: mnHelper.wrapInFunction(_.clone(alerts, true))
      }
    });
  }
}
