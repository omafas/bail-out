/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import _ from 'lodash';
import uiBootstrap from 'angular-ui-bootstrap';

import mnFilters from './mn_filters.js';

export default 'mnAlertsService';


angular
  .module('mnAlertsService', [uiBootstrap, mnFilters])
  .service('mnAlertsService', ["$uibModal", "$rootScope", "$timeout", mnAlertsServiceFactory]);

function mnAlertsServiceFactory($uibModal, $rootScope, $timeout) {
  var alerts = [];
  var alertsHistory = [];
  var clientAlerts = {
    hideCompatibility: false
  };
  var mnAlertsService = {
    setAlert: setAlert,
    formatAndSetAlerts: formatAndSetAlerts,
    showAlertInPopup: showAlertInPopup,
    alerts: alerts,
    removeItem: removeItem,
    isNewAlert: isNewAlert,
    clientAlerts: clientAlerts
  };

  return mnAlertsService;

  function showAlertInPopup(message, title) {
    var scope = $rootScope.$new();
    scope.alertsCtl = {
      message: message
    };
    scope.title = title;
    return $uibModal.open({
      scope: scope,
      templateUrl: "app/components/mn_alerts_popup_message.html"
    }).result;
  }

  function isNewAlert(item) {
    var findedItem = _.find(alertsHistory, item);
    return _.indexOf(alertsHistory, findedItem) === -1;
  }

  function startTimer(item, timeout) {
    return $timeout(function () {
      removeItem(item);
    }, parseInt(timeout, 10));
  }

  function removeItem(item) {
    var index = _.indexOf(alerts, item);
    item.timeout && $timeout.cancel(item.timeout);
    alerts.splice(index, 1);
  }

  function setAlert(type, message, timeout, id) {
    var item = {
      type: type || 'error',
      msg: message,
      id: id
    };

    //in case we get alert with the same message
    //but different id find and remove it
    var findedItem = _.find(alerts, {
      type: type,
      msg: message
    });

    if (findedItem) {
      removeItem(findedItem);
    }

    alerts.push(item);
    alertsHistory.push(item);

    if (timeout) {
      item.timeout = startTimer(item, timeout);
    }
  }
  function formatAndSetAlerts(incomingAlerts, type, timeout) {
    timeout = timeout || (60000 * 5);
    if ((angular.isArray(incomingAlerts) && angular.isString(incomingAlerts[0])) ||
        angular.isObject(incomingAlerts)) {
      angular.forEach(incomingAlerts, function (msg) {
        setAlert(type, msg, timeout);
      });
    }

    if (angular.isString(incomingAlerts)) {
      setAlert(type, incomingAlerts, timeout);
    }
  }
}
