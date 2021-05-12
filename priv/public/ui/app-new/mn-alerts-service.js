/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnAlerts = (function (Rx) {
  "use strict";

  function scrollTop() {
    window.scrollTo(0, 0);
  }

  MnAlertsService.annotations = [
    new ng.core.Injectable()
  ];

  MnAlertsService.parameters = [];

  MnAlertsService.prototype.setAlert = setAlert;
  MnAlertsService.prototype.removeItem = removeItem;
  MnAlertsService.prototype.startTimer = startTimer
  MnAlertsService.prototype.success = success;
  MnAlertsService.prototype.error = error;
  MnAlertsService.prototype.warning = warning;

  return MnAlertsService;

  function MnAlertsService() {
    this.alerts = [];
    this.stream = {};
    this.stream.alert = new Rx.Subject();

    this.stream.alerts =
      this.stream.alert.pipe(Rx.operators.tap(scrollTop),
                             Rx.operators.map(this.setAlert.bind(this)));
  }

  function success(message) {
    return function () {
      this.stream.alert.next({
        message: message,
        type: "success",
        timeout: 4000
      })
    }.bind(this)
  }

  function error(staticMessage) {
    return function (serverError) {
      this.stream.alert.next({
        message: staticMessage || serverError,
        type: "error",
        timeout: 4000
      })
    }.bind(this)
  }

  function warning(staticMessage) {
    return function (serverError) {
      this.stream.alert.next({
        message: staticMessage || serverError,
        type: "warning",
        timeout: 4000
      })
    }.bind(this)
  }

  function startTimer(item, timeout) {
    return setTimeout(function () {
      this.removeItem(item);
    }.bind(this), parseInt(timeout, 10));
  }

  function removeItem(item) {
    var index = R.indexOf(item, this.alerts);
    item.timeout && clearTimeout(item.timeout);
    this.alerts.splice(index, 1);
  }

  // type, message, timeout, id
  function setAlert(alert) {
    //in case we get alert with the same message
    //but different id find and remove it
    var sameType = R.propEq('type', alert.type);
    var sameMessage = R.propEq('message', alert.message);
    var byTypeAndMessage = R.allPass([sameType, sameMessage]);
    var findedItem = R.find(byTypeAndMessage)(this.alerts);

    findedItem && this.removeItem(findedItem);
    alert.timeout && (alert.timeout = this.startTimer(alert, alert.timeout));

    this.alerts.push(alert);
    // alertsHistory.push(item);
    return this.alerts;
  }

})(window.rxjs);
