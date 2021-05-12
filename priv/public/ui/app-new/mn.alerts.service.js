/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Injectable } from '../web_modules/@angular/core.js';
import { Subject } from '../web_modules/rxjs.js';
import { tap, map } from '../web_modules/rxjs/operators.js';

export { MnAlertsService };

class MnAlertsService {
  static annotations = [
    new Injectable()
  ]

  static parameters = []

  constructor() {
    this._alerts = [];
    this.stream = {};
    this.stream.alert = new Subject();

    this.stream.alerts =
      this.stream.alert.pipe(tap(function () {
        window.scrollTo(0, 0);
      }), map(this._setAlert.bind(this)));
  }

  success(message) {
    return () => {
      this.stream.alert.next({
        message: message,
        type: "success",
        timeout: 4000
      })
    }
  }

  error(staticMessage) {
    return (serverError) => {
      this.stream.alert.next({
        message: staticMessage || serverError,
        type: "error",
        timeout: 4000
      })
    }
  }

  warning(staticMessage) {
    return (serverError) => {
      this.stream.alert.next({
        message: staticMessage || serverError,
        type: "warning",
        timeout: 4000
      })
    }
  }

  _startTimer(item, timeout) {
    return setTimeout(() => {
      this._removeItem(item);
    }, parseInt(timeout, 10));
  }

  _removeItem(item) {
    var index = this._alerts.indexOf(item);
    item.timeout && clearTimeout(item.timeout);
    this._alerts.splice(index, 1);
  }

  // type, message, timeout, id
  _setAlert(alert) {
    //in case we get alert with the same message
    //but different id find and remove it
    var findedItem = this._alerts.find((allAlerts) => {
      return alert.type == allAlerts.type && alert.message == allAlerts.message;
    });

    findedItem && this._removeItem(findedItem);
    alert.timeout && (alert.timeout = this._startTimer(alert, alert.timeout));

    this._alerts.push(alert);

    return this._alerts;
  }
}
