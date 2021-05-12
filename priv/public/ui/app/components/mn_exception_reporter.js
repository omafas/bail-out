/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import _ from "/ui/web_modules/lodash.js";
import { Rejection } from '/ui/web_modules/@uirouter/core.js';

export default 'mnExceptionReporter';

angular
  .module("mnExceptionReporter", [])
  .config(mnExceptionReporterConfig);

function mnExceptionReporterConfig($provide) {
  $provide.decorator('$exceptionHandler', mnExceptionReporter)
}

function mnExceptionReporter($delegate, $injector) {
  var errorReportsLimit = 8;
  var sentReports = 0;

  // TransitionRejection types
  // 2 "SUPERSEDED";
  // 3 "ABORTED";
  // 4 "INVALID";
  // 5 "IGNORED";
  // 6 "ERROR";
  return function (exception, cause) {
    if (
      exception instanceof Rejection &&
        (exception.type === 2 || exception.type === 3 || exception.type === 5)
    ) {
      return; //we are not interested in these Rejection exceptions;
    }
    exception.cause = cause;
    send(exception);
    $delegate(exception, cause);
  };

  function formatErrorMessage(exception) {
    var error = ["Got unhandled javascript error:\n"];
    angular.forEach(["name", "message", "fileName", "lineNumber", "columnNumber", "stack"], function (property) {
      if (exception[property]) {
        error.push(property + ": " + exception[property] + ";\n");
      }
    });
    return error;
  }

  function send(exception) {
    if (exception.hasOwnProperty("config") &&
        exception.hasOwnProperty("headers") &&
        exception.hasOwnProperty("status") &&
        exception.hasOwnProperty("statusText")) {
      return; //we are not interested in http exception;
    }
    var error;
    if (sentReports < errorReportsLimit) {
      sentReports++;
      error = formatErrorMessage(exception);
      if (sentReports == errorReportsLimit - 1) {
        error.push("Further reports will be suppressed\n");
      }
    }
    // mozilla can report errors in some cases when user leaves current page
    // so delay report sending
    if (error) {
      _.delay(function () {
        $injector.get("$http")({
          method: "POST",
          url: "/logClientError",
          data: error.join("")
        });
      }, 500);
    }
  }
}
