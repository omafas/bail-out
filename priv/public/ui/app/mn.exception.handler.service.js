/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Injectable } from '../web_modules/@angular/core.js';
import { HttpClient, HttpErrorResponse } from '../web_modules/@angular/common/http.js';
import { UIRouter } from '../web_modules/@uirouter/angular.js';
import { MnAppService } from './mn.app.service.js';
import { MnPoolsService } from './mn.pools.service.js';
import { combineLatest, Subject } from '../web_modules/rxjs.js'
import { take, filter, map, withLatestFrom, tap } from '../web_modules/rxjs/operators.js';
import { Rejection } from '/ui/web_modules/@uirouter/core.js';

export { MnExceptionHandlerService };

class MnExceptionHandlerService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnPoolsService,
    // UIRouter
  ]}

  constructor(http, mnPoolsService, uiRouter) {
    this.stream = {};
    this.http = http;
    this.errorReportsLimit = 8;

    this.stream.appError = new Subject();

    this.stream.appException = this.stream.appError.pipe(
      filter(this.filterException.bind(this)),
      take(this.errorReportsLimit),
      map(this.formatErrorMessage.bind(this))
    );

    // uiRouter.stateService.defaultErrorHandler(this.handleError.bind(this));
  }

  handleError(exception, cause) {
    console.error(exception);
    this.stream.appError.next(exception);
  }

  activate() {
    this.stream.appException.subscribe(this.send.bind(this));
  }

  deactivate() {
    this.stream.appException.unsubscribe();
  }

  send(error) {
    return this.http.post("/logClientError", error);
  }

  // TransitionRejection types
  // 2 "SUPERSEDED";
  // 3 "ABORTED";
  // 4 "INVALID";
  // 5 "IGNORED";
  // 6 "ERROR";
  filterException(exception) {
    return !(exception instanceof HttpErrorResponse) &&
      //we are not interested in these Rejection exceptions;
    !(exception instanceof Rejection &&
      (exception.type === 2 || exception.type === 3 || exception.type === 5));
  }

  formatErrorMessage(exception, index) {
    let error = ["Got unhandled javascript error:\n"];
    let props = ["name", "message", "fileName", "lineNumber", "columnNumber", "stack", "detail"];
    props.forEach(function (property) {
      if (exception[property]) {
        error.push(property + ": " + exception[property] + ";\n");
      }
    });
    if ((index + 1) === this.errorReportsLimit) {
      error.push("Further reports will be suppressed\n");
    }
    return error.join("");
  }
}
