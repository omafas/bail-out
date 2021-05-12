/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from "../web_modules/@angular/core.js";
import {BehaviorSubject, Subject} from "../web_modules/rxjs.js";
import {filter} from "../web_modules/rxjs/operators.js";
import {HttpErrorResponse, HttpClient} from '../web_modules/@angular/common/http.js';

export {MnAppService};

class MnAppService {
  static get annotations() { return [
    new Injectable()
  ]}

  constructor() {
    this.stream = {};
    this.stream.loading = new BehaviorSubject(false);
    this.stream.httpResponse = new Subject();
    this.stream.pageNotFound = new Subject();
    this.stream.http401 =
      this.stream.httpResponse.pipe(filter(function (rv) {
        //rejection.config.url !== "/controller/changePassword"
        //$injector.get('mnLostConnectionService').getState().isActivated
        return (rv instanceof HttpErrorResponse) &&
          (rv.status === 401) && !rv.headers.get("ignore-401");
      }));
  }
}
