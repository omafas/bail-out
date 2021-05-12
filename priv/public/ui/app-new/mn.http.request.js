/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Subject, of, merge, NEVER } from '../web_modules/rxjs.js';
import { HttpErrorResponse } from '../web_modules/@angular/common/http.js';
import { catchError, switchMap, shareReplay, mapTo, filter, map } from '../web_modules/rxjs/operators.js';
import { MnHelperService } from './mn.helper.service.js';

export { MnHttpRequest };

class MnHttpRequest {
  constructor(call) {
    this._dataSubject = new Subject();
    this._errorSubject = new Subject();
    this._loadingSubject = new Subject();
    this.addResponse(call);
  }

  clearError() {
    this._errorSubject.next(null);
  }

  addResponse(call) {
    let errorsAndSuccess = switchMap((data) => call(data).pipe(catchError((err) => of(err))));
    this.response = this._dataSubject.pipe(errorsAndSuccess, shareReplay(1));
    return this;
  }

  addError(modify) {
    let extractErrorsPipe =
        switchMap((rv) => {
          if (rv instanceof HttpErrorResponse) {
            return of(rv);
          } else if (MnHelperService.prototype.isJson(rv) && rv.includes("errors")) {
            return of(new HttpErrorResponse({error: rv}));
          } else {
            return NEVER;
          }
        });

    var error = merge(
      this._errorSubject,
      this.response.pipe(
        extractErrorsPipe,
        map(function (rv) {
          if (!!rv.error && MnHelperService.prototype.isJson(rv.error)) {
            return JSON.parse(rv.error);
          } else {
            return rv.status;
          }
        }),
        shareReplay(1)));

    if (modify) {
      error = error.pipe(modify);
    }
    this.error = error;
    return this;
  }

  addLoading() {
    this.loading = merge(this._loadingSubject, this.response.pipe(mapTo(false)));
    return this;
  }

  addSuccess(modify) {
    var success =
        this.response.pipe(
          filter((rv) => !(rv instanceof HttpErrorResponse)),
          shareReplay(1)
        );
    if (modify) {
      success = success.pipe(modify);
    }
    this.success = success;
    return this;
  }

  post(data) {
    this._loadingSubject.next(true);
    this._dataSubject.next(data);
  }
}
