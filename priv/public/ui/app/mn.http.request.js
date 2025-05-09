/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Subject, of, merge, NEVER, zip } from 'rxjs';
import {HttpErrorResponse } from '@angular/common/http';
import {catchError, switchMap, shareReplay, mapTo, filter, map,
        tap} from 'rxjs/operators';

import {MnHelperService} from './mn.helper.service.js';

export {MnHttpRequest, MnHttpGroupRequest};

class MnHttpGroupRequest {
  constructor(httpMap) {
    this.request = new Subject();
    this.httpMap = httpMap;
    this.fakeMap = Object.keys(this.httpMap).reduce((acc, name) => {
      acc[name] = new Subject();
      return acc;
    }, {});
  }

  clearError() {
    Object.keys(this.httpMap).forEach((key) => this.httpMap[key].clearError());
  }

  addError() {
    this.error =
      zip.apply(null, this.getHttpGroupStreams.bind(this)("response"))
      .pipe(filter((responses) =>
                   responses.find((resp) =>
                                  resp instanceof HttpErrorResponse)))

    return this;
  }

  addSuccess() {
    this.success =
      zip.apply(null, this.getHttpGroupStreams.bind(this)("response"))
      .pipe(filter((responses) =>
                   !responses.find((resp) =>
                                   resp instanceof HttpErrorResponse)));
    return this;
  }

  doOrderedRequest(data) {
    Object.keys(this.httpMap).forEach((key) => {
      if (!data.get(key)) {
        data.set(key, null);
      }
    });
    Array.from(data.keys()).forEach((key) => {
      if (data.get(key) === null) {
        this.fakeMap[key].next(null);
      } else {
        this.httpMap[key].post(data.get(key));
      }
    });
  }

  post(data) {
    data = data || {};
    this.request.next();
    if (data instanceof Map) {
      this.doOrderedRequest(data);
    } else {
      Object.keys(this.httpMap).forEach((key) => this.httpMap[key].post(data[key]));
    }
  }

  getHttpGroupStreams(stream) {
    return Object.keys(this.httpMap).reduce((result, key) => {
      result.push(merge(this.httpMap[key][stream], this.fakeMap[key]));
      return result;
    }, []);
  }

  addLoading() {
    this.loading =
      merge(
        zip.apply(null, this.getHttpGroupStreams.bind(this)("response")).pipe(mapTo(false)),
        this.request.pipe(mapTo(true)));
    return this;
  }
}

class MnHttpRequest {
  constructor(call) {
    this.request = new Subject();
    this._errorSubject = new Subject();
    this._loadingSubject = new Subject();
    this.addResponse(call);
  }

  clearError() {
    this._errorSubject.next(null);
  }

  addResponse(call) {
    let errorsAndSuccess = switchMap((data) => call(data).pipe(catchError((err) => of(err))));
    this.response = this.request.pipe(errorsAndSuccess,
                                      shareReplay({refCount: true, bufferSize: 1}));
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
            let val = JSON.parse(rv.error);
            val.status = rv.status;
            return val;
          } else {
            return rv;
          }
        }),
        (modify ? modify : tap()),
        shareReplay({refCount: true, bufferSize: 1})));

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
          shareReplay({refCount: true, bufferSize: 1})
        );
    if (modify) {
      success = success.pipe(modify);
    }
    this.success = success;
    return this;
  }

  post(data) {
    this._loadingSubject.next(true);
    this.request.next(data);
  }
}
