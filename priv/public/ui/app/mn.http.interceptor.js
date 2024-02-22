/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {HttpParams} from '@angular/common/http';
import {is} from 'ramda';
import {throwError} from 'rxjs';
import {tap, catchError} from 'rxjs/operators';

import { MnAppService } from './mn.app.service.js';

export { MnHttpInterceptor };

class MnHttpInterceptor {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    MnAppService
  ]}

  constructor(mnAppService) {
    this.httpResponse = mnAppService.stream.httpResponse;
  }

  intercept(req, next) {
    var mnReq = req.clone({
      setHeaders: {
        'invalid-auth-response': 'on',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'ns-server-ui': 'yes'
      }
    });

    var params;
    var headers;

    if ((req.method === 'POST' || req.method === 'PUT')) {
      if (!req.headers.get('isNotForm')) {
        if (is(Object, mnReq.body) && !Array.isArray(mnReq.body)) {
          params = new HttpParams({
            encoder: new CustomEncoder(),
            fromObject: mnReq.body
          });
        } else {
          params = mnReq.body;
        }
        mnReq = mnReq.clone({
          body: params,
          responseType: 'text',
          headers: mnReq.headers.set(
            'Content-Type', 'application/x-www-form-urlencoded; charset=UTF-8')});
      }
      if (req.headers.has('isNotForm')) {
        headers = mnReq.headers.delete('isNotForm');
        mnReq = mnReq.clone({headers: headers, responseType: 'text'});
      }
    }

    return next
      .handle(mnReq).pipe(
        tap((event) => {
          this.httpResponse.next(event);
        }),
        catchError((event) => {
          this.httpResponse.next(event);
          return throwError(event);
        })
      );
  }
}

class CustomEncoder {
  encodeKey(key) {
    return encodeURIComponent(key);
  }

  encodeValue(value) {
    return encodeURIComponent(value);
  }

  decodeKey(key) {
    return decodeURIComponent(key);
  }

  decodeValue(value) {
    return decodeURIComponent(value);
  }
}
