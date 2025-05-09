/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {timer} from 'rxjs';
import {switchMap, shareReplay} from 'rxjs/operators';
import {singletonGuard} from './mn.core.js';

export {MnLogsListService};

class MnLogsListService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient
  ]}

  constructor(http) {
    singletonGuard(MnLogsListService);
    this.http = http;

    this.stream = {};

    this.stream.logs =
      timer(0, 10000).pipe(
        switchMap(this.getLogs.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));
  }

  getLogs() {
    return this.http.get('/logs');
  }
}
