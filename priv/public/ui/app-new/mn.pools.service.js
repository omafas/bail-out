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
import { BehaviorSubject } from '../web_modules/rxjs.js';
import { switchMap,
         shareReplay,
         pluck,
         distinctUntilChanged,
         map
       } from '../web_modules/rxjs/operators.js';
import { MnParseVersion } from './mn.pipes.js'

export { MnPoolsService };

let launchID =  (new Date()).valueOf() + '-' + ((Math.random() * 65536) >> 0);

class MnPoolsService {
  static annotations = [
    new Injectable()
  ]

  static parameters = [
    HttpClient,
    MnParseVersion
  ]

  constructor(http, mnParseVersionPipe) {
    this.http = http;
    this.stream = {};

    this.stream.getSuccess =
      (new BehaviorSubject()).pipe(switchMap(this.get), shareReplay(1));

    this.stream.isEnterprise =
      this.stream.getSuccess.pipe(pluck("isEnterprise"), distinctUntilChanged());

    this.stream.implementationVersion =
      this.stream.getSuccess.pipe(pluck("implementationVersion"));

    this.stream.majorMinorVersion =
      this.stream.implementationVersion.pipe(
        map(mnParseVersionPipe.transform.bind(mnParseVersionPipe)),
        map(function (rv) {
          return rv[0].split('.').splice(0,2).join('.');
        })
      );

    this.stream.mnServices =
      this.stream.isEnterprise
      .pipe(map(function (isEnterprise) {
        return isEnterprise ?
          ["kv", "index", "fts", "n1ql", "eventing", "cbas"] :
          ["kv", "index", "fts", "n1ql"];
      }), shareReplay(1));

    this.stream.quotaServices =
      this.stream.isEnterprise
      .pipe(map(function (isEnterprise) {
        return isEnterprise ?
          ["kv", "index", "fts", "eventing", "cbas"] :
          ["kv", "index", "fts"];
      }), shareReplay(1));
  }

  get() {
    return this.http.get('/pools').pipe(
      map(function (pools) {
        pools.isInitialized = !!pools.pools.length;
        pools.launchID = pools.uuid + '-' + launchID;
        return pools;
      })
    );
  }
}
