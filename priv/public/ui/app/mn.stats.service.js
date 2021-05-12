/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Injectable } from "/ui/web_modules/@angular/core.js";
import { HttpClient } from '/ui/web_modules/@angular/common/http.js';
import { map } from '/ui/web_modules/rxjs/operators.js';
import { MnHttpRequest } from './mn.http.request.js';

export { MnStatsService };

class MnStatsService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient
  ]}

  constructor(http) {
    this.http = http;

    this.stream = {};
  }

  postStatsRange(configs) {
    return this.http.post("/pools/default/stats/range/", configs)
      .pipe(map(resp => JSON.parse(resp)));
  }
}
