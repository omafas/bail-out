/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { BehaviorSubject } from "/ui/web_modules/rxjs.js";
import { shareReplay } from '/ui/web_modules/rxjs/operators.js';
import { Injectable } from "/ui/web_modules/@angular/core.js";

export { MnTasksService }

class MnTasksService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
  ]}

  constructor() {
    this.stream = {};
    this.stream.tasksXDCRPlug = new BehaviorSubject();
    this.stream.tasksXDCR = this.stream.tasksXDCRPlug
      .pipe(shareReplay({refCount: true, bufferSize: 1}));
  }
}
