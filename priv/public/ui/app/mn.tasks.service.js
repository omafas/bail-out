/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {BehaviorSubject} from 'rxjs';
import {shareReplay, map, filter} from 'rxjs/operators';
import {groupBy, prop} from 'ramda';

import {singletonGuard} from './mn.core.js';

export {MnTasksService}

class MnTasksService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [] }

  constructor() {
    singletonGuard(MnTasksService);

    this.stream = {};
    this.stream.tasksXDCRPlug = new BehaviorSubject();
    this.stream.tasksXDCR = this.stream.tasksXDCRPlug
      .pipe(shareReplay({refCount: true, bufferSize: 1}));

    this.stream.taskCollectInfoPlug = new BehaviorSubject();
    this.stream.taskCollectInfo = this.stream.taskCollectInfoPlug
      .pipe(shareReplay({refCount: true, bufferSize: 1}));

    this.stream.tasksWarmingUpPlug = new BehaviorSubject();
    this.stream.tasksWarmingUp = this.stream.tasksWarmingUpPlug
      .pipe(shareReplay({refCount: true, bufferSize: 1}));

    this.stream.tasksBucketCompactionPlug = new BehaviorSubject();
    this.stream.tasksCompactionByBucket = this.stream.tasksBucketCompactionPlug
      .pipe(filter(v => v !== undefined),
            map(groupBy(prop('bucket'))),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.tasksViewCompactionPlug = new BehaviorSubject([]);
    this.stream.tasksCompactionByView = this.stream.tasksViewCompactionPlug
        shareReplay({refCount: true, bufferSize: 1});

    this.stream.tasksLoadingSamplesPlug = new BehaviorSubject();
    this.stream.tasksLoadingSamples = this.stream.tasksLoadingSamplesPlug
      .pipe(filter(v => v !== undefined),
            shareReplay({refCount: true, bufferSize: 1}));
  }
}
