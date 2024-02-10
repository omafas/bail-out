/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Injectable } from "@angular/core";
import { UIRouter } from "@uirouter/angular";
import { switchMap, pluck, distinctUntilChanged, withLatestFrom } from 'rxjs/operators';
import { combineLatest, NEVER } from "rxjs";
import { of } from "ramda";

import { MnLifeCycleHooksToStream } from './mn.core.js';

export { MnRouterService }

class MnRouterService extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    UIRouter
  ]}

  constructor(uiRouter) {
    super();

    this.uiRouter = uiRouter;
  }

  createBucketDropdown(getBuckets) {
    let getBucketUrlParam = this.uiRouter.globals.params$
      .pipe(pluck("commonBucket"),
            distinctUntilChanged());

    let getBucketUrlParamDefined =
        combineLatest(getBucketUrlParam,
                      getBuckets)
        .pipe(switchMap(([param, buckets]) => {
          let hasBucket = buckets.find(bucket => bucket.name === param);
          return hasBucket ? of(hasBucket) : NEVER;
        }));

    let getBucketUrlParamDefinedChanged = getBucketUrlParamDefined
        .pipe(distinctUntilChanged((a, b) => a.name === b.name));

    let bucketsWithParams = getBuckets
      .pipe(withLatestFrom(getBucketUrlParam));

    return { getBucketUrlParamDefinedChanged, bucketsWithParams }
  }
}
