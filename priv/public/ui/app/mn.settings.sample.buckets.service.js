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
import {HttpErrorResponse} from '@angular/common/http';
import {map, switchMap, shareReplay} from 'rxjs/operators';
import {groupBy, prop} from 'ramda';

import {MnHttpRequest} from './mn.http.request.js';
import {MnBucketsService} from "./mn.buckets.service.js";
import {singletonGuard} from './mn.core.js';

export {MnSettingsSampleBucketsService}

class MnSettingsSampleBucketsService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnBucketsService
  ]}

  constructor(http, mnBucketsService) {
    singletonGuard(MnSettingsSampleBucketsService);
    this.http = http;
    this.stream = {};

    this.stream.getSampleBuckets =
      mnBucketsService.stream.bucketsUri
      .pipe(switchMap(this.getSampleBuckets.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.sampleBucketsGroupByName = this.stream.getSampleBuckets
      .pipe(map(groupBy(prop("name"))));

    this.stream.installSampleBuckets =
      new MnHttpRequest(this.installSampleBuckets.bind(this))
      .addSuccess()
      .addError(map(this.extractInstallSampleBucketsError));
  }

  getSampleBuckets() {
    return this.http.get('/sampleBuckets');
  }

  installSampleBuckets(selectedSamples) {
    return this.http.post('/sampleBuckets/install', selectedSamples);
  }

  extractInstallSampleBucketsError(errors) {
    return (errors instanceof HttpErrorResponse) ? [errors.error] : errors;
  }
}
