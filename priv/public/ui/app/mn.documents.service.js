/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Injectable } from "@angular/core";
import { HttpClient } from '@angular/common/http';
import { UIRouter } from '@uirouter/angular';
import { switchMap, shareReplay, pluck, withLatestFrom,
         combineLatest, distinctUntilChanged } from 'rxjs/operators';
import { Subject, BehaviorSubject } from 'rxjs';

export { MnDocumentsService }

class MnDocumentsService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    UIRouter
  ]}

  constructor(http, uiRouter) {
    this.http = http;
    this.stream = {};

    this.stream.recalculateRandomDocument = new BehaviorSubject();
    this.stream.getManualDocument = new Subject();

    this.commonBucket = uiRouter.globals.params$
      .pipe(pluck('commonBucket'),
            distinctUntilChanged());

    this.stream.getDocuments = new BehaviorSubject()
      .pipe(switchMap(this.getDocuments.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getDocument = this.stream.getManualDocument
      .pipe(switchMap(this.getDocument.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getRandomDocument =
      this.stream.recalculateRandomDocument
      .pipe(combineLatest(this.commonBucket),
            pluck(1),
            switchMap(this.getRandomKey.bind(this)),
            pluck('key'),
            withLatestFrom(this.commonBucket),
            switchMap(this.getDocument.bind(this)),
            shareReplay({ bufferSize: 1, refCount: true }));
  }

  getDocumentsURI(params) {
    let bucket = params.bucket || params.commonBucket;
    let base = "/pools/default/buckets/" + encodeURIComponent(bucket);

    return base + "/docs";
  }

  buildDocumentUrl(params) {
    return this.getDocumentsURI(params) + '/' + encodeURIComponent(params.documentId);
  }

  getDocument([key, bucket]) {
    let params = { bucket, documentId: key}
    return this.http.get(this.buildDocumentUrl(params));
  }

  getDocuments(params) {
    return this.http.get(this.getDocumentsURI(params));
  }

  getRandomKey(bucket) {
    return this.http.get(`/pools/default/buckets/${encodeURIComponent(bucket)}/localRandomKey`);
  }
}
