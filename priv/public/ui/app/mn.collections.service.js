/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/
import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {BehaviorSubject} from 'rxjs';
import {map, shareReplay} from 'rxjs/operators';

import {MnHttpRequest} from './mn.http.request.js';
import {MnBucketsService} from './mn.buckets.service.js';
import {MnPermissions} from './ajs.upgraded.providers.js';
import {singletonGuard} from './mn.core.js';

const restApiBase = "/pools/default/buckets";

export {MnCollectionsService}

class MnCollectionsService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnBucketsService,
    MnPermissions
  ]}

  constructor(http, mnBucketsService, mnPermissions) {
    singletonGuard(MnCollectionsService);

    this.http = http;
    this.stream = {};

    this.stream.updateManifest =
      new BehaviorSubject();

    this.stream.addScopeHttp =
      new MnHttpRequest(this.addScope.bind(this))
      .addSuccess()
      .addError(map(error => {
        if (error.status === 404) {
          return {errors: {bucketName: "This bucket doesn't exist"}};
        }
        if (typeof error === "string") {
          //hanlde "Scope with this name already exists" error
          return {errors: {name: error}};
        }
        return error;
      }));

    this.stream.deleteScopeHttp =
      new MnHttpRequest(this.deleteScope.bind(this))
      .addSuccess()
      .addError();

    this.stream.addCollectionHttp =
      new MnHttpRequest(this.addCollection.bind(this))
      .addSuccess()
      .addError();

    this.stream.deleteCollectionHttp =
      new MnHttpRequest(this.deleteCollection.bind(this))
      .addSuccess()
      .addError();

    this.stream.collectionBuckets = mnBucketsService.stream.bucketsMembaseEphemeral
      .pipe(map(buckets => buckets
                .filter(bucket => {
                  let scope = mnPermissions.export.cluster.collection[bucket.name + ':.:.'];
                  return scope && scope.collections.read;
                })),
            shareReplay({refCount: true, bufferSize: 1}));
  }

  extractInterestingStatsPipe(statsStream) {
    return statsStream.pipe(
      map(stats => Object.keys(stats).reduce((acc, statName) => {
        if (stats[statName] && stats[statName]["aggregate"]) {
          acc[statName] = stats[statName]["aggregate"].values
            .slice()
            .reverse()
            .find(stat => stat != null)[1];
        }
        return acc;
      }, {})),
      shareReplay({refCount: true, bufferSize: 1}));
  }

  getManifest(bucket) {
    bucket = encodeURIComponent(bucket);
    return this.http.get(`${restApiBase}/${bucket}/scopes`);
  }

  addScope({name, bucketName}) {
    bucketName = encodeURIComponent(bucketName);
    return this.http.post(`${restApiBase}/${bucketName}/scopes`, {
      name: name
    });
  }

  addCollection([bucket, scope, name, ttl]) {
    bucket = encodeURIComponent(bucket);
    scope = encodeURIComponent(scope);
    return this.http.post(`${restApiBase}/${bucket}/scopes/${scope}/collections`, {name: name, maxTTL: ttl || 0});
  }

  deleteScope([bucket, scope]) {
    bucket = encodeURIComponent(bucket);
    scope = encodeURIComponent(scope);
    return this.http.delete(`${restApiBase}/${bucket}/scopes/${scope}`);
  }

  deleteCollection([bucket, scope, collection]) {
    bucket = encodeURIComponent(bucket);
    scope = encodeURIComponent(scope);
    collection = encodeURIComponent(collection);
    return this.http.delete(`${restApiBase}/${bucket}/scopes/${scope}/collections/${collection}`);
  }
}
