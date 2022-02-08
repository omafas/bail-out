/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt. As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Injectable } from "@angular/core";
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { UIRouter } from '@uirouter/angular';
import { switchMap, shareReplay, map, pluck, filter } from 'rxjs/operators';
import { BehaviorSubject, timer, combineLatest } from 'rxjs';
import { partition, compose, sortBy, reverse, prop, filter as ramdaFilter } from 'ramda';

import { MnPermissions } from './ajs.upgraded.providers.js';

import { MnHttpRequest } from './mn.http.request.js';

export { MnViewsListService }

class MnViewsListService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    UIRouter,
    MnPermissions
  ]}

  constructor(http, uiRouter, mnPermissions) {
    this.http = http;
    this.stream = {};
    this.permissions = mnPermissions.stream;

    this.commonBucket = uiRouter.globals.params$
      .pipe(pluck('commonBucket'));

    this.ddocumentId = uiRouter.globals.params$
      .pipe(pluck('ddocumentId'));

    this.bucketNames = this.permissions
      .pipe(filter(p => Object.keys(p.bucketNames).length),
            map(p => p.bucketNames['.views!read']));

    this.stream.updateDdocsPoller = new BehaviorSubject();

    this.stream.getDdocsResponse =
      combineLatest(this.commonBucket,
                    this.bucketNames,
                    timer(0, 4000),
                    this.stream.updateDdocsPoller)
      .pipe(filter(([bucket, existingBuckets]) => existingBuckets.includes(bucket)),
            switchMap(this.getDdocs.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getDdoc =
      combineLatest(this.commonBucket,
                    this.ddocumentId)
      .pipe(map(val => this.getDdocUrl(val)),
            switchMap(this.getDdoc.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getDdocs = this.stream.getDdocsResponse
      .pipe(map(response => response.body),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getDdocsStatus = this.stream.getDdocsResponse
      .pipe(map(response => response.status),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getDdocsByType = this.stream.getDdocs
      .pipe(map(ddocs => {
        let [development, production] = partition(this.isDevModeDoc, ddocs.rows);
        return { development, production } }),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.postDdoc =
      new MnHttpRequest(this.postDdoc.bind(this))
      .addSuccess()
      .addError();

    this.stream.deleteDdoc =
      new MnHttpRequest(this.deleteDdoc.bind(this))
      .addSuccess()
      .addError();
  }

  getDdocUrl([bucket, id, prefix]) {
    let encodedBucket = encodeURIComponent(bucket);

    if (prefix) {
      id = prefix + id;
    }

    return '/couchBase/' + encodedBucket + '/' + id;
  }

  getDdoc(url) {
    return this.http.get(url);
  }

  getDdocs([bucket,]) {
    return this.http.get(`/pools/default/buckets/${bucket}/ddocs`, { observe: 'response' });
  }

  postDdoc(data) {
    return this.http.put(data.url, data.json, { headers: new HttpHeaders().set("isNotForm", true) });
  }

  deleteDdoc(url) {
    return this.http.delete(url);
  }

  isDevModeDoc(row) {
    let devPrefix = "_design/dev_";
    return row.doc.meta.id.substring(0, devPrefix.length) === devPrefix;
  }

  removeDesignPrefix(id) {
    return id.replace("_design/dev_", "");
  }

  addDevPrefix(id) {
    return id.replace("_design/", "_design/dev_");
  }

  prepareCompactionProgressText(compactionTask) {
    return compactionTask ? (compactionTask.progress + '% complete') : '';
  }

  /* the bucket tasks are sorted in reverse alphabetical order,
   * as view_compaction must take precedence over indexing tasks. */
  getCompactionTask([compactionTasks, bucketName]) {
    let sorter = compose(reverse(),
                         sortBy(prop('type')),
                         ramdaFilter(t => t.bucket == bucketName));

    return compactionTasks && sorter(compactionTasks)[0];
  }

  showCompactBtn([compactionTask, bucketName, permissions]) {
    return (!compactionTask || !compactionTask.cancelURI) &&
            permissions.cluster.tasks.read &&
            permissions.cluster.bucket[bucketName].compact;
  }

  showCancelCompactBtn([compactionTask, bucketName, permissions]) {
    return (compactionTask && compactionTask.cancelURI) &&
            permissions.cluster.tasks.read &&
            permissions.cluster.bucket[bucketName].compact;
  }

  postCompact(postURL) {
    return this.http.post(postURL);
  }
}
