/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {Subject, combineLatest, timer, merge} from 'rxjs';
import {distinctUntilChanged, pluck, map, shareReplay,
        withLatestFrom, filter, takeUntil, switchMap, mapTo} from 'rxjs/operators';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnTasksService} from './mn.tasks.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnBucketsService} from './mn.buckets.service.js';
import {MnPermissions} from './ajs.upgraded.providers.js';

import {MnBucketDialogComponent} from './mn.bucket.dialog.component.js';
import {MnBucketDeleteDialogComponent} from './mn.bucket.delete.dialog.component.js';
import {MnBucketFlushDialogComponent} from './mn.bucket.flush.dialog.component.js';
import template from "./mn.bucket.item.details.html";

export {MnBucketItemDetailsComponent};

class MnBucketItemDetailsComponent extends MnLifeCycleHooksToStream {
  static get annotations() {
    return [
      new Component({
        selector: 'mn-bucket-item-details',
        template,
        inputs: [
          'bucket'
        ],
        changeDetection: ChangeDetectionStrategy.OnPush
      })
    ];
  }

  static get parameters() {
    return [
      MnBucketsService,
      MnAdminService,
      MnTasksService,
      MnPermissions,
      NgbModal
    ];
  }

  constructor(mnBucketsService, mnAdminService, mnTasksService, mnPermissions, modalService) {
    super();

    this.permissions = mnPermissions.stream;
    this.mnTasksService = mnTasksService;
    this.mnBucketsService = mnBucketsService;
    this.modalService = modalService;

    let currentBucket = this.mnOnChanges
      .pipe(filter(v => v !== undefined),
            pluck('bucket', 'currentValue'));

    this.bucketName = currentBucket
      .pipe(pluck('name'),
            distinctUntilChanged());

    this.bucketType = currentBucket
      .pipe(pluck('bucketType'),
            distinctUntilChanged());

    this.bucketControllers = currentBucket
      .pipe(pluck('controllers'),
            distinctUntilChanged());

    this.bucketRamConfig = currentBucket
      .pipe(map(v => mnBucketsService.getRamConfigParams(v)),
            map(v => mnBucketsService.getRamConfig(v)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.bucketRamConfigTotal = this.bucketRamConfig
      .pipe(pluck('topRight', 'value'));

    this.bucketDiskConfig = currentBucket
      .pipe(map(v => mnBucketsService.getDiskConfigParams(v)),
            map(v => mnBucketsService.getDiskConfig(v)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.bucketDiskConfigTotal = this.bucketDiskConfig
      .pipe(pluck('topRight', 'value'));

    this.ejectionMethod = currentBucket
      .pipe(map(v => mnBucketsService.prepareEjectionMethodText(v)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.storageBackend = currentBucket
      .pipe(map(v => mnBucketsService.prepareStorageBackendText(v)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.warmUpTasks = mnTasksService.stream.tasksWarmingUp
      .pipe(withLatestFrom(currentBucket),
            filter(([tasks, bucket]) => tasks && bucket),
            map(v => mnBucketsService.getWarmUpTasks(v)));

    this.isRebalancing = mnAdminService.stream.isRebalancing;
  }

  ngOnInit() {
    this.compactionTask =
      combineLatest(this.mnTasksService.stream.tasksCompactionByBucket,
                    this.bucketName)
      .pipe(map(v => this.mnBucketsService.getCompactionTask(v)),
            distinctUntilChanged());

    this.compactionProgress = this.compactionTask
      .pipe(map(v => this.mnBucketsService.prepareCompactionProgressText(v)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.showCompactBtn =
      combineLatest(this.compactionTask,
                    this.bucketName,
                    this.bucketType,
                    this.permissions)
      .pipe(map(v => this.mnBucketsService.showCompactBtn(v)));

    this.clickCompact = new Subject();
    let postCompact = this.clickCompact
      .pipe(map(this.packCompactURL.bind(this)),
            switchMap((url) => this.mnBucketsService.postCompact(url)),
            shareReplay({refCount: true, bufferSize: 1}));

    let after10secsCompact = postCompact
      .pipe(switchMap(() => timer(10000)));

    this.disableCompactBtn =
      merge(postCompact.pipe(mapTo(true)),
            after10secsCompact.pipe(mapTo(false)));

    this.showCancelCompactBtn =
      combineLatest(this.compactionTask,
                    this.bucketName,
                    this.permissions)
      .pipe(map(v => this.mnBucketsService.showCancelCompactBtn(v)));

    let cancelCompactURL = this.compactionTask
      .pipe(filter(v => !!v),
            pluck('cancelURI'),
            distinctUntilChanged(),
            shareReplay({refCount: true, bufferSize: 1}));

    this.clickCancelCompact = new Subject();
    let postCancelCompact =
      combineLatest(this.clickCancelCompact.pipe(map(this.stopEvent.bind(this))),
                    cancelCompactURL)
      .pipe(switchMap(([, url]) => this.mnBucketsService.postCompact(url)),
            shareReplay({refCount: true, bufferSize: 1}));

    let after10secsCancelCompact = postCancelCompact
      .pipe(switchMap(() => timer(10000)));

    this.disableCancelCompactBtn =
      merge(postCancelCompact.pipe(mapTo(true)),
            after10secsCancelCompact.pipe(mapTo(false)));

    this.showFlushBtn =
      combineLatest(this.bucketControllers,
                    this.bucketName,
                    this.permissions)
      .pipe(map(v => this.mnBucketsService.showFlushBtn(v)));

    this.clickDelete = new Subject();
    this.clickDelete
      .pipe(map(this.stopEvent.bind(this)),
            takeUntil(this.mnOnDestroy))
      .subscribe(() => {
        let ref = this.modalService.open(MnBucketDeleteDialogComponent);
        ref.componentInstance.bucket = this.bucket;
      });

    this.clickEdit = new Subject();
    this.clickEdit
      .pipe(map(this.stopEvent.bind(this)),
            takeUntil(this.mnOnDestroy))
      .subscribe(() => {
        let ref = this.modalService.open(MnBucketDialogComponent);
        ref.componentInstance.bucket = this.bucket;
      });

    this.clickFlush = new Subject();
    this.clickFlush
      .pipe(map(this.stopEvent.bind(this)),
            takeUntil(this.mnOnDestroy))
      .subscribe(() => {
        let ref = this.modalService.open(MnBucketFlushDialogComponent);
        ref.componentInstance.bucket = this.bucket;
      });
  }

  stopEvent(event) {
    event.stopPropagation();
  }

  packCompactURL(event) {
    event.stopPropagation();
    return this.bucket.controllers.compactAll;
  }
}
