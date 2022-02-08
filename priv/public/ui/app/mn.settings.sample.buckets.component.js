/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {map, takeUntil, startWith, pairwise, shareReplay,
        pluck, distinctUntilChanged} from 'rxjs/operators';
import {FormControl} from '@angular/forms';
import {combineLatest} from 'rxjs';
import {not, isEmpty} from 'ramda';

import {MnFormService} from './mn.form.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnBucketsService} from './mn.buckets.service.js';
import {MnSettingsSampleBucketsService} from './mn.settings.sample.buckets.service.js';
import {MnTasksService} from './mn.tasks.service.js';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import template from "./mn.settings.sample.buckets.html";

export {MnSettingsSampleBucketsComponent};

class MnSettingsSampleBucketsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    MnAdminService,
    MnBucketsService,
    MnSettingsSampleBucketsService,
    MnPermissions,
    MnTasksService
  ]}

  constructor(mnFormService, mnAdminService, mnBucketsService, mnSettingsSampleBucketsService, mnPermissions, mnTasksService) {
    super();

    this.getSampleBuckets = mnSettingsSampleBucketsService.stream.getSampleBuckets;
    this.sampleBucketsGroupByName = mnSettingsSampleBucketsService.stream.sampleBucketsGroupByName;
    this.isRebalancing = mnAdminService.stream.isRebalancing;
    this.maxBucketCount = mnAdminService.stream.maxBucketCount;
    this.postRequest = mnSettingsSampleBucketsService.stream.installSampleBuckets;
    this.isMixedMode = mnAdminService.stream.isMixedMode;
    this.tasksLoadingSamples = mnTasksService.stream.tasksLoadingSamples;

    this.form = mnFormService.create(this)
      .setFormGroup({})
      .setPackPipe(map(this.getPackedData.bind(this)))
      .setPostRequest(this.postRequest)
      .successMessage("Task added successfully!")
      .clearErrors()
      .showGlobalSpinner();

    let hasClusterBucketsCreate =
        mnPermissions.stream.pipe(pluck("cluster", "buckets", "create"),
                                  distinctUntilChanged());

    combineLatest(this.getSampleBuckets
                    .pipe(startWith(null),
                          pairwise()),
                  this.tasksLoadingSamples
                    .pipe(distinctUntilChanged((p, c) => p.length === c.length)),
                  hasClusterBucketsCreate)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.addFormControls.bind(this));

    this.indexQueryNodes = mnAdminService.stream.allActiveNodes
      .pipe(map(this.areThereIndexQueryNodes.bind(this)));

    this.selectedQuotas =
      combineLatest(this.form.group.valueChanges,
                    this.sampleBucketsGroupByName)
      .pipe(map(this.getSelectedQuotas.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.maxBucketsExceeded =
      combineLatest(mnBucketsService.stream.getBuckets,
                    mnAdminService.stream.maxBucketCount,
                    this.selectedQuotas)
      .pipe(map(([existingBuckets, maximumBuckets, selected]) =>
        (existingBuckets.length + selected.length) > maximumBuckets),
            shareReplay({refCount: true, bufferSize: 1}));

    this.maxQuotaExceeded =
      combineLatest(mnAdminService.stream.getPoolsDefault.pipe(pluck('storageTotals', 'ram')),
                    this.selectedQuotas)
      .pipe(map(this.getQuotaExceeded.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.allInstalled = this.getSampleBuckets
      .pipe(map(buckets => buckets.every(b => b.installed)));

    this.hasSelectedBuckets = this.form.group.valueChanges
      .pipe(startWith(this.form.group.value),
            map(values => Object.values(values).some(v => v)));

    this.isDisabled =
      combineLatest(this.hasSelectedBuckets.pipe(map(not)),
                    this.isRebalancing,
                    this.maxQuotaExceeded,
                    this.maxBucketsExceeded,
                    this.isMixedMode,
                    this.allInstalled,
                    hasClusterBucketsCreate.pipe(map(not)),
                    this.tasksLoadingSamples.pipe(map(t => !isEmpty(t))))
      .pipe(map(conditions => conditions.some(v => v)));
  }

  addFormControls([[oldBuckets, buckets], tasks, hasPermission]) {
    buckets.forEach((bucket, index) => {
      let isInstalling = tasks.find(t => t.bucket === bucket.name);
      let isDisabled = !hasPermission || bucket.installed || isInstalling;
      let bucketControl = this.form.group.get(bucket.name);
      if (bucketControl && oldBuckets) {
        bucketControl[isDisabled ? 'disable' : 'enable']();
        if (bucket.installed != oldBuckets[index].installed) {
          bucketControl.patchValue(bucket.installed);
        }
      } else {
        this.form.group.addControl(bucket.name, new FormControl({
          value: bucket.installed,
          disabled: isDisabled
        }));
      }
    });
  }

  getQuotaExceeded([ram, selectedQuotas]) {
    if (!ram) {
      return;
    }

    let quotaNeeded = selectedQuotas.reduce((acc, val) =>
      (acc + parseInt(val, 10)), 0);

    let {quotaTotalPerNode, quotaUsedPerNode} = ram;

    if (quotaNeeded > (quotaTotalPerNode - quotaUsedPerNode)) {
      return Math.ceil(quotaNeeded - (quotaTotalPerNode - quotaUsedPerNode)) / 1024 / 1024;
    }
  }

  areThereIndexQueryNodes(nodes) {
    return !!nodes.filter(node => (["index", "n1ql"]
                            .some(service => node.services.includes(service)))).length;
  }

  getSelectedQuotas([group, sampleBuckets]) {
    return Object.keys(group)
                 .filter(k => group[k])
                 .map(val => sampleBuckets[val][0].quotaNeeded);
  }

  getPackedData() {
    return Object.keys(this.form.group.value).reduce((acc, val) => {
      if (this.form.group.value[val]) {
        acc.push(val)
      }
      return acc;
    }, []);
  }
}
