/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {combineLatest, merge} from 'rxjs';
import {takeUntil} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnPoolsService} from "./mn.pools.service.js";
import {MnAdminService} from "./mn.admin.service.js";
import {MnXDCRService} from "./mn.xdcr.service.js";
import {MnBucketsService} from "./mn.buckets.service.js";

import {MnPermissions} from './ajs.upgraded.providers.js';
import template from "./mn.xdcr.mobile.setting.html";

export {MnXDCRMobileSettingComponent};

class MnXDCRMobileSettingComponent extends MnLifeCycleHooksToStream {
  static get annotations() {
    return [
      new Component({
        selector: "mn-xdcr-mobile-setting",
        template,
        changeDetection: ChangeDetectionStrategy.OnPush,
        inputs: [
          "group",
          "fromBucket"
        ]
      })
    ]
  }

  static get parameters() {
    return [
      MnPoolsService,
      MnAdminService,
      MnXDCRService,
      MnBucketsService,
      MnPermissions
    ]
  }

  constructor(mnPoolsService, mnAdminService, mnXDCRService, mnBucketsService,
    mnPermissions) {
    super();

    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.compatVersion80 = mnAdminService.stream.compatVersion80;

    this.bucketsService = mnBucketsService;
    this.enableCrossClusterVersioningBuckets = this.bucketsService.stream.bucketsCrossClusterVersioningEnabled;

    let postCreateReplication = mnXDCRService.stream.postCreateReplication;
    let postSettingsReplications = mnXDCRService.stream.postSettingsReplications;
    let postSettingsReplicationsValidation = mnXDCRService.stream.postSettingsReplicationsValidation;
    let postCreateReplicationValidation = mnXDCRService.stream.postCreateReplicationValidation;
    this.error = merge(
      postCreateReplication.error,
      postSettingsReplications.error,
      postSettingsReplicationsValidation.error,
      postCreateReplicationValidation.error);

    this.mnPermissions = mnPermissions;
  }

  ngOnInit() {
    if (!this.fromBucket.value) {
      this.group.get("mobile").disable({onlySelf: true});
    }

    if (this.fromBucket.valueChanges) {
      combineLatest(
        this.fromBucket.valueChanges,
        this.enableCrossClusterVersioningBuckets)
      .pipe(takeUntil(this.mnOnDestroy)).subscribe(this.handleBucketChange.bind(this));
    } else {
      this.enableCrossClusterVersioningBuckets.pipe(takeUntil(this.mnOnDestroy)).subscribe((enableCrossClusterVersioningBuckets) =>
        this.handleBucketChange([this.fromBucket.value, enableCrossClusterVersioningBuckets])
      )
    }
  }

  handleBucketChange([sourceBucketName, enableCrossClusterVersioningBuckets]) {
    let action = (sourceBucketName && enableCrossClusterVersioningBuckets.includes(sourceBucketName)) ? "enable" : "disable";
    this.group.get("mobile")[action]({onlySelf: true});
    if (action === 'disable') {
      this.group.get('mobile').patchValue(false);
    }
  }
}
