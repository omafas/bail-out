/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {BehaviorSubject, combineLatest} from 'rxjs';
import {map, takeUntil} from 'rxjs/operators';

import {MnPoolsService} from './mn.pools.service.js';
import {MnSettingsAutoCompactionService} from './mn.settings.auto.compaction.service.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnPermissions} from './ajs.upgraded.providers.js';

export {MnSettingsAutoCompactionFormComponent};

class MnSettingsAutoCompactionFormComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-auto-compaction-form",
      templateUrl: "app/mn.settings.auto.compaction.form.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        'mnGroup',
        'mnErrors',
        'isBucketSettings',
        'mnStorage'
      ]
    })
  ]}

  static get parameters() { return [
    MnPoolsService,
    MnSettingsAutoCompactionService,
    MnHelperService,
    MnPermissions
  ] }

  constructor(mnPoolsService, mnSettingsAutoCompactionService, mnHelperService, mnPermissions) {
    super();

    this.storageMode = mnSettingsAutoCompactionService.stream.storageMode;
    this.daysOfWeek = mnHelperService.daysOfWeek;
    this.postRequest = mnSettingsAutoCompactionService.stream.postAutoCompaction;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.isMemoryOptimized = mnSettingsAutoCompactionService.stream.isMemoryOptimized;
    this.permissions = mnPermissions.stream;

    this.showIndexFragmentation =
      combineLatest(this.isMemoryOptimized,
                    this.isEnterprise)
      .pipe(map(this.showIndexFragmentation.bind(this)));

    this.showGsiWarning =
      combineLatest(this.isMemoryOptimized,
                    this.isEnterprise)
      .pipe(map(this.showGsiWarning.bind(this)));
  }

  ngOnInit() {
    this.showMagmaFragmentationPercentage = this.isBucketSettings ?
      this.mnStorage.pipe(map(this.isMagma.bind(this))) :
      new BehaviorSubject(true);

    this.purgeIntervalIsOne =
      this.mnGroup.get('purgeInterval').valueChanges
        .pipe(map(interval => interval == 1 ? "" : "s"));

    this.hasWritePermissions = this.permissions
      .pipe(map(permissions => permissions.cluster.settings.autocompaction.write));

    this.thresholdFlags = combineLatest(
      this.mnGroup.get('databaseFragmentationThreshold.percentageFlag').valueChanges,
      this.mnGroup.get('databaseFragmentationThreshold.sizeFlag').valueChanges,
      this.mnGroup.get('viewFragmentationThreshold.percentageFlag').valueChanges,
      this.mnGroup.get('viewFragmentationThreshold.sizeFlag').valueChanges
    );

    this.hasWritePermissions
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.initiallyEnabledControls.bind(this));

    combineLatest(this.hasWritePermissions,
                  this.thresholdFlags)
      .pipe(map(([hasPermission, flags]) => hasPermission && flags.some(v => v)),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this, 'timePeriodFlag'));

    combineLatest(this.hasWritePermissions,
                  this.thresholdFlags,
                  this.mnGroup.get('timePeriodFlag').valueChanges)
      .pipe(map(([hasPermission, flags, checked]) =>
            hasPermission && flags.some(v => v) && checked),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this, 'allowedTimePeriod'));

    this.addThresholdToggle('databaseFragmentationThreshold.percentage');
    this.addThresholdToggle('databaseFragmentationThreshold.size');
    this.addThresholdToggle('viewFragmentationThreshold.percentage');
    this.addThresholdToggle('viewFragmentationThreshold.size');

    combineLatest(this.hasWritePermissions,
                  this.mnGroup.get('indexCompactionMode').valueChanges)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.toggleIndexFragmentation.bind(this));
  }

  showGsiWarning([isMemoryOptimized, isEnterprise]) {
    return isMemoryOptimized || isEnterprise;
  }

  showIndexFragmentation([isMemoryOptimized, isEnterprise]) {
    return !isMemoryOptimized && !isEnterprise && !this.isBucketSettings;
  }

  addThresholdToggle(control) {
    return combineLatest(this.mnGroup.get(`${control}Flag`).valueChanges,
                         this.hasWritePermissions)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(([flag, hasPermission]) =>
        this.mnGroup.get(control)[flag && hasPermission ? 'enable' : 'disable']());
  }

  maybeDisableField(control, enabled) {
    let controlField = this.mnGroup.get(control);
    if (controlField) {
      controlField[enabled ? "enable" : "disable"]();
    }
  }

  initiallyEnabledControls(hasPermission) {
    this.maybeDisableField('databaseFragmentationThreshold.percentageFlag', hasPermission);
    this.maybeDisableField('databaseFragmentationThreshold.sizeFlag', hasPermission);
    this.maybeDisableField('viewFragmentationThreshold.percentageFlag', hasPermission);
    this.maybeDisableField('viewFragmentationThreshold.sizeFlag', hasPermission);
    this.maybeDisableField('purgeInterval', hasPermission);
    this.maybeDisableField('parallelDBAndViewCompaction', hasPermission);
    this.maybeDisableField('indexCompactionMode', hasPermission);
    this.maybeDisableField('indexCircularCompaction', hasPermission);
    this.maybeDisableField('magmaFragmentationPercentage', hasPermission);
  }

  toggleIndexFragmentation([hasPermission, mode]) {
    let enabled = mode == "circular";

    if (hasPermission) {
      this.maybeDisableField('indexFragmentationThreshold', !enabled);
      this.maybeDisableField('indexCircularCompaction', enabled);
    }
  }

  isMagma(mnStorage) {
    return mnStorage === 'magma';
  }
}
