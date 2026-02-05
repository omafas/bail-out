/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {of, NEVER, combineLatest, merge} from 'rxjs';
import {map, filter, switchMap, shareReplay, takeUntil, startWith, distinctUntilChanged, withLatestFrom} from 'rxjs/operators';
import {FormBuilder} from '@angular/forms';

import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {collectionDelimiter, MnXDCRService} from "./mn.xdcr.service.js";
import {MnBucketsService} from "./mn.buckets.service.js";
import {MnCollectionsService} from './mn.collections.service.js';

import {MnPermissions} from './ajs.upgraded.providers.js';
import template from "./mn.xdcr.conflict.log.html";

export {MnXDCRConflictLogComponent};

class MnXDCRConflictLogComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-conflict-log",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "mappingRules",
        "mappingGroup",
        "conflictLogging",
        "group",
        "sourceBucket"
      ]
    })
  ]}

  static get parameters() { return [
    MnXDCRService,
    MnBucketsService,
    MnCollectionsService,
    MnPermissions,
    FormBuilder
  ]}

  constructor(mnXDCRService, mnBucketsService, mnCollectionsService,
    mnPermissions, formBuilder) {
    super();
    this.mnCollectionsService = mnCollectionsService;
    this.bucketsService = mnBucketsService;
    this.enableCrossClusterVersioningBuckets = this.bucketsService.stream.bucketsCrossClusterVersioningEnabled;
    this.formBuilder = formBuilder;

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
    this.sourceBucketName =
      (this.sourceBucket ? of(this.sourceBucket) : this.group.get("fromBucket").valueChanges);

    this.scopes = this.sourceBucketName
      .pipe(filter(v => !!v),
        distinctUntilChanged(),
        withLatestFrom(this.mnPermissions.stream),
        switchMap(([bucketName, permission]) =>
          permission.cluster.collection[bucketName + ':.:.'].collections.read ?
            this.mnCollectionsService.getManifest(bucketName) :
            NEVER),
        map(v => [v.scopes.filter(s => s.name !== '_system')]),
        shareReplay({refCount: true, bufferSize: 1}));


    combineLatest(this.scopes, this.conflictLogging).pipe(takeUntil(this.mnOnDestroy)).subscribe(([scopes, conflictLogging]) => {
      if (scopes.length) {
        scopes[0].forEach(scope => {
          this.mappingGroup.ruleControls.scopes[scope.name] = this.formBuilder.group({});
          this.mappingGroup.ruleControls.scopes[scope.name].addControl(`scopes_${scope.name}_target`, this.formBuilder.control(this.getScopeTarget(scope.name, conflictLogging.loggingRules)));
          this.mappingGroup.ruleControls.scopes[scope.name].addControl('bucket', this.formBuilder.control({value: '', disabled: true}));
          this.mappingGroup.ruleControls.scopes[scope.name].addControl('collection', this.formBuilder.control({value: '', disabled: true}));
          this.mappingGroup.ruleControls.scopes[scope.name].addControl(`conflict_log_custom_collections_${scope.name}`, this.formBuilder.control(this.hasCustomCollectionRule(scope.name, conflictLogging.loggingRules)));
          this.mappingGroup.ruleControls.scopes[scope.name].collections = {};
          if (scope.collections.length) {
            scope.collections.forEach(collection => {
              this.mappingGroup.ruleControls.scopes[scope.name].collections[collection.name] = this.formBuilder.group({});
              this.mappingGroup.ruleControls.scopes[scope.name].collections[collection.name].addControl(`collections_${collection.name}_target`, this.formBuilder.control(this.getCollectionTarget(scope.name, collection.name, conflictLogging.loggingRules)));
              this.mappingGroup.ruleControls.scopes[scope.name].collections[collection.name].addControl('bucket', this.formBuilder.control({value: '', disabled: true}));
              this.mappingGroup.ruleControls.scopes[scope.name].collections[collection.name].addControl('collection', this.formBuilder.control({value: '', disabled: true}));
            });
          }
        });
      }

      Object.keys(conflictLogging.loggingRules || {}).forEach(rule => {
        if (conflictLogging.loggingRules[rule] && conflictLogging.loggingRules[rule].bucket && conflictLogging.loggingRules[rule].collection) {
          let ruleGroup = this.formBuilder.group({});
          ruleGroup.addControl(rule, this.formBuilder.control(rule));
          ruleGroup.addControl('bucket', this.formBuilder.control({value: conflictLogging.loggingRules[rule].bucket, disabled: false}));
          ruleGroup.addControl('collection', this.formBuilder.control({value: conflictLogging.loggingRules[rule].collection, disabled: false}));
          this.mappingGroup[rule] = ruleGroup;
          if (scopes.length) {
            // update the scope and collections controls with the existing rules
            if (rule.includes(collectionDelimiter)) {
              // scope.collection rule
              const [ruleScope, ruleColl] = rule.split(collectionDelimiter);
              this.mappingGroup.ruleControls.scopes[ruleScope].collections[ruleColl].setControl('bucket', this.formBuilder.control({value: conflictLogging.loggingRules[rule].bucket, disabled: false}));
              this.mappingGroup.ruleControls.scopes[ruleScope].collections[ruleColl].setControl('collection', this.formBuilder.control({value: conflictLogging.loggingRules[rule].collection, disabled: false}));
              this.mappingGroup.ruleControls.scopes[ruleScope].collections[ruleColl].setControl(`collections_${ruleColl}_target`, this.formBuilder.control({value: 'custom', disabled: false}));
            } else {
              // scope rule
              this.mappingGroup.ruleControls.scopes[rule].setControl('bucket', this.formBuilder.control({value: conflictLogging.loggingRules[rule].bucket, disabled: false}));
              this.mappingGroup.ruleControls.scopes[rule].setControl('collection', this.formBuilder.control({value: conflictLogging.loggingRules[rule].collection, disabled: false}));
              this.mappingGroup.ruleControls.scopes[rule].setControl(`scopes_${rule}_target`, this.formBuilder.control({value: 'custom', disabled: false}));
            }
          }
        }
      });
    });

    let hasSourceBucketField = this.group.get("fromBucket");
    if (hasSourceBucketField) {
      hasSourceBucketField.valueChanges
      .pipe(startWith(hasSourceBucketField.value),
        takeUntil(this.mnOnDestroy))
      .subscribe(v => {
        let action = v ? "enable" : "disable";
        this.group.get("conflictLogMapping")[action]({onlySelf: true});
        this.group.get("enableConflictLog")[action]({onlySelf: true});
      });
    }

    combineLatest(
      this.sourceBucketName,
      this.enableCrossClusterVersioningBuckets,
      this.conflictLogging)
    .pipe(takeUntil(this.mnOnDestroy))
    .subscribe(([sourceBucketName, enableCrossClusterVersioningBuckets, conflictLogging]) => {
      let action = (sourceBucketName && enableCrossClusterVersioningBuckets.includes(sourceBucketName)) ? "enable" : "disable";
      this.group.get("conflictLogMapping")[action]({onlySelf: true});
      this.group.get("enableConflictLog")[action]({onlySelf: true});


      let hasConflictLoggingRules = !!((conflictLogging.bucket && conflictLogging.collection) || conflictLogging.loggingRules);
      if (action === 'disable') {
        this.group.get("conflictLogMapping").patchValue(false);
        this.group.get("enableConflictLog").patchValue(false);
      } else {
        this.group.get("conflictLogMapping").patchValue(hasConflictLoggingRules);
        this.group.get("enableConflictLog").patchValue(hasConflictLoggingRules ? !conflictLogging.disabled : false);
      }
    });

    this.group.get("enableConflictLog").valueChanges
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe((v) => this.mappingGroup?.rootControls.get("enableConflictLog").patchValue(v, {onlySelf: true}));
  }

  getScopeTarget(scopeName, rules) {
    const hasCustomTarget = rules && rules[scopeName]?.bucket && rules[scopeName]?.collection;
    const hasNullTarget = rules && rules[scopeName] === null;

    if (hasCustomTarget) {
      return 'custom';
    }

    if (hasNullTarget) {
      return 'null';
    }

    return 'default';
  }

  getCollectionTarget(scopeName, collectionName, rules) {
    const ruleKey = `${scopeName}${collectionDelimiter}${collectionName}`;
    const hasCustomTarget = rules && rules[ruleKey]?.bucket && rules[scopeName]?.collection;
    const hasNullTarget = rules && rules[ruleKey] === null;
    const hasDefaultTarget = rules && rules[ruleKey] && (typeof rules[ruleKey] === 'object' && Object.keys(rules[ruleKey]).length === 0);

    if (hasCustomTarget) {
      return 'custom';
    }

    if (hasNullTarget) {
      return 'null';
    }

    if (hasDefaultTarget) {
      return 'default';
    }

    return 'parent';
  }

  hasCustomCollectionRule(scopeName, rules) {
    return !!(rules && Object.keys(rules).find(rule => rule.startsWith(`${scopeName}${collectionDelimiter}`)));
  }
}
