/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {FormBuilder} from '@angular/forms';
import {UIRouter} from '@uirouter/angular';
import {pluck, filter, switchMap, distinctUntilChanged, withLatestFrom,
        shareReplay, takeUntil, map} from 'rxjs/operators';
import {combineLatest, Subject, timer, NEVER, of} from 'rxjs';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';

import {MnPermissions, MnStatisticsNew,
        MnServers, $rootScope} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnCollectionsService} from './mn.collections.service.js';
import {MnCollectionsAddScopeComponent} from './mn.collections.add.scope.component.js';
import {MnHelperService} from './mn.helper.service.js';

export {MnCollectionsComponent};

class MnCollectionsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: 'app/mn.collections.html',
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnCollectionsService,
    MnPermissions,
    UIRouter,
    NgbModal,
    FormBuilder,
    MnHelperService,
    MnServers,
    MnStatisticsNew,
    $rootScope
  ]}

  constructor(mnCollectionsService, mnPermissions, uiRouter, modalService,
              formBuilder, mnHelperService, mnServers, mnStatisticsNew,
              $rootScope) {
    super();

    var clickAddScope = new Subject();

    var bucketSelect = formBuilder.group({item: null});

    var setBucket = (v) => bucketSelect.patchValue({item: v});

    var setBucketUrlParam = (name, location) =>
        uiRouter.stateService.go('.', {
          commonBucket: name ? name : null,
          commonScope: null,
          commonCollection: null
        }, {
          notify: false,
          location: location || true
        });

    var getBuckets =
        mnCollectionsService.stream.collectionBuckets;

    var getBucketUrlParam =
        uiRouter.globals.params$.pipe(pluck("commonBucket"),
                                      distinctUntilChanged());
    var getBucketUrlParamDefined =
        combineLatest(
          getBucketUrlParam,
          getBuckets
        ).pipe(switchMap(([param, buckets]) => {
          var hasBucket = buckets.find(bucket => bucket.name === param);
          return hasBucket ? of(hasBucket) : NEVER;
        }));

    var getBucketUrlParamDefinedChanged =
        getBucketUrlParamDefined.pipe(distinctUntilChanged((a, b) => a.name === b.name));

    var bucketsWithParams =
        getBuckets.pipe(withLatestFrom(getBucketUrlParam));

    var statusClass = getBucketUrlParamDefined
        .pipe(map(item =>
                  ("dynamic_" + mnServers.addNodesByStatus(item.nodes).statusClass)),
              shareReplay({refCount: true, bufferSize: 1}));

    getBucketUrlParamDefinedChanged
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(setBucket);

    bucketSelect.get("item").valueChanges
      .pipe(pluck("name"),
            distinctUntilChanged(),
            takeUntil(this.mnOnDestroy))
      .subscribe(setBucketUrlParam);

    bucketsWithParams
      .pipe(filter(([buckets, param]) => param && !buckets
                   .map(bucket => bucket.name).includes(param)),
            pluck(0, 0, "name"),
            takeUntil(this.mnOnDestroy))
      .subscribe(setBucketUrlParam);

    bucketsWithParams
      .pipe(filter(([, param]) => !param),
            pluck(0, 0, "name"),
            takeUntil(this.mnOnDestroy))
      .subscribe(v => setBucketUrlParam(v, "replace"));

    var scopesSorter = mnHelperService.createSorter('name');
    var scopesFilter = mnHelperService.createFilter(this);

    var scopes =
        combineLatest(getBucketUrlParamDefinedChanged,
                      mnCollectionsService.stream.updateManifest,
                      timer(0, 5000))
        .pipe(switchMap(([bucket]) => mnCollectionsService.getManifest(bucket.name)),
              pluck("scopes"),
              scopesSorter.pipe,
              scopesFilter.pipe,
              shareReplay({refCount: true, bufferSize: 1}));

    var scopesPaginator = mnHelperService.createPagenator(this, scopes, "scopesPage");


    clickAddScope
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(() => {
        var ref = modalService.open(MnCollectionsAddScopeComponent);
        ref.componentInstance.bucketName = bucketSelect.get("item").value.name;
      });

    this.scopesSorter = scopesSorter;
    this.scopesFilter = scopesFilter;
    this.scopesPaginator = scopesPaginator;

    this.permissions = mnPermissions.stream;
    this.buckets = getBuckets;
    this.bucketSelect = bucketSelect;
    this.scopes = scopes;
    this.clickAddScope = clickAddScope;
    this.statusClass = statusClass;
    this.$scope = $rootScope.$new();
    this.mnCollectionsStatsPoller = mnStatisticsNew.createStatsPoller(this.$scope);
  }

  ngOnDestroy() {
    this.mnOnDestroy.next();
    this.mnOnDestroy.complete();
    this.$scope.$destroy();
  }

  bucketValuesMapping(bucket) {
    return bucket.name;
  }

  trackByFn(statusClass, _, scope) {
    return this.bucketSelect.get('item').value.name + scope.uid + scope.name + statusClass;
  }
}
