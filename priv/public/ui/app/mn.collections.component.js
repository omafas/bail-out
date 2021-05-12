/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {FormBuilder} from "/ui/web_modules/@angular/forms.js";
import {UIRouter} from "/ui/web_modules/@uirouter/angular.js";
import {pluck, filter, switchMap, distinctUntilChanged, withLatestFrom,
        shareReplay, takeUntil, tap, map} from '/ui/web_modules/rxjs/operators.js';
import {combineLatest, Subject, timer, NEVER, of} from "/ui/web_modules/rxjs.js";
import {NgbModal} from "/ui/web_modules/@ng-bootstrap/ng-bootstrap.js";
import {MnPermissions, MnStatisticsNewService,
        MnServersService, $rootScope} from '/ui/app/ajs.upgraded.providers.js';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnCollectionsService} from './mn.collections.service.js';
import {MnCollectionsAddScopeComponent} from './mn.collections.add.scope.component.js';

import {MnHelperService} from './mn.helper.service.js';

export {MnCollectionsComponent};

class MnCollectionsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "/ui/app/mn.collections.html",
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
    MnServersService,
    MnStatisticsNewService,
    $rootScope
  ]}

  constructor(mnCollectionsService, mnPermissions, uiRouter, modalService,
              formBuilder, mnHelperService, mnServersService, mnStatisticsNewService,
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
        }), distinctUntilChanged((a, b) => a.name === b.name));

    var bucketsWithParams =
        getBuckets.pipe(withLatestFrom(getBucketUrlParam));

    var statusClass = getBucketUrlParamDefined
        .pipe(map(item =>
                  ("dynamic_" + mnServersService.addNodesByStatus(item.nodes).statusClass)));

    getBucketUrlParamDefined
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
      .pipe(filter(([_, param]) => !param),
            pluck(0, 0, "name"),
            takeUntil(this.mnOnDestroy))
      .subscribe(v => setBucketUrlParam(v, "replace"));

    var scopesSorter = mnHelperService.createSorter(this);
    var scopesFilter = mnHelperService.createFilter(this);

    var scopes =
        combineLatest(getBucketUrlParamDefined,
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
    this.bucketsNames = getBuckets.pipe(map(buckets => buckets.map((bucket) => bucket.name)));
    this.bucketSelect = bucketSelect;
    this.scopes = scopes;
    this.clickAddScope = clickAddScope;
    this.statusClass = statusClass;
    this.$scope = $rootScope.$new();
    this.mnCollectionsStatsPoller = mnStatisticsNewService.createStatsPoller(this.$scope);
  }

  ngOnDestroy() {
    this.mnOnDestroy.next();
    this.mnOnDestroy.complete();
    this.$scope.$destroy();
  }

  trackByFn(_, scope) {
    return this.bucketSelect.get('item').value.name + scope.uid + scope.name;
  }
}
