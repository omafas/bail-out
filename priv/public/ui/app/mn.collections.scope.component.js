/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js'
import {NgbModal} from "/ui/web_modules/@ng-bootstrap/ng-bootstrap.js"
import {takeUntil} from '/ui/web_modules/rxjs/operators.js';
import {Subject, BehaviorSubject} from "/ui/web_modules/rxjs.js";
import {UIRouter} from "/ui/web_modules/@uirouter/angular.js";
import {MnPermissions, $rootScope} from '/ui/app/ajs.upgraded.providers.js';

import {MnLifeCycleHooksToStream, DetailsHashObserver} from './mn.core.js';
import {MnCollectionsService} from './mn.collections.service.js';
import {MnCollectionsDeleteScopeComponent} from './mn.collections.delete.scope.component.js';
import {MnCollectionsAddItemComponent} from './mn.collections.add.item.component.js';

export {MnCollectionsScopeComponent};

class MnCollectionsScopeComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-collections-scope",
      templateUrl: "app/mn.collections.scope.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "scope",
        "mnCollectionsStatsPoller",
        "bucketName",
        "statusClass"
      ]
    })
  ]}

  static get parameters() { return [
    MnCollectionsService,
    MnPermissions,
    NgbModal,
    UIRouter,
    $rootScope
  ]}

  constructor(mnCollectionsService, mnPermissions, modalService, uiRouter, $rootScope) {
    super();

    var clickDeleteScope = new Subject();
    var clickAddCollection = new Subject();

    clickDeleteScope
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(() => {
        var ref = modalService.open(MnCollectionsDeleteScopeComponent);
        ref.componentInstance.scopeName = this.scope.name;
        ref.componentInstance.bucketName = this.bucketName;
      });

    clickAddCollection
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(() => {
        var ref = modalService.open(MnCollectionsAddItemComponent);
        ref.componentInstance.scopeName = this.scope.name;
        ref.componentInstance.bucketName = this.bucketName;
      });

    this.uiRouter = uiRouter;
    this.clickDeleteScope = clickDeleteScope;
    this.clickAddCollection = clickAddCollection;
    this.permissions = mnPermissions.stream;
    this.mnPermissions = mnPermissions;
    this.mnCollectionsService = mnCollectionsService;
    this.$scope = $rootScope.$new();
    this.stats = new BehaviorSubject({});
  }

  ngOnInit() {
    var detailsHashObserver =
        new DetailsHashObserver(this.uiRouter, this, "scopeDetails", this.scope.name);
    this.detailsHashObserver = detailsHashObserver;
    this.interestingPermissions = this.mnPermissions.getPerScopePermissions(this.bucketName,
                                                                            this.scope.name);
    this.interestingPermissions.forEach(this.mnPermissions.set);
    this.mnPermissions.throttledCheck();

    this.mnCollectionsStatsPoller.subscribeUIStatsPoller({
      bucket: this.bucketName,
      scope: this.scope.name,
      node: "all",
      zoom: 3000,
      applyFunctions: ["sum"],
      step: 1,
      stats: ["@kv-.kv_collection_item_count",
              "@kv-.kv_collection_mem_used_bytes",
              "@kv-.kv_collection_data_size_bytes",
              "@kv-.kv_collection_ops_sum"]
    }, this.$scope);

    this.$scope.$watch("mnUIStats", stats => this.stats.next(stats ? stats.stats : {}));

    this.interestingStats =
      this.stats.pipe(this.mnCollectionsService.extractInterestingStatsPipe);

  }

  ngOnDestroy() {
    this.$scope.$destroy();
    this.mnOnDestroy.next();
    this.mnOnDestroy.complete();
    this.interestingPermissions.forEach(this.mnPermissions.remove);
  }
}
