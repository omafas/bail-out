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
import {BehaviorSubject, Subject} from "/ui/web_modules/rxjs.js";
import {MnPermissions, $rootScope} from '/ui/app/ajs.upgraded.providers.js'

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnCollectionsService} from './mn.collections.service.js';
import {MnCollectionsDeleteItemComponent} from './mn.collections.delete.item.component.js';

export {MnCollectionsItemComponent};

class MnCollectionsItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-collections-item",
      templateUrl: "app/mn.collections.item.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "collection",
        "scopeName",
        "bucketName",
        "mnCollectionsStatsPoller"
      ]
    })
  ]}

  static get parameters() { return [
    MnCollectionsService,
    MnPermissions,
    NgbModal,
    $rootScope
  ]}

  constructor(mnCollectionsService, mnPermissions, modalService,  $rootScope) {
    super();

    var clickDeleteCollection = new Subject();

    clickDeleteCollection
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(() => {
        var ref = modalService.open(MnCollectionsDeleteItemComponent);
        ref.componentInstance.scopeName = this.scopeName;
        ref.componentInstance.bucketName = this.bucketName;
        ref.componentInstance.collectionName = this.collection.name;
      });

    this.clickDeleteCollection = clickDeleteCollection;
    this.permissions = mnPermissions.stream;
    this.mnPermissions = mnPermissions;
    this.mnCollectionsService = mnCollectionsService;
    this.$scope = $rootScope.$new();
    this.stats = new BehaviorSubject({});
  }

  ngOnInit() {
    this.interestingPermissions =
      this.mnPermissions.getPerCollectionPermissions(this.bucketName,
                                                     this.scopeName,
                                                     this.collection.name);
    this.interestingPermissions.forEach(this.mnPermissions.set);
    this.mnPermissions.throttledCheck();

    this.mnCollectionsStatsPoller.subscribeUIStatsPoller({
      bucket: this.bucketName,
      scope: this.scopeName,
      collection: this.collection.name,
      node: "all",
      zoom: 3000,
      step: 1,
      stats: ["@kv-.kv_collection_item_count",
              "@kv-.kv_collection_mem_used_bytes",
              "@kv-.kv_collection_data_size_bytes",
              "@kv-.kv_collection_ops"]
    }, this.$scope);

    this.$scope.$watch("mnUIStats", stats => this.stats.next(stats ? stats.stats : {}));

    this.interestingStats =
      this.stats.pipe(this.mnCollectionsService.extractInterestingStatsPipe);
  }

  ngOnDestroy() {
    this.$scope.$destroy();
    this.interestingPermissions.forEach(this.mnPermissions.remove);
  }
}
