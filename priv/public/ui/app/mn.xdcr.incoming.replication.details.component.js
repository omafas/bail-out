/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core'
import {map, pluck, shareReplay, startWith, switchMap, takeUntil} from 'rxjs/operators';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {DetailsHashObserver, MnLifeCycleHooksToStream} from './mn.core.js';
import template from "./mn.xdcr.incoming.replication.details.html";
import {UIRouter} from '@uirouter/angular';
import {combineLatest, NEVER, Subject} from "rxjs";
import {MnXDCRIncomingReplicationSettingsComponent} from "./mn.xdcr.incoming.replication.settings.component.js";
import {MnHelperService} from "./mn.helper.service.js";

export {MnXDCRIncomingReplicationDetailsComponent};

class MnXDCRIncomingReplicationDetailsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-incoming-replication-details",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ]
    })
  ]}

  static get parameters() { return [
    MnPermissions,
    UIRouter,
    NgbModal,
    MnHelperService
  ]}

  constructor(mnPermissions, uiRouter, modalService, mnHelperService) {
    super();

    this.permissions = mnPermissions.stream;
    this.uiRouter = uiRouter;

    let itemStream = this.mnOnChanges.pipe(pluck("item", "currentValue"));
    this.hasActiveFilters = itemStream.pipe(map(this.activeFilters.bind(this)));
    this.getFilterExpression = itemStream.pipe(map(this.filterExpression));
    this.getFilterExpiration = itemStream.pipe(map(this.filterExpiration));
    this.getFilterDeletion = itemStream.pipe(map(this.filterDeletion));
    this.getFilterBypassExpiry = itemStream.pipe(map(this.filterBypassExpiry));
    this.getFilterBinary = itemStream.pipe(map(this.filterBinary));
    this.isActiveReplication = itemStream.pipe(map(item => item.replicationSettings.active));
    this.status = itemStream.pipe(map(item => item.replicationSettings.active ? "replicating" : "paused"));
    this.sourceBucketName = itemStream.pipe(map(item => item.sourceBucketName));
    this.targetBucketName = itemStream.pipe(map(item => item.targetBucketName));
    this.targetNozzlePerNode = itemStream.pipe(map(item => item.replicationSettings.target_nozzle_per_node));

    let onShowAllSettings = new Subject();
    onShowAllSettings
    .pipe(takeUntil(this.mnOnDestroy))
    .subscribe(() => {
      let ref = modalService.open(MnXDCRIncomingReplicationSettingsComponent);
      ref.componentInstance.settings = itemStream.pipe(map(item => item.replicationSettings));
    });
    this.onShowAllSettings = onShowAllSettings;

    this.itemStream = itemStream;
    this.toggler = mnHelperService.createToggle();
  }

  ngOnInit() {
    let detailsHashObserver = new DetailsHashObserver(
      this.uiRouter, this, "xdcrIncomingReplicationDetails", this.item.id);

    let isDetailsOpened = this.permissions
      .pipe(switchMap((perm) => {
        return perm.cluster.xdcr.settings.read ?
          detailsHashObserver.stream.isOpened : NEVER;
      }),
      startWith(false),
      shareReplay(1));
    let getStatusClass = item => item.replicationSettings.active ? 'dynamic_healthy' : 'dynamic_warmup';
    this.statusClass = this.itemStream.pipe(map(getStatusClass));
    let toggleClass =
      combineLatest(this.statusClass, isDetailsOpened);
    let sectionClass = toggleClass
      .pipe(map(([currentClass, isOpened]) => isOpened ? currentClass : ""));
    let tableClass = toggleClass
      .pipe(map(([currentClass, isOpened]) => isOpened ? ""  : currentClass));

    this.sectionClass = sectionClass;
    this.tableClass = tableClass;
    this.detailsHashObserver = detailsHashObserver;
    this.isDetailsOpened = isDetailsOpened;

    this.areThereMappingRules = this.itemStream
      .pipe(map((item) => Object.keys(item.replicationSettings.values.colMappingRules).length));
    this.mappingRules = this.itemStream
      .pipe(map(item => Object.entries(item.replicationSettings.values.colMappingRules)));
  }

  filterExpression(item) {
    return item.replicationSettings.filter_exp;
  }

  filterExpiration(item) {
    return (item.replicationSettings.values.filter_exp_del & (1 << 2)) > 0;
  }

  filterDeletion(item) {
    return (item.replicationSettings.values.filter_exp_del & (1 << 1)) > 0;
  }

  filterBypassExpiry(item) {
    return (item.replicationSettings.values.filter_exp_del & (1 << 0)) > 0;
  }

  filterBinary(item) {
    return (item.replicationSettings.values.filter_exp_del & (1 << 4)) > 0;
  }

  activeFilters(item) {
    return !!this.filterExpression(item) || this.filterExpiration(item) || this.filterDeletion(item) || this.filterBypassExpiry(item) || this.filterBinary(item);
  }
}
