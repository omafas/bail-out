/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {combineLatest, Subject, NEVER} from 'rxjs';
import {pluck, map, shareReplay, takeUntil,
        switchMap, startWith} from 'rxjs/operators';
import {UIRouter} from '@uirouter/angular';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream, DetailsHashObserver} from './mn.core.js';
import {MnXDCRService} from './mn.xdcr.service.js';
import {MnXDCRErrorsComponent} from "./mn.xdcr.errors.component.js";
import template from "./mn.xdcr.item.html";

export {MnXDCRItemComponent};

class MnXDCRItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-item",
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
    MnXDCRService,
    NgbModal
  ]}

  constructor(mnPermissions, uiRouter, mnXDCRService, modalService) {
    super();

    var itemStream = this.mnOnChanges.pipe(pluck("item", "currentValue"));
    var humanStatus = itemStream.pipe(map(this.getStatus),
                                      shareReplay({refCount: true, bufferSize: 1}));
    var getStatusClass = v => v == 'replicating' ? 'dynamic_healthy' : 'dynamic_warmup';
    var getTargetBucket = v => v.target.split('buckets/')[1];

    var onShowErrorsReplication = new Subject();
    onShowErrorsReplication
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(item => {
        var ref = modalService.open(MnXDCRErrorsComponent);
        ref.componentInstance.errors = item.errors;
      });

    this.humanStatus = humanStatus;
    this.statusClass = humanStatus.pipe(map(getStatusClass));
    this.toBucket = itemStream.pipe(map(getTargetBucket));
    this.uiRouter = uiRouter;
    this.permissions = mnPermissions.stream;

    this.toCluster =
      combineLatest(
        itemStream,
        mnXDCRService.stream.getRemoteClustersByUUID)
      .pipe(map(this.getCluster.bind(this)));

    this.onShowErrorsReplication = onShowErrorsReplication;
  }

  ngOnInit() {
    var detailsHashObserver = new DetailsHashObserver(
      this.uiRouter, this, "xdcrDetails", this.item.id);
    var isDetailsOpened = this.permissions
        .pipe(switchMap((perm) => {
          return perm.cluster.bucket[this.item.source].xdcr.read ?
            detailsHashObserver.stream.isOpened : NEVER;
        }),
              startWith(false),
              shareReplay(1));

    var toggleClass =
        combineLatest(this.statusClass, isDetailsOpened);
    var sectionClass = toggleClass
        .pipe(map(([currentClass, isOpened]) => isOpened ? currentClass : ""));
    var tableClass = toggleClass
        .pipe(map(([currentClass, isOpened]) => isOpened ? ""  : currentClass));

    this.sectionClass = sectionClass;
    this.tableClass = tableClass;
    this.detailsHashObserver = detailsHashObserver;
    this.isDetailsOpened = isDetailsOpened;
  }

  getCluster(source) {
    if (!source[0]) {
      return;
    }
    var uuid = source[0].id.split("/")[0];
    var target = source[1][uuid][0];
    return  !target ? "unknown" : !target.deleted ? target.name : ("at " + target.hostname);
  }

  getStatus(row) {
    if (row.pauseRequested && row.status != 'paused') {
      return 'pausing';
    } else {
      switch (row.status) {
      case 'running': return 'replicating';
      case 'paused': return 'paused';
      default: return 'starting up';
      }
    }
  }
}
