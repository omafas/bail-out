/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {Subject} from 'rxjs';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {takeUntil, map, pluck, distinctUntilChanged,
        startWith, combineLatest,
        shareReplay, filter} from 'rxjs/operators';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnXDCRService} from './mn.xdcr.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnTasksService} from './mn.tasks.service.js';
import {MnHelperService} from './mn.helper.service.js';

import {MnXDCRAddRefComponent} from "./mn.xdcr.add.ref.component.js";

export { MnXDCRComponent };

class MnXDCRComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "app/mn.xdcr.html",
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnPermissions,
    MnXDCRService,
    MnPoolsService,
    MnTasksService,
    MnHelperService,
    NgbModal
  ]}

  constructor(mnPermissions, mnXDCRService, mnPoolsService, mnTasksService,
              mnHelperService, modalService) {
    super();

    var onAddReference = new Subject();
    onAddReference
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(() => modalService.open(MnXDCRAddRefComponent));

    var referenceSorter = mnHelperService.createSorter("name");

    this.tasksXDCR = mnTasksService.stream.tasksXDCR;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.permissions = mnPermissions.stream;
    this.getRemoteClustersFiltered = mnXDCRService.stream.getRemoteClustersFiltered;
    this.getRemoteClustersSorted = this.getRemoteClustersFiltered
      .pipe(referenceSorter.pipe);

    let remoteClusterRead = this.permissions
        .pipe(pluck("cluster", "xdcr", "remote_clusters", "read"),
              distinctUntilChanged());

    let tasksRead = this.permissions
        .pipe(pluck("cluster", "tasks", "read"),
              distinctUntilChanged());

    let hasPermissionsToWrite =
        this.permissions.pipe(map((p) => {
          return p.cluster.xdcr.settings.read &&
            p.cluster.xdcr.settings.write &&
            p.cluster.bucket['.'].xdcr.write;
        }), distinctUntilChanged());

    let hasReferences = this.getRemoteClustersFiltered
        .pipe(pluck("length"),
              map(Boolean),
              startWith(false),
              distinctUntilChanged(),
              shareReplay({refCount: true, bufferSize: 1}));

    let hasReplications = this.tasksXDCR
        .pipe(filter(v => v),
              pluck("length"),
              map(Boolean),
              startWith(false),
              distinctUntilChanged(),
              shareReplay({refCount: true, bufferSize: 1}));

    this.remoteClustersSpinner =
      this.getRemoteClustersSorted.pipe(combineLatest(this.isEnterprise));

    this.tasksSpinner =
      this.tasksXDCR.pipe(combineLatest(this.isEnterprise));

    this.hasReferencesAndHasPermissionsToWrite = hasReferences
      .pipe(combineLatest(hasPermissionsToWrite),
            map(([hasReferences, hasPermissions]) => hasReferences && hasPermissions),
            shareReplay({refCount: true, bufferSize: 1}));

    this.hasReferencesAndisNotEnterprise = this.isEnterprise
      .pipe(combineLatest(hasReferences),
            map(([isEnterprise, hasReferences]) => !isEnterprise && hasReferences));

    this.onAddReference = onAddReference;
    this.referenceSorter = referenceSorter;
    this.hasReferences = hasReferences;
    this.hasReplications = hasReplications;
    this.tasksRead = tasksRead;
    this.remoteClusterRead = remoteClusterRead;

    this.getChangesLeftTotal = mnXDCRService.stream.getChangesLeftTotal;

  }

  trackByFn(_, row) {
    return row.name;
  }

  tasksTrackByFn(_, row) {
    return row.id;
  }
}
