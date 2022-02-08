/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, Pipe, ChangeDetectionStrategy} from '@angular/core'
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {Subject, BehaviorSubject, pipe} from 'rxjs';
import {pluck, map, shareReplay, takeUntil, filter,
        combineLatest, merge, mapTo, pairwise, startWith} from 'rxjs/operators';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnXDCRService} from './mn.xdcr.service.js';
import {MnFormService} from "./mn.form.service.js";
import {MnHelperService} from "./mn.helper.service.js";
import {MnXDCRDeleteRepComponent} from "./mn.xdcr.delete.rep.component.js";
import template from "./mn.xdcr.item.details.html";

export {MnXDCRItemDetailsComponent, MnReplicationStatus};

class MnXDCRItemDetailsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-item-details",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ]
    })
  ]}

  static get parameters() { return [
    MnPermissions,
    MnXDCRService,
    MnFormService,
    NgbModal,
    MnHelperService
  ]}

  constructor(mnPermissions, mnXDCRService, mnFormService, modalService, mnHelperService) {
    super();

    this.mnFormService = mnFormService;
    this.mnXDCRService = mnXDCRService;

    var onDeleteReplication = new Subject();
    onDeleteReplication
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(item => {
        var ref = modalService.open(MnXDCRDeleteRepComponent);
        ref.componentInstance.item = item;
      });

    this.permissions = mnPermissions.stream;
    this.onDeleteReplication = onDeleteReplication;

    this.createGetSettingsReplicationsPipe =
      mnXDCRService.createGetSettingsReplicationsPipe.bind(mnXDCRService);
    this.explicitMappingRules = new BehaviorSubject({});
    this.explicitMappingMigrationRules = new BehaviorSubject({});
    this.isMigrationMode = new BehaviorSubject();
    this.isExplicitMappingMode = new BehaviorSubject();
    this.toggler = mnHelperService.createToggle();
  }

  ngOnInit() {
    var form = this.mnFormService.create(this);
    var itemStream = this.mnOnChanges.pipe(pluck("item", "currentValue"));
    var status = itemStream.pipe(map(this.getStatus),
                                 shareReplay({refCount: true, bufferSize: 1}));
    form
      .setFormGroup({})
      .setPackPipe(pipe(startWith(null),
                        pairwise(),
                        filter(([prevItem, item]) => !prevItem || prevItem.status !== item.status),
                        map(([, item]) => [item.id, {
                          pauseRequested: item.status !== "paused"
                        }])))
      .setPostRequest(this.mnXDCRService.stream.postPausePlayReplication)
      .hasNoHandler();

    this.form = form;
    this.status = status.pipe(merge(form.submit.pipe(pluck("status"))));
    this.statusClass = status.pipe(merge(form.submit.pipe(mapTo("spinner"))),
                                   map(v => "fa-" + v));

    this.replicationSettings = this.createGetSettingsReplicationsPipe(this.item.id);
    this.replicationSettings
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.unpackReplicationMappings.bind(this));

    this.areThereMappingRules = this.explicitMappingRules
      .pipe(combineLatest(this.explicitMappingMigrationRules),
            map(([mappingRules, mappingMigrationRules]) => {
              return Object.keys(mappingRules).length || Object.keys(mappingMigrationRules).length;
            }));
  }

  unpackReplicationMappings(v) {
    this.explicitMappingRules.next(v.collectionsExplicitMapping ? v.colMappingRules : {});
    this.explicitMappingMigrationRules.next(v.collectionsMigrationMode ? v.colMappingRules : {});
    this.isMigrationMode.next(v.collectionsMigrationMode);
    this.isExplicitMappingMode.next(v.collectionsExplicitMapping);
  }

  getStatus(row) {
    switch (row.status) {
    case 'running': return 'pause';
    case 'paused': return 'play';
    default: return 'spinner';
    }
  }
}

class MnReplicationStatus {
  static get annotations() { return [
    new Pipe({name: "mnReplicationStatus"})
  ]}

  transform(status) {
    switch (status) {
    case "running": return "Pausing";
    case "pause": return "Pause";
    case "paused": return "Running";
    case "play": return "Run";
    }
  }
}
