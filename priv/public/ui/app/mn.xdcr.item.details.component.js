/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, Pipe, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js'
import {NgbModal} from "/ui/web_modules/@ng-bootstrap/ng-bootstrap.js"
import {Subject, BehaviorSubject, pipe} from "/ui/web_modules/rxjs.js";
import {pluck, map, shareReplay, takeUntil,
        withLatestFrom, filter, combineLatest} from '/ui/web_modules/rxjs/operators.js';
import {MnPermissions, $rootScope} from '/ui/app/ajs.upgraded.providers.js';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnXDCRService} from './mn.xdcr.service.js';
import {MnFormService} from "./mn.form.service.js";
import {MnHelperService} from "./mn.helper.service.js";
import {MnXDCRDeleteRepComponent} from "./mn.xdcr.delete.rep.component.js";

export {MnXDCRItemDetailsComponent, MnReplicationStatus};

class MnXDCRItemDetailsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-item-details",
      templateUrl: "app/mn.xdcr.item.details.html",
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
    $rootScope,
    MnHelperService
  ]}

  constructor(mnPermissions, mnXDCRService, mnFormService, modalService, $rootScope, mnHelperService) {
    super();

    this.mnFormService = mnFormService;
    this.mnXDCRService = mnXDCRService;
    this.$rootScope = $rootScope;

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
      .setPackPipe(pipe(withLatestFrom(status),
                        filter(([_, status]) => status !== "spinner"),
                        map(([item, _]) => [item.id, {
                          pauseRequested: item.status !== "paused"
                        }])))
      .setPostRequest(this.mnXDCRService.stream.postPausePlayReplication)
      .success(() => this.$rootScope.$broadcast("reloadTasksPoller"));

    this.form = form;
    this.status = status;
    this.statusClass = status.pipe(map(v => "fa-" + v));

    this.replicationSettings = this.createGetSettingsReplicationsPipe(this.item.id);
    this.replicationSettings
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.unpackReplicationMappings.bind(this));
    this.areThereMappingRules = this.explicitMappingRules.pipe(
      combineLatest(this.explicitMappingMigrationRules),
      map(([mappingRules, mappingMigrationRules]) => {
        return Object.keys(mappingRules).length || Object.keys(mappingMigrationRules).length;
      })
    );
  }

  unpackReplicationMappings(v) {
    this.explicitMappingRules.next(v.collectionsExplicitMapping ? v.colMappingRules : {});
    this.explicitMappingMigrationRules.next(v.collectionsMigrationMode ? v.colMappingRules : {});
    this.isMigrationMode.next(v.collectionsMigrationMode);
    this.isExplicitMappingMode.next(v.collectionsExplicitMapping);
  }

  getStatus(row) {
    if (row.pauseRequested && row.status != 'paused') {
      return 'spinner';
    } else {
      switch (row.status) {
      case 'running': return 'pause';
      case 'paused': return 'play';
      default: return 'spinner';
      }
    }
  }
}

class MnReplicationStatus {
  static get annotations() { return [
    new Pipe({name: "mnReplicationStatus"})
  ]}

  transform(status) {
    switch (status) {
    case "spinner": return "Pausing";
    case "pause": return "Pause";
    case "play": return "Run";
    }
  }
}
