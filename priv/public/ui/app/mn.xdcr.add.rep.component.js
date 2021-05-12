/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {pipe, BehaviorSubject} from '/ui/web_modules/rxjs.js';
import {withLatestFrom, map, takeUntil, startWith, filter} from '/ui/web_modules/rxjs/operators.js';
import {UIRouter} from '/ui/web_modules/@uirouter/angular.js';
import {FormBuilder, Validators} from '/ui/web_modules/@angular/forms.js'

import {MnAlertsService, $rootScope} from '/ui/app/ajs.upgraded.providers.js';

import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnFormService} from "./mn.form.service.js";
import {MnXDCRService} from "./mn.xdcr.service.js";
import {MnPoolsService} from "./mn.pools.service.js";
import {MnAdminService} from "./mn.admin.service.js";
import {MnBucketsService} from "./mn.buckets.service.js";

export {MnXDCRAddRepComponent};

class MnXDCRAddRepComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "/ui/app/mn.xdcr.add.rep.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
    })
  ]}

  static get parameters() { return [
    MnFormService,
    MnPoolsService,
    MnXDCRService,
    MnBucketsService,
    MnAdminService,
    MnAlertsService,
    $rootScope,
    UIRouter,
    FormBuilder
  ]}

  constructor(mnFormService, mnPoolsService, mnXDCRService, mnBucketsService, mnAdminService,
              mnAlertsService, $rootScope, uiRouter, formBuilder) {
    super();

    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.compatVersion70 = mnAdminService.stream.compatVersion70;
    this.bucketsMembaseEphemeral = mnBucketsService.stream.bucketsMembaseEphemeral.pipe(map((buckets)=> buckets.map((bucket) => bucket.name)));
    this.getSettingsReplications = mnXDCRService.stream.getSettingsReplications;
    this.remoteClusters = mnXDCRService.stream.getRemoteClustersFiltered.pipe(map((clusters)=> clusters.map((cluster) => cluster.name)));
    this.postCreateReplication = mnXDCRService.stream.postCreateReplication;
    this.postSettingsReplicationsValidation =
      mnXDCRService.stream.postSettingsReplicationsValidation;

    this.form = mnFormService.create(this)
      .setFormGroup({fromBucket: ["", [Validators.required]],
                     toCluster: ["", [Validators.required]],
                     toBucket: ["", [Validators.required]],
                     priority: null,
                     collectionsExplicitMapping: false,
                     collectionsMigrationMode: false,
                     filterExpiration: false,
                     filterSkipRestream: "false",
                     filterDeletion: false,
                     filterBypassExpiry: false,
                     compressionType: null,
                     sourceNozzlePerNode: null,
                     targetNozzlePerNode: null,
                     checkpointInterval: null,
                     workerBatchSize: null,
                     docBatchSizeKb: null,
                     failureRestartInterval: null,
                     optimisticReplicationThreshold: null,
                     statsInterval: null,
                     networkUsageLimit: null,
                     logLevel: null});

    this.isSaveButtonDisabled =
      this.form.group.statusChanges
        .pipe(startWith(this.form.group.status),
              map(v => v === "INVALID"));

    this.form
      .setPackPipe(pipe(withLatestFrom(this.isEnterprise,
                                       mnAdminService.stream.compatVersion55,
                                       this.isSaveButtonDisabled),
                        filter(([_, isEnterprise, compatVersion, isDisabled]) => !isDisabled),
                        map(mnXDCRService.prepareReplicationSettigns.bind(this))))
      .setSourceShared(this.getSettingsReplications)
      .setPostRequest(this.postCreateReplication)
      .setValidation(this.postSettingsReplicationsValidation)
      .clearErrors()
      .showGlobalSpinner()
      .success(data => {
        $rootScope.$broadcast("reloadTasksPoller");
        uiRouter.stateService.go('app.admin.replications').then(() => {
          var hasWarnings = !!(data.warnings && data.warnings.length);
          mnAlertsService.formatAndSetAlerts(
            hasWarnings ? data.warnings : "Replication created successfully!",
            hasWarnings ? 'warning': "success",
            hasWarnings ? 0 : 2500);
        });
      });

    this.filterRegexpGroup = formBuilder.group({
      docId: "",
      filterExpression: ""
    });

    this.explicitMappingGroup = {};
    this.explicitMappingRules = new BehaviorSubject();
    this.explicitMappingMigrationRules = new BehaviorSubject();
    this.explicitMappingGroup.migrationMode = formBuilder.group({key: "", target: ""});
    let migrationMode = this.form.group.get("collectionsMigrationMode");
    this.isMigrationMode = migrationMode.valueChanges.pipe(startWith(migrationMode.value));
    let explicitMappingMode = this.form.group.get("collectionsExplicitMapping");
    this.isExplicitMappingMode = explicitMappingMode.valueChanges.pipe(startWith(explicitMappingMode.value));

    function resetMappingRules() {
      this.explicitMappingGroup.scopesControls = {};
      this.explicitMappingGroup.scopes = {};
      this.explicitMappingGroup.collections = {};
      this.explicitMappingGroup.collectionsControls = {};
      this.explicitMappingRules.next({});
      this.explicitMappingMigrationRules.next({});
    }
    resetMappingRules.bind(this)();

    this.form.group.get("fromBucket").valueChanges
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(resetMappingRules.bind(this));

  }
}
