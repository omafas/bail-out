/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {combineLatest, pipe, BehaviorSubject} from '/ui/web_modules/rxjs.js';
import {map, withLatestFrom, takeUntil, pluck, first, startWith} from '/ui/web_modules/rxjs/operators.js';
import {find, where, includes, flip} from "/ui/web_modules/ramda.js";
import {UIRouter} from "/ui/web_modules/@uirouter/angular.js";
import {FormBuilder} from '/ui/web_modules/@angular/forms.js';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnXDCRService, collectionDelimiter} from "./mn.xdcr.service.js";
import {MnFormService} from "./mn.form.service.js";
import {MnPoolsService} from "./mn.pools.service.js";
import {MnAdminService} from "./mn.admin.service.js";

export {MnXDCREditRepComponent};

class MnXDCREditRepComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "/ui/app/mn.xdcr.edit.rep.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ]
    })
  ]}

  static get parameters() { return [
    MnXDCRService,
    MnFormService,
    MnPoolsService,
    MnAdminService,
    UIRouter,
    FormBuilder
  ]}

  constructor(mnXDCRService, mnFormService, mnPoolsService, mnAdminService, uiRouter,
              formBuilder) {
    super();
    this.item = uiRouter.globals.params.item;
    this.uiRouter = uiRouter;
    this.isEditMode = true;
    this.formBuilder = formBuilder;
    this.mnFormService = mnFormService;
    this.mnXDCRService = mnXDCRService;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.compatVersion55 = mnAdminService.stream.compatVersion55;
    this.compatVersion70 = mnAdminService.stream.compatVersion70;

    this.prepareReplicationSettigns = mnXDCRService.prepareReplicationSettigns.bind(this);
    this.getSettingsReplications = mnXDCRService.stream.getSettingsReplications
    this.postSettingsReplicationsValidation =
      mnXDCRService.stream.postSettingsReplicationsValidation;
    this.postSettingsReplications =
      mnXDCRService.stream.postSettingsReplications;
    this.createGetSettingsReplicationsPipe =
      mnXDCRService.createGetSettingsReplicationsPipe.bind(mnXDCRService);
  }

  ngOnInit() {
    var thisReplicationSettings = this.createGetSettingsReplicationsPipe(this.item.id);
    this.replicationSettings =
      combineLatest(this.getSettingsReplications,
                    thisReplicationSettings)
      .pipe(map(function (source) {
        if (source[1].collectionsMigrationMode) {
          source[1].collectionsExplicitMapping = false;
        }
        return Object.assign({}, source[0], source[1]);
      }))

    this.form = this.mnFormService.create(this)
      .setFormGroup({type: null,
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
                     logLevel: null})
      .setPackPipe(pipe(
        withLatestFrom(this.isEnterprise, this.compatVersion55),
        map(this.prepareReplicationSettigns),
        map(data => [this.item.id, data])))
      .setSourceShared(this.replicationSettings)
      .setPostRequest(this.postSettingsReplications)
      .setValidation(this.postSettingsReplicationsValidation)
      .successMessage("Settings saved successfully!")
      .clearErrors()
      .showGlobalSpinner()
      .success(() => this.uiRouter.stateService.go('app.admin.replications'));

    this.filterRegexpGroup = this.formBuilder.group({
      docId: "",
      filterExpression: ""
    });

    thisReplicationSettings
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.unpackReplicationSettings.bind(this));

    this.toCluster = this.mnXDCRService.stream.getRemoteClusters
      .pipe(map(find(where({uuid: flip(includes)(this.item.target)}))),
            pluck("name"),
            first());
    this.toBucket = this.item.target.split('buckets/')[1];

    this.explicitMappingRules = new BehaviorSubject({});
    this.explicitMappingMigrationRules = new BehaviorSubject({});
  }

  unpackReplicationSettings(v) {
    let scopesFlags = {};
    let scopesFields = {};
    let scopesControls = {
      root: this.formBuilder.group({
        checkAll: this.formBuilder.control(false)
      })
    };
    let collections = {};
    let collectionsControls = {};

    Object.keys(v.colMappingRules).forEach(sourceRule => {
      let targetRule = v.colMappingRules[sourceRule];
      let sourcePair = sourceRule.split(collectionDelimiter);

      if (sourcePair.length == 2) {
        if (!collections[sourcePair[0]]) {
          collections[sourcePair[0]] = {
            flags: this.formBuilder.group({}),
            fields: this.formBuilder.group({})
          };
        }

        let collectionFlag = collections[sourcePair[0]].flags.get(sourcePair[1]);
        let collectionField = collections[sourcePair[0]].fields.get(sourcePair[1]);
        let fieldValue = targetRule ? targetRule.split(collectionDelimiter)[1] : sourcePair[1];

        if (collectionFlag) {
          collectionFlag.setValue(!!targetRule, {emitEvent: false});
          collectionField.setValue(fieldValue, {emitEvent: false});
        } else {
          collections[sourcePair[0]]
            .flags.addControl(sourcePair[1], this.formBuilder.control(!!targetRule));
          collections[sourcePair[0]]
            .fields.addControl(sourcePair[1], this.formBuilder.control(fieldValue));
        }

        if (!collectionsControls[sourcePair[0]]) {
          collectionsControls[sourcePair[0]] = this.formBuilder.group({
            checkAll: this.formBuilder.control(false)
          });
        }

        if (targetRule) {
          scopesFields[sourcePair[0]] = targetRule.split(collectionDelimiter)[0];
        } else {
          scopesFields[sourcePair[0]] = sourcePair[0];
        }
      } else {
        scopesFlags[sourcePair[0]] = !!targetRule;
        if (targetRule) {
          scopesFields[sourcePair[0]] = targetRule;
        } else {
          scopesFields[sourcePair[0]] = sourcePair[0];
        }
      }
    });

    this.explicitMappingGroup = {
      scopes: {
        root: {
          flags: this.formBuilder.group(scopesFlags),
          fields: this.formBuilder.group(scopesFields)
        }
      },
      scopesControls: scopesControls,
      collections: collections,
      collectionsControls: collectionsControls,
      migrationMode: this.formBuilder.group({key: "", target: ""})
    };

    this.explicitMappingRules.next(v.collectionsMigrationMode ? {} : v.colMappingRules);
    this.explicitMappingMigrationRules.next(v.collectionsMigrationMode ? v.colMappingRules : {});
    let migrationMode = this.form.group.get("collectionsMigrationMode");
    this.isMigrationMode = migrationMode.valueChanges.pipe(startWith(v.collectionsMigrationMode));
    let explicitMappingMode = this.form.group.get("collectionsExplicitMapping");
    this.isExplicitMappingMode = explicitMappingMode.valueChanges.pipe(startWith(v.collectionsExplicitMapping));
  }
}
