/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';
import {FormBuilder} from '@angular/forms';
import {UIRouter} from '@uirouter/angular';
import {BehaviorSubject, combineLatest, pipe} from 'rxjs';
import {map, merge, pluck, filter, shareReplay, startWith,
  takeUntil, distinctUntilChanged, withLatestFrom, combineLatest as combineLatestOp} from 'rxjs/operators';

import {MnAlerts, MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnFormService} from './mn.form.service.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnBucketsService} from './mn.buckets.service.js';
import {MnUserRolesService} from './mn.user.roles.service.js';
import {MnPermissionsService} from './mn.permissions.service.js';
import {MnSettingsAutoCompactionService} from './mn.settings.auto.compaction.service.js';

export {MnBucketDialogComponent};

class MnBucketDialogComponent extends MnLifeCycleHooksToStream {

  static get annotations() { return [
    new Component({
      selector: 'mn-buckets-dialog',
      templateUrl: 'app/mn.bucket.dialog.html',
      inputs: [
        'bucket'
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() {
    return [
      NgbActiveModal,
      MnPoolsService,
      MnAdminService,
      MnFormService,
      FormBuilder,
      MnHelperService,
      MnBucketsService,
      MnAlerts,
      MnPermissions,
      MnPermissionsService,
      MnUserRolesService,
      MnSettingsAutoCompactionService,
      UIRouter
  ]}

  constructor(activeModal, mnPoolsService, mnAdminService, mnFormService, formBuilder,
      mnHelperService, mnBucketsService, mnAlerts, mnPermissions, mnPermissionsService,
      mnUserRolesService, mnSettingsAutoCompactionService, uiRouter) {
    super();

    this.activeModal = activeModal;
    this.mnAlerts = mnAlerts;
    this.mnPoolsService = mnPoolsService;
    this.mnAdminService = mnAdminService;
    this.mnBucketsService = mnBucketsService;
    this.mnFormService = mnFormService;
    this.formBuilder = formBuilder;
    this.mnHelperService = mnHelperService;
    this.mnSettingsAutoCompactionService = mnSettingsAutoCompactionService;
    this.focusFieldSubject = new BehaviorSubject(true);
    this.isDeveloperPreview = mnPoolsService.stream.isDeveloperPreview;
    this.majorMinorVersion = mnAdminService.stream.majorMinorVersion;
    this.permissions = mnPermissions.stream;
    this.mnPermissionsService = mnPermissionsService;
    this.mnUserRolesService = mnUserRolesService;
    this.uiRouter = uiRouter;
    this.isEnterprise = this.mnPoolsService.stream.isEnterprise;
    this.compatVersion55 = this.mnAdminService.stream.compatVersion55;
  }

  ngOnInit() {
    let postRequest = this.mnBucketsService.createPostBucketPipe(this.bucket && this.bucket.uuid);
    let postValidation = this.mnBucketsService.createPostValidationPipe(this.bucket && this.bucket.uuid);

    let formData = this.bucket ?
      this.mnBucketsService.createBucketFormData(this.bucket) :
      this.mnBucketsService.createInitialFormData(this.storageTotals);

    this.form = this.mnFormService.create(this)
      .setFormGroup(this.formBuilder.group({
        name: null,
        ramQuotaMB: null,
        bucketType: null,
        replicaNumberEnabled: null,
        replicaNumber: null,
        replicaIndex: null,
        evictionPolicy: null,
        evictionPolicyEphemeral: null,
        maxTTLEnabled: null,
        maxTTL: null,
        compressionMode: null,
        conflictResolutionType: null,
        flushEnabled: null,
        threadsNumber: null,
        storageBackend: null,
        durabilityMinLevel: null,
        purgeInterval: null,
        autoCompactionDefined: null,
        autoCompactionSettings: this.formBuilder.group({
          indexCompactionMode: null,
          allowedTimePeriod: this.formBuilder.group({
            fromHour: null,
            toHour: null,
            fromMinute: null,
            toMinute: null,
            abortOutside: false}),
          databaseFragmentationThreshold: this.formBuilder.group({
            percentageFlag: null,
            sizeFlag: null,
            percentage: null,
            size: null}),
          magmaFragmentationPercentage: null,
          viewFragmentationThreshold: this.formBuilder.group({
            percentageFlag: null,
            sizeFlag: null,
            percentage: null,
            size: null}),
          parallelDBAndViewCompaction: null,
          purgeInterval: null,
          timePeriodFlag: null})}))
      .setSource(formData)
      .setPackPipe(pipe(withLatestFrom(this.compatVersion55, this.isEnterprise),
                   map(this.packData.bind(this))))
      .setPostRequest(postRequest)
      .setValidation(postValidation, undefined, undefined, true)
      .successMessage('Bucket settings saved successfully!')
      .success(() => {
        this.activeModal.dismiss();
        this.mnBucketsService.stream.updateBucketsPoller.next();
      });

    this.httpError = postRequest.error
      .pipe(merge(postValidation.error));

    let ramSummary = postValidation.success
      .pipe(merge(postValidation.error),
            pluck("summaries", "ramSummary"),
            filter(v => !!v));

    this.bucketRam = ramSummary
      .pipe(map(v => this.mnBucketsService.getRamConfig(v)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.bucketTotalRam = this.bucketRam
      .pipe(map(this.getBucketTotalRam.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.showAdvancedSettings = this.mnHelperService.createToggle();

    this.bucketType = this.form.group.get('bucketType').valueChanges
      .pipe(startWith(this.form.group.get('bucketType').value),
            shareReplay({bufferSize: 1}));

    this.replicaNumberEnabled = this.form.group.get('replicaNumberEnabled').valueChanges
      .pipe(startWith(this.form.group.get('replicaNumberEnabled').value));

    this.showReplicaNumberError =
      combineLatest(this.replicaNumberEnabled,
                    this.httpError
                      .pipe(filter(v => !!v),
                            pluck('errors', 'replicaNumber')))
      .pipe(map(this.isReplicaNumberErrorVisible.bind(this)));

    this.replicaNumberEnabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.onReplicaNumberEnabled.bind(this));

    this.showMaxTTL =
      combineLatest(this.isEnterprise,
                    this.compatVersion55,
                    this.bucketType)
      .pipe(map(this.isMaxTTLVisible.bind(this)));

    this.showMaxTTLWarning =
      combineLatest(this.form.group.get('maxTTLEnabled').valueChanges,
                    this.form.group.get('maxTTL').valueChanges)
      .pipe(map(this.isMaxTTlWarningVisible.bind(this)));

    this.isMaxTTLPlural = this.form.group.get('maxTTL').valueChanges
      .pipe(startWith(this.form.group.get('maxTTL').valueChanges),
            map(ttl => ttl == 1 ? '' : 's'));

    this.maybeDisableWhenChecked(['maxTTLEnabled', 'maxTTL']);

    this.showCompressionMode = this.showMaxTTL;

    this.showConflictResolution =
      combineLatest(this.isEnterprise,
                    this.bucketType)
      .pipe(map(this.isConflictResolutionVisible.bind(this)));

    this.durabilityMinLevelOptions = this.bucketType
      .pipe(map(this.setDurabilityMinLevelOptions.bind(this)));

    this.showStorageBackend =
      combineLatest(this.isEnterprise,
                    this.bucketType)
      .pipe(map(this.isStorageBackendVisible.bind(this)));

    this.storageBackend = this.form.group.get('storageBackend').valueChanges
      .pipe(startWith(this.form.group.get('storageBackend').value),
            shareReplay({bufferSize: 1}));

    this.autoCompactionDefined = this.form.group.get('autoCompactionDefined').valueChanges
      .pipe(startWith(this.form.group.get('autoCompactionDefined').value),
            shareReplay({bufferSize: 1}));

    this.autoCompactionMode =
      combineLatest(this.autoCompactionDefined,
                    this.storageBackend)
      .pipe(map(this.getAutoCompactionMode.bind(this)));

    if (!this.bucket) {
      this.storageBackend
        .pipe(takeUntil(this.mnOnDestroy))
        .subscribe(storageBackend => {
          let eviction = (storageBackend === 'magma') ? 'fullEviction' : 'valueOnly';
          this.form.group.get('evictionPolicy').patchValue(eviction);
        });
    }

    this.permissions
      .pipe(pluck("cluster", "settings", "write"),
            distinctUntilChanged(),
            takeUntil(this.mnOnDestroy))
      .subscribe(v => this.maybeDisableField('purgeInterval', v));

    if (this.bucket) {
      ['name', 'bucketType', 'conflictResolutionType',
        'evictionPolicyEphemeral', 'storageBackend']
        .forEach(field =>
                 this.maybeDisableField(field, false));

      (['threadsNumber','evictionPolicy']).forEach(this.threadsEvictionWarning.bind(this));
    }

    let usersByPermission =
      this.mnPermissionsService.generateBucketPermissions({
        name: this.bucket ? this.bucket.name : '.'
      }).map(permission =>
        this.mnUserRolesService.getUsers({
          permission: permission,
          pageSize: 4
        }));

    this.users = this.mnAdminService.stream.whoami
      .pipe(combineLatestOp(usersByPermission),
            map(this.getAuthorizedUsers.bind(this)),
            shareReplay(1));

    this.showAuthorizedUsers = this.permissions
      .pipe(map(this.isAuthorizedUsersVisible.bind(this)));

    this.showUsersLink = this.users
      .pipe(map(users => users.length > 3));
  }

  getBucketTotalRam(ramSummary) {
    return (ramSummary.items[2].name === 'overcommitted') ?
      ramSummary.topLeft.value : ramSummary.topRight.value;
  }

  maybeDisableField(field, enable) {
    this.form.group.get(field)[enable ? 'enable': 'disable']();
  }

  maybeDisableWhenChecked([flag, field]) {
    this.form.group.get(flag).valueChanges
      .pipe(startWith(this.form.group.get(flag).value),
            takeUntil(this.mnOnDestroy))
      .subscribe(v => this.maybeDisableField(field, v));
  }

  onReplicaNumberEnabled(enabled) {
    let replicaNumber = this.form.group.get('replicaNumber');
    if (enabled) {
      replicaNumber.setValue(replicaNumber.value || 1);
    } else {
      this.form.group.get('replicaNumber').setValue(0);
      this.form.group.get('replicaIndex').setValue(0);
    }

    this.maybeDisableField('replicaIndex', enabled && !this.bucket);
  }

  isMaxTTLVisible([isEnterprise, compatVersion55, bucketType]) {
    return isEnterprise && compatVersion55 && ['membase', 'ephemeral'].includes(bucketType);
  }

  isMaxTTlWarningVisible([maxTTlEnabled, maxTTL]) {
    return maxTTlEnabled && maxTTL > 0;
  }

  isConflictResolutionVisible([isEnterprise, bucketType]) {
    return isEnterprise && ['membase', 'ephemeral'].includes(bucketType);
  }

  threadsEvictionWarning(fieldName) {
    let initValue = this.bucket[fieldName];

    this[fieldName + "Warning"] = this.form.group.get(fieldName).valueChanges
      .pipe(startWith(this.form.group.get(fieldName).value),
            map((value) =>
              (value != initValue) ?
                ('Changing ' + (fieldName === 'evictionPolicy' ?
                  'eviction policy' :
                  'bucket priority')  +
                  ' will restart the bucket. This will lead to closing all' +
                  ' open connections and some downtime') : ''));
  }

  setDurabilityMinLevelOptions(bucketType) {
    let durabilityMinLevelOptionsComplete = ['none', 'majority', 'majorityAndPersistActive', 'persistToMajority'];
    let durabilityMinLevelOptionsBasic = ['none', 'majority'];

    switch(bucketType) {
      case 'membase':
        return durabilityMinLevelOptionsComplete;
      case 'memcached':
        return durabilityMinLevelOptionsBasic;
      case 'ephemeral':
        return durabilityMinLevelOptionsBasic;
    }
  }

  isStorageBackendVisible([isEnterprise, bucketType]) {
    return isEnterprise && bucketType === 'membase';
  }

  getAutoCompactionMode([autoCompactionDefined, storageBackend]) {
    return autoCompactionDefined && storageBackend;
  }

  isReplicaNumberErrorVisible([enabled, error]) {
    return enabled && error;
  }

  isAuthorizedUsersVisible(permissions) {
    return permissions.cluster.admin.security.read;
  }

  getAuthorizedUsers([whoAmI, ...usersByPermission]) {
    return this.mnUserRolesService.getUniqueUsers(usersByPermission, whoAmI);
  }

  packData([, compat55, isEnterprise]) {
    let formData = this.form.group.getRawValue();
    let saveData = {};

    let copyProperty = (property) => {
      if (formData[property] !== undefined && formData[property] !== null) {
        saveData[property] = formData[property];
      }
    };
    let copyProperties = (properties) => {
      properties.forEach(copyProperty);
    };

    let isMembase = formData.bucketType === 'membase';
    let isEphemeral = formData.bucketType === 'ephemeral';

    copyProperty('name');
    if (!this.bucket) {
      copyProperty('bucketType');
    }

    if (isEnterprise && isMembase) {
      copyProperty('storageBackend');
    }
    if (isMembase) {
      copyProperties(['autoCompactionDefined', 'evictionPolicy']);
    }

    if (isEphemeral) {
      copyProperties(['purgeInterval', 'durabilityMinLevel']);
      saveData['evictionPolicy'] = formData['evictionPolicyEphemeral'];
    }

    if (isMembase || isEphemeral) {
      copyProperties(['threadsNumber', 'replicaNumber', 'durabilityMinLevel']);
      if (isEnterprise && compat55) {
        copyProperty('compressionMode');
        if (!formData.maxTTLEnabled) {
          saveData.maxTTL = 0;
        } else {
          copyProperty('maxTTL');
        }
      }
      if (!this.bucket) {
        if (!isEphemeral) {
          copyProperty('replicaIndex', true);
          saveData.replicaIndex = saveData.replicaIndex ? 1 : 0;
        }

        if (isEnterprise) {
          copyProperty('conflictResolutionType');
        }
      }

      if (formData.autoCompactionDefined) {
        let autoCompactionData = this.mnSettingsAutoCompactionService.getAutoCompactionData(this.form.group.get('autoCompactionSettings'));
        switch(formData.storageBackend) {
          case 'magma':
            saveData.magmaFragmentationPercentage = autoCompactionData.magmaFragmentationPercentage;
            break;
          case 'couchstore':
            saveData = Object.assign(saveData, autoCompactionData);
            delete saveData.magmaFragmentationPercentage;
            break;
        }
      }
    }

    if (this.bucket && this.bucket.isWizard) {
      copyProperty("otherBucketsRamQuotaMB");
    }

    copyProperties(['ramQuotaMB', 'flushEnabled']);
    saveData.flushEnabled = saveData.flushEnabled ? 1 : 0;

    return saveData;
  }
}
