/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {HttpErrorResponse } from '@angular/common/http';

import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from './mn.form.service.js';
import template from "./mn.security.secrets.add.dialog.html";
import {FormBuilder} from '@angular/forms';
import {UIRouter} from '@uirouter/angular';
import {BehaviorSubject, merge} from 'rxjs';
import {map, startWith, takeUntil, first} from 'rxjs/operators';
import {MnSecuritySecretsService} from './mn.security.secrets.service.js';
import {MnTimezoneDetailsService} from './mn.timezone.details.service.js';
import {MnHelperService} from "./mn.helper.service.js";

import {MnPermissions} from './ajs.upgraded.providers.js';

export {MnSecuritySecretsAddDialogComponent};

class MnSecuritySecretsAddDialogComponent extends MnLifeCycleHooksToStream {

  static get annotations() { return [
    new Component({
      template,
      inputs: [
        'item',
        'secrets'
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnSecuritySecretsService,
    NgbActiveModal,
    MnFormService,
    UIRouter,
    FormBuilder,
    MnPermissions,
    MnHelperService,
    MnTimezoneDetailsService
  ]}

  constructor(mnSecuritySecretsService, activeModal, mnFormService, uiRouter, formBuilder, mnPermissions, mnHelperService, mnTimezoneDetailsService) {
    super();

    this.formBuilder = formBuilder;
    this.activeModal = activeModal;
    this.permissions = mnPermissions.stream;
    this.mnSecuritySecretsService = mnSecuritySecretsService;
    this.mnTimezoneDetailsService = mnTimezoneDetailsService;
    this.mnFormService = mnFormService;
    this.uiRouter = uiRouter;
    this.toggler = mnHelperService.createToggle();
  }

  ngOnInit() {
    this.permissions
      .pipe(map(p => p.bucketNames['.views!read']),
            first())
      .subscribe(this.doInit.bind(this));
  }

  doInit(buckets) {
    this.bucketNames = buckets || [];
    this.options = ['KEK', 'bucket', ...this.mnSecuritySecretsService.types];
    this.mapTypeToNames = this.mnSecuritySecretsService.mapTypeToNames;
    this.preventMinus = this.mnSecuritySecretsService.preventMinus;

    this.form = this.mnFormService.create(this)
      .setFormGroup({
        name: '',
        type: 'awskms-symmetric-key',
        'generated-secret': this.formBuilder.group({
          encryptWith: 'nodeSecretManager',
          encryptWithKeyId: null,
          canBeCached: true,
          autoRotation: false,
          rotationIntervalInDays: null,
          nextRotationTime: this.formBuilder.group({
            date: new Date(),
            hour: null,
            minute: null
          })
        }),
        'aws-secret': this.formBuilder.group({
          keyARN: "",
          region: "",
          useIMDS: false,
          credentialsFile: "",
          configFile: "",
          profile: ""
        }),
        'kmip-secret': this.formBuilder.group({
          caSelection: 'useSysAndCbCa',
          reqTimeoutMs: null,
          encryptionApproach: 'useGet',
          encryptWith: 'nodeSecretManager',
          encryptWithKeyId: null,
          activeKey: "",
          keyPath: "",
          certPath: "",
          keyPassphrase: "",
          host: "",
          port: null
        }),
        usageBuckets: this.formBuilder.group(this.bucketNames.reduce((acc, bucket) => {
          //set false by default for editing mode and true for adding mode
          acc['bucket-encryption-' + bucket] = !this.item;
          return acc;
        }, {})),
        usage: this.formBuilder.group(this.options.reduce((acc, v) => {
          //set false by default for editing mode and true for adding mode
          acc[v + '-encryption'] = !this.item;
          return acc;
        }, {}))
      })
      .setPackPipe(map(this.packData.bind(this)))
      .setPostRequest(this.item ? this.mnSecuritySecretsService.stream.putSecret : this.mnSecuritySecretsService.stream.postSecret)
      .setReset(this.uiRouter.stateService.reload)
      .successMessage(this.item ? "Encryption key updated successfully!" : "Encryption key created successfully!")
      .success(() => {
        this.mnSecuritySecretsService.stream.updateSecretsList.next();
        this.activeModal.dismiss();
      })
      .trackSubmit()
      .clearErrors();

    this.filteredSecrets = this.secrets.filter(secret => {
      return secret.usage.find(u => this.item?.id !== secret.id && u.includes('KEK-encryption'));
    });

    this.httpError = this.item ?
      this.mnSecuritySecretsService.stream.putSecret.error : this.mnSecuritySecretsService.stream.postSecret.error;
    this.testHttpError = this.item ?
      this.mnSecuritySecretsService.stream.testPutSecret.error : this.mnSecuritySecretsService.stream.testPostSecret.error;
    this.error = merge(this.httpError, this.testHttpError);

    let testAddResponse = this.mnSecuritySecretsService.stream.testPostSecret.response;
    let testEditResponse = this.mnSecuritySecretsService.stream.testPutSecret.response;

    this.isTestResultValid = this.item ?
      testEditResponse.pipe(map(resp => !(resp instanceof HttpErrorResponse))) :
      testAddResponse.pipe(map(resp => !(resp instanceof HttpErrorResponse)));

    this.formType =
      this.form.group.get('type').valueChanges
        .pipe(startWith(this.form.group.get('type').value));

    const isAutoRotationEnabled =
      this.form.group.get('generated-secret.autoRotation').valueChanges
        .pipe(startWith(this.form.group.get('generated-secret.autoRotation').value));

    const isDataEnabled =
      this.form.group.get('usage.bucket-encryption').valueChanges
        .pipe(startWith(this.form.group.get('usage.bucket-encryption').value));

    isDataEnabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeToggleAllBuckets.bind(this));

    isAutoRotationEnabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeEnableRotation.bind(this));

    this.testSettings = this.mnFormService.create(this)
      .setFormGroup({})
      .setPackPipe(map(this.packData.bind(this)))
      .setPostRequest(this.item ? this.mnSecuritySecretsService.stream.testPutSecret : this.mnSecuritySecretsService.stream.testPostSecret)
      .trackSubmit()
      .clearErrors();

    if (this.item) {
      setTimeout(() => {
        this.form.group.patchValue(this.unpackData(this.item));
        this.form.group.get('type').disable();
        this.form.group.get('aws-secret.region').disable();
        this.form.group.get('aws-secret.keyARN').disable();
      });
    }

    this.serverTimeExample = this.item ? (new BehaviorSubject(this.item.creationDateTime)) : '';
  }

  isAllUsesSelected() {
    return Object.values(this.form.group.get('usage').value).every(v => v);
  }

  getSelected() {
    const isAnyBucketSelected = Object.values(this.form.group.get('usageBuckets').value).some(v => v);
    const selected = this.options.filter(v => this.form.group.get('usage').value[v + '-encryption']).map(this.mapTypeToNames).join(', ');
    if (isAnyBucketSelected && !selected.includes('Data')) {
        return selected.length ? selected + ', Data (Custom)' : 'Data (Custom)';
    }
    return selected;
  }

  maybeToggleAllBuckets(value) {
    this.form.group.get('usageBuckets')[value ? 'disable' : 'enable']({emitEvent: false});
  }

  maybeEnableRotation(enable) {
    this.form.group.get('generated-secret.rotationIntervalInDays')[enable ? "enable" : "disable"]();
    this.form.group.get('generated-secret.nextRotationTime')[enable ? "enable" : "disable"]();
  }

  maybeEnableForm(writePermission) {
    this.form.group[writePermission ? "enable" : "disable"]({emitEvent: false});
  }

  setDate(value) {
    // take the exact date from the input (MB-68377)
    const [year, month, day] = value.split('-').map(Number);
    const localDate = new Date(year, month - 1, day); // month is 0-indexed
    this.form.group.get('generated-secret.nextRotationTime.date').setValue(localDate);
  }

  unpackData(item) {
    let rv = {
      name: item.name,
      type: item.type,
      usage: item.usage.reduce((acc, v) => {
        //it is safe to unpack both usage and usageBuckets equally since
        //angular's patchValue filters out unknown keys
        acc[v] = true;
        return acc;
      }, {}),
      usageBuckets: item.usage.reduce((acc, v) => {
        //it is safe to unpack both usage and usageBuckets equally since
        //angular's patchValue filters out unknown keys
        acc[v] = true;
        return acc;
      }, {})
    };

    switch (rv.type) {
      case 'awskms-symmetric-key':
        rv['aws-secret'] = item.data;
        break;
      case 'cb-server-managed-aes-key-256':
        if (item.data.autoRotation) {
          const localTime = new Date(item.data.nextRotationTime);
          rv['generated-secret'] = {
            autoRotation: item.data.autoRotation,
            rotationIntervalInDays: item.data.rotationIntervalInDays,
            nextRotationTime: {
              date: localTime,
              hour: localTime.getHours(),
              minute: localTime.getMinutes(),
            },
            canBeCached: item.data.canBeCached
          };
        } else {
          rv['generated-secret'] = {
            autoRotation: item.data.autoRotation,
            canBeCached: item.data.canBeCached
          };
        }
        const {encryptWithKeyId, encryptWith} = item.data;
        rv['generated-secret'].encryptWith = encryptWith;
        if (encryptWith === 'encryptionKey') {
          rv['generated-secret']['encryptWithKeyId'] = (encryptWithKeyId === null || encryptWithKeyId === undefined || encryptWithKeyId < 0) ? null : this.secrets.find(i => i.id === encryptWithKeyId);
        }
        break;
      case 'kmip-aes-key-256':
        rv['kmip-secret'] = item.data;
        rv['kmip-secret'].encryptWith = item.data.encryptWith;
        rv['kmip-secret'].encryptWithKeyId = item.data.encryptWithKeyId;
        if (item.data.encryptWith === 'encryptionKey') {
          rv['kmip-secret'].encryptWithKeyId = (item.data.encryptWithKeyId === null || item.data.encryptWithKeyId === undefined || item.data.encryptWithKeyId < 0) ? null : this.secrets.find(i => i.id === item.data.encryptWithKeyId);
        }
        rv['kmip-secret'].activeKey = item.data.activeKey?.kmipId;
        break;
    }
    return rv;
  }

  packData() {
    let value = this.form.group.getRawValue();
    let {usage, usageBuckets, name, type, 'generated-secret': generatedSecret, 'aws-secret': awsSecret, 'kmip-secret': kmipSecret} = value;

    let data = {};
    switch (type) {
      case 'awskms-symmetric-key':
        data = awsSecret;
        break;
      case 'cb-server-managed-aes-key-256':
        const {rotationIntervalInDays, nextRotationTime, autoRotation, encryptWith, encryptWithKeyId, canBeCached} = generatedSecret;
        data = {autoRotation, encryptWith};
        if (encryptWith === 'encryptionKey') {
          data.encryptWithKeyId = encryptWithKeyId?.id ?? -1;
        }
        if (autoRotation) {
          data.rotationIntervalInDays = rotationIntervalInDays;
          const {date, hour, minute} = nextRotationTime;
          date.setHours(hour || 0);
          date.setMinutes(minute || 0);
          date.setSeconds(0);
          date.setMilliseconds(0);
          var copiedDate = new Date(date.getTime());
          data.nextRotationTime = copiedDate.toISOString();
        }
        data.canBeCached = canBeCached;
        break;
      case 'kmip-aes-key-256':
        data = kmipSecret;
        data.encryptWithKeyId = data.encryptWith === 'nodeSecretManager' ? -1 : data.encryptWithKeyId;
        data.activeKey = {kmipId: data.activeKey};
        if (data.encryptWith === 'encryptionKey') {
          data.encryptWithKeyId = data.encryptWithKeyId?.id ?? -1;
        }
        if (!data.keyPassphrase || data.keyPassphrase === '******') {
          data.keyPassphrase = undefined;
        }
        break;
    }

    let usageToSend = Object.keys(usage).filter(v => usage[v]);
    if (!usageToSend.includes('bucket-encryption')) {
      usageToSend = usageToSend.concat(Object.keys(usageBuckets).filter(v => usageBuckets[v]));
    }

    return [{ name, type, usage: usageToSend, data}, this.item?.id];
  }

  valuesMapping(item) {
    if (item === -1) {
      return null;
    }
    return item ? item.name || '[empty name]' : item;
  }
}
