/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';

import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from './mn.form.service.js';
import template from "./mn.security.secrets.encryption.dialog.html";
import {FormBuilder} from '@angular/forms';
import {map} from 'rxjs/operators';
import {MnSecuritySecretsService} from './mn.security.secrets.service.js';
import {timeUnitToSeconds} from './constants/constants.js';

export {MnSecuritySecretsEncryptionDialogComponent};

class MnSecuritySecretsEncryptionDialogComponent extends MnLifeCycleHooksToStream {

  static get annotations() { return [
    new Component({
      template,
      inputs: [
        'secrets',
        'type'
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnSecuritySecretsService,
    NgbActiveModal,
    MnFormService,
    FormBuilder,
  ]}

  constructor(mnSecuritySecretsService, activeModal, mnFormService, formBuilder) {
    super();

    this.formBuilder = formBuilder;
    this.activeModal = activeModal;
    this.mnSecuritySecretsService = mnSecuritySecretsService;
    this.mnFormService = mnFormService;
    this.mapTypeToNames = this.mnSecuritySecretsService.mapTypeToNames;
    this.preventMinus = this.mnSecuritySecretsService.preventMinus;
  }

  ngOnInit() {
    this.form = this.mnFormService.create(this)
      .setFormGroup(this.doUnpack(this.config[this.type]))
      .setPackPipe(map(this.packData.bind(this)))
      .setPostRequest(this.mnSecuritySecretsService.stream.postEncryptionAtRestType)
      .fieldToggler(['dekLifetimeEnabled', 'dekLifetime'])
      .fieldToggler(['dekRotationIntervalEnabled', 'dekRotationInterval'])
      .successMessage("Encryption at rest config saved successfully!")
      .success(() => {
        this.activeModal.dismiss();
        this.mnSecuritySecretsService.stream.updateEncryptionAtRest.next();
      });

    this.httpError = this.mnSecuritySecretsService.stream.postEncryptionAtRestType.error;
    this.filteredSecrets = this.secrets.filter(secret => secret.usage.find(u => u.includes(this.type + '-encryption') ));
  }

  doUnpack({encryptionMethod, encryptionKeyId, dekLifetime, dekRotationInterval}) {
    let dekLifetimeUnpacked;
    if (dekLifetime === 0) {
      dekLifetimeUnpacked = 0;
    } else {
      dekLifetimeUnpacked = (dekLifetime ? dekLifetime : timeUnitToSeconds.year) / timeUnitToSeconds.day;
    }

    return {
      encryptionMethod: encryptionMethod || 'disabled',
      encryptionKeyId: (encryptionKeyId === null || encryptionKeyId === undefined || encryptionKeyId < 0) ? null : this.secrets.find(i => i.id === encryptionKeyId),
      dekLifetime: dekLifetimeUnpacked,
      dekLifetimeEnabled: dekLifetime !== 0,
      dekRotationInterval: (dekRotationInterval ? dekRotationInterval : timeUnitToSeconds.month) / timeUnitToSeconds.day,
      dekRotationIntervalEnabled: dekRotationInterval !== 0
    };
  }

  doPack({encryptionMethod, encryptionKeyId, dekLifetime, dekRotationInterval, dekLifetimeEnabled, dekRotationIntervalEnabled}) {
    return {
      encryptionMethod,
      encryptionKeyId: encryptionMethod === 'encryptionKey' ? encryptionKeyId?.id ?? -1 : -1,
      dekLifetime: dekLifetimeEnabled ? Math.round(dekLifetime * timeUnitToSeconds.day) : 0,
      dekRotationInterval: dekRotationIntervalEnabled ? Math.round(dekRotationInterval * timeUnitToSeconds.day) : 0
    };
  }

  packData() {
    return [this.type, this.doPack(this.form.group.getRawValue())];
  }

  valuesMapping(item) {
    return item ? item.name || '[empty name]' : item;
  }
}
