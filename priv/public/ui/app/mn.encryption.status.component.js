/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in
the file licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {pluck, map} from 'rxjs/operators';
import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnSecuritySecretsService} from './mn.security.secrets.service.js';
import {MnTimezoneDetailsService} from './mn.timezone.details.service.js';
import {MnSecurityService} from './mn.security.service.js';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {MnEncryptionForceConfirmationComponent} from './mn.encryption.force.confirmation.component.js';

import template from "./mn.encryption.status.html";


export {MnEncryptionStatusComponent};

class MnEncryptionStatusComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-encryption-status",
      template,
      inputs: [
        "isEncryptionEnabled",
        "encryptionInfo",
        "itemType", // 'config' | 'audit' | 'logs' | 'bucket'
        "bucketName" // required if itemType is 'bucket'
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnSecuritySecretsService,
    MnTimezoneDetailsService,
    MnSecurityService,
    NgbModal,
  ]}

  constructor(mnSecuritySecretsService, mnTimezoneDetailsService, mnSecurityService, modalService) {
    super();

    this.mnTimezoneDetailsService = mnTimezoneDetailsService;
    this.mnSecurityService = mnSecurityService;
    this.modalService = modalService;

    this.mapEncryptionStatusToLabels = mnSecuritySecretsService.mapEncryptionStatusToLabels;
    this.currentEncryptionInfo = this.mnOnChanges.pipe(pluck('encryptionInfo', 'currentValue'));
  }

  ngOnInit() {
    this.statusLabel = this.currentEncryptionInfo.pipe(map(r => this.mapEncryptionStatusToLabels(r.dataStatus)));
    this.hasIssues = this.currentEncryptionInfo.pipe(map(r => !!r.issues?.length));
    this.shouldShowIcon = this.currentEncryptionInfo.pipe(map(this.isIconVisible.bind(this)));
  }

  forceEncryption() {
    const ref = this.modalService.open(MnEncryptionForceConfirmationComponent);
    ref.componentInstance.itemType = this.itemType;
    ref.componentInstance.bucketName = this.bucketName;
    ref.componentInstance.isEncryptionEnabled = this.isEncryptionEnabled;

    ref.result.then((confirmed) => {
      if (confirmed) {
        this.mnSecurityService.forceEncryption(this.itemType, this.bucketName)
          .subscribe({
            next: (response) => {
            },
            error: (error) => {
              console.error('Error initiating force encryption:', error);
            }
          });
      }
    }).catch((error) => {
    });
  }

  isIconVisible(encryption) {
    let hiddenCondition = !this.isEncryptionEnabled && (encryption.dataStatus === 'unencrypted') && !encryption.issues.length;
    return this.isEncryptionEnabled || !hiddenCondition;
  }

  getUserVisibleDataStatus(encryption) {
    switch (encryption.dataStatus) {
      case 'encrypted': return 'Fully Encrypted';
      case 'partiallyEncrypted': return 'Partially Encrypted';
      case 'unencrypted': return 'Not Encrypted';
      case 'unknown':
      default:
        return 'unknown';
    }
  }
}
