/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {pluck, map} from 'rxjs/operators';
import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnSecuritySecretsService} from './mn.security.secrets.service.js';
import {MnTimezoneDetailsService} from './mn.timezone.details.service.js';

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
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnSecuritySecretsService,
    MnTimezoneDetailsService
  ]}

  constructor(mnSecuritySecretsService, mnTimezoneDetailsService) {
    super();

    this.mnTimezoneDetailsService = mnTimezoneDetailsService;

    this.mapEncryptionStatusToLabels = mnSecuritySecretsService.mapEncryptionStatusToLabels;
    this.currentEncryptionInfo = this.mnOnChanges.pipe(pluck('encryptionInfo', 'currentValue'));
  }

  ngOnInit() {
    this.statusLabel = this.currentEncryptionInfo.pipe(map(r => this.mapEncryptionStatusToLabels(r.dataStatus)));
    this.hasIssues = this.currentEncryptionInfo.pipe(map(r => !!r.issues.length));
    this.shouldShowIcon = this.currentEncryptionInfo.pipe(map(this.isIconVisible.bind(this)));
  }

  isIconVisible(encryption) {
    let hiddenCondition = !this.isEncryptionEnabled && (encryption.dataStatus === 'unencrypted') && !encryption.issues.length;
    return this.isEncryptionEnabled || !hiddenCondition;
  }
}
