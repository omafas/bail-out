/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {MnHelperService} from './mn.helper.service.js';
import {takeUntil, withLatestFrom, map, shareReplay} from 'rxjs/operators';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {MnSecuritySecretsService} from './mn.security.secrets.service.js';
import {MnTimezoneDetailsService} from './mn.timezone.details.service.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnPermissions} from './ajs.upgraded.providers.js';
import template from "./mn.security.secrets.html";
import {MnSecuritySecretsAddDialogComponent} from './mn.security.secrets.add.dialog.component.js';
import {MnSecuritySecretsEncryptionDialogComponent} from './mn.security.secrets.encryption.dialog.component.js';
import {MnSecuritySecretsReencryptConfirmationComponent} from './mn.security.secrets.reencrypt.confirmation.component.js';
import {Subject} from 'rxjs';
import {timeUnitToSeconds} from './constants/constants.js';
import {DatePipe} from '@angular/common';

export {MnSecuritySecretsComponent};

class MnSecuritySecretsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnPermissions,
    MnSecuritySecretsService,
    MnTimezoneDetailsService,
    MnHelperService,
    NgbModal,
    DatePipe
  ]}

  constructor(mnPermissions, mnSecuritySecretsService, mnTimezoneDetailsService, mnHelperService, modalService, datePipe) {
    super();

    this.secondsInDay = timeUnitToSeconds.day;
    this.datePipe = datePipe;
    this.mnTimezoneDetailsService = mnTimezoneDetailsService;

    this.getEncryptionAtRest = mnSecuritySecretsService.stream.getEncryptionAtRest;
    this.getEncryptionAtRestKeys = mnSecuritySecretsService.stream.getEncryptionAtRestKeys.pipe(map(this.displayOrder));
    this.mapTypeToNames = mnSecuritySecretsService.mapTypeToNames;
    this.mapTypeToReEncryptNames = mnSecuritySecretsService.mapTypeToReEncryptNames;
    this.mapMethodToNames = mnSecuritySecretsService.mapMethodToNames;
    this.secretsByIds = mnSecuritySecretsService.stream.secretsByIds;

    this.modalService = modalService;
    this.permissions = mnPermissions.stream;
    this.types = mnSecuritySecretsService.types;

    this.sorter = mnHelperService.createSorter('creationDateTime', true);
    this.filter = mnHelperService.createFilter(this,
                                               ['name', 'type', '_uiUsage', '_uiCreationDateTime', '_uiMediumTime'],
                                               true);

    this.onAddSecretClick = new Subject();
    this.onAddSecretClick
      .pipe(withLatestFrom(mnSecuritySecretsService.stream.getSecrets),
            takeUntil(this.mnOnDestroy))
      .subscribe(([, secrets]) => {
        const ref = this.modalService.open(MnSecuritySecretsAddDialogComponent);
        ref.componentInstance.item = null;
        ref.componentInstance.secrets = secrets;
      });

    this.onEncryptionAtRestClick = new Subject();
    this.onEncryptionAtRestClick
      .pipe(withLatestFrom(mnSecuritySecretsService.stream.getSecrets,
                           mnSecuritySecretsService.stream.getEncryptionAtRest),
            takeUntil(this.mnOnDestroy))
      .subscribe(([type, secrets, config]) => {
        const ref = this.modalService.open(MnSecuritySecretsEncryptionDialogComponent);
        ref.componentInstance.type = type;
        ref.componentInstance.secrets = secrets;
        ref.componentInstance.config = config;
      });

    this.onReencryptClick = new Subject();
    this.onReencryptClick
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe((type) => {
        const ref = this.modalService.open(MnSecuritySecretsReencryptConfirmationComponent);
        ref.componentInstance.type = type;
        ref.componentInstance.bucketName = null;
      });

    this.secrets = mnSecuritySecretsService.stream.getSecrets;

    this.filteredSecrets = this.secrets
      .pipe(map(this.addUiFields.bind(this)),
        this.filter.pipe,
        this.sorter.pipe,
        shareReplay({refCount: true, bufferSize: 1}));

    this.serverTimeExample = this.secrets.pipe(map(secrets => secrets.length ? secrets[0].creationDateTime : ''));
  }

  trackByMethod(i, item) {
    return item.id;
  }

  usageToReadableWords(usage) {
    const usages = usage.filter(usage => usage.endsWith('-encryption'));
    const usagesBucket = usage.filter(usage => usage.includes('-encryption-'));
    const rv = usages.map(usage => this.mapTypeToNames(usage.split('-')[0]));
    if (usagesBucket.length) {
      rv.push(`Data (${usagesBucket.map(usage => usage.split('-')[2])})`);
    }
    return rv.join(', ');
  }

  addUiFields(secrets) {
    return secrets.map(secret => {
      secret._uiUsage = this.usageToReadableWords(secret.usage);
      secret._uiMediumTime = this.datePipe.transform(secret.creationDateTime, 'mediumTime');
      secret._uiCreationDateTime = this.datePipe.transform(secret.creationDateTime, 'd MMM, y');
      return secret;
    });
  }

  displayOrder(keys) {
    return keys.sort((key) => {
      if (key === 'config') {
        return -1;
      } else return 1;
    });
  }
}
