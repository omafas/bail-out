/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core'
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {Subject, BehaviorSubject} from 'rxjs';
import {takeUntil, map, withLatestFrom} from 'rxjs/operators';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from "./mn.form.service.js";
import {MnHelperService} from "./mn.helper.service.js";
import {MnSecuritySecretsDeleteDialogComponent} from "./mn.security.secrets.delete.dialog.component.js";
import {MnSecuritySecretsAddDialogComponent} from "./mn.security.secrets.add.dialog.component.js";
import {MnSecuritySecretsDeleteKeyDialogComponent} from "./mn.security.secrets.delete.key.dialog.component.js";
import template from "./mn.security.secrets.item.details.html";
import {MnSecuritySecretsService} from "./mn.security.secrets.service.js";
import {MnTimezoneDetailsService} from './mn.timezone.details.service.js';


export {MnSecuritySecretsItemDetailsComponent};

class MnSecuritySecretsItemDetailsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-security-secrets-item-details",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ]
    })
  ]}

  static get parameters() { return [
    MnPermissions,
    MnFormService,
    NgbModal,
    MnHelperService,
    MnSecuritySecretsService,
    MnTimezoneDetailsService
  ]}

  constructor(mnPermissions, mnFormService, modalService, mnHelperService, mnSecuritySecretsService, mnTimezoneDetailsService) {
    super();

    var onDeleteSecret = new Subject();
    onDeleteSecret
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(item => {
        var ref = modalService.open(MnSecuritySecretsDeleteDialogComponent);
        ref.componentInstance.item = item;
      });

    let onDeleteKey = new Subject();
    onDeleteKey
    .pipe(takeUntil(this.mnOnDestroy))
    .subscribe(data => {
      let ref = modalService.open(MnSecuritySecretsDeleteKeyDialogComponent);
      ref.componentInstance.secret = data.item;
      ref.componentInstance.key = data.key;
    });

    var onEditKey = new Subject();
    onEditKey
      .pipe(withLatestFrom(mnSecuritySecretsService.stream.getSecrets),
            takeUntil(this.mnOnDestroy))
      .subscribe(([item, secrets]) => {
        var ref = modalService.open(MnSecuritySecretsAddDialogComponent);
        ref.componentInstance.item = item;
        ref.componentInstance.secrets = secrets;
      });

    this.rotateKey = mnFormService.create(this)
      .setFormGroup({})
      .setPackPipe(map(() => this.item.id))
      .setPostRequest(mnSecuritySecretsService.stream.postRotateSecret)
      .trackSubmit()
      .successMessage("Key was rotated successfully!")
      .errorMessage("An error occurred rotating the key.")
      .success(() => mnSecuritySecretsService.stream.updateSecretsList.next())

    let serverTime = this.item?.keys.length ? this.item.keys[0].creationDateTime : '';
    this.serverTimeExample = new BehaviorSubject(serverTime);

    this.permissions = mnPermissions.stream;
    this.onDeleteSecret = onDeleteSecret;
    this.onDeleteKey = onDeleteKey;
    this.onEditKey = onEditKey;
    this.toggler = mnHelperService.createToggle();
    this.mnTimezoneDetailsService = mnTimezoneDetailsService;
  }
}
