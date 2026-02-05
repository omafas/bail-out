/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {UIRouter} from '@uirouter/angularjs';
import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream, DetailsHashObserver} from './mn.core.js';
import template from "./mn.security.secrets.item.html";
import {MnSecuritySecretsService} from "./mn.security.secrets.service.js";

export {MnSecuritySecretsItemComponent};

class MnSecuritySecretsItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() {
    return [
      new Component({
        selector: 'mn-security-secrets-item',
        template,
        inputs: [
          'item'
        ],
        changeDetection: ChangeDetectionStrategy.OnPush
      })
    ];
  }

  static get parameters() {
    return [
      UIRouter,
      MnPermissions,
      MnSecuritySecretsService
    ];
  }

  constructor(uiRouter, mnPermissions, mnSecuritySecretsService) {
    super();

    this.uiRouter = uiRouter;
    this.permissions = mnPermissions.stream;
    this.mapTypeToNames = mnSecuritySecretsService.mapTypeToNames;
  }

  ngOnInit() {
    this.detailsHashObserver =
      new DetailsHashObserver(this.uiRouter, this, 'openedSecrets', this.item.id.toString());
  }

}
