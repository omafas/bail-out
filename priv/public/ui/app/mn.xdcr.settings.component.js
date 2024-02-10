/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';

import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnPoolsService} from "./mn.pools.service.js";
import {MnAdminService} from "./mn.admin.service.js";
import {MnHelperService} from "./mn.helper.service.js";
import template from "./mn.xdcr.settings.html";

export {MnXDCRSettingsComponent};

class MnXDCRSettingsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-settings",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "form",
        "validationRequest"
      ]
    })
  ]}

  static get parameters() { return [
    MnPoolsService,
    MnAdminService,
    MnHelperService
  ]}

  constructor(mnPoolsService, mnAdminService, mnHelperService) {
    super();

    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.compatVersion55 = mnAdminService.stream.compatVersion55;
    this.toggler = mnHelperService.createToggle();
  }

  ngOnInit() {
    this.error = this.validationRequest.error;
    this.success = this.validationRequest.success;
  }
}
