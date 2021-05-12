/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {combineLatest} from "/ui/web_modules/rxjs.js";

import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnXDCRService} from "./mn.xdcr.service.js";
import {MnPoolsService} from "./mn.pools.service.js";
import {MnAdminService} from "./mn.admin.service.js";
import {MnHelperService} from "./mn.helper.service.js";

export {MnXDCRSettingsComponent};

class MnXDCRSettingsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-settings",
      templateUrl: "/ui/app/mn.xdcr.settings.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "form"
      ]
    })
  ]}

  static get parameters() { return [
    MnXDCRService,
    MnPoolsService,
    MnAdminService,
    MnHelperService
  ]}

  constructor(mnXDCRService, mnPoolsService, mnAdminService, mnHelperService) {
    super();

    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.compatVersion55 = mnAdminService.stream.compatVersion55;
    this.error = mnXDCRService.stream.postSettingsReplicationsValidation.error;

    this.toggler = mnHelperService.createToggle();
  }
}
