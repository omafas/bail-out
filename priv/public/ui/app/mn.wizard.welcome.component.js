/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {BehaviorSubject} from 'rxjs';

import {MnLifeCycleHooksToStream } from './mn.core.js';
import {MnAdminService} from './mn.admin.service.js';
import template from "./mn.wizard.welcome.html";

export {MnWizardWelcomeComponent};

class MnWizardWelcomeComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnAdminService
  ]}

  constructor(mnAdminService) {
    super();
    this.focusFieldSubject = new BehaviorSubject(true);
    this.prettyVersion = mnAdminService.stream.prettyVersion;
    this.majorMinorVersion = mnAdminService.stream.majorMinorVersion;
  }
}
