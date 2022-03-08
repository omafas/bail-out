/*
  Copyright 2021-Present Couchbase, Inc.

  Use of this software is governed by the Business Source License included in
  the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
  file, in accordance with the Business Source License, use of this software
  will be governed by the Apache License, Version 2.0, included in the file
  licenses/APL2.txt.
*/
import {Component, ChangeDetectionStrategy} from '@angular/core';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnPermissions} from './ajs.upgraded.providers.js';
import template from "./mn.logs.html";

export {MnLogsComponent};

class MnLogsComponent extends MnLifeCycleHooksToStream {

  static get annotations() {
    return [
      new Component({
        template,
        changeDetection: ChangeDetectionStrategy.OnPush
      })
  ]}

  static get parameters() { return [
    MnPermissions
  ]}

  constructor(mnPermissions) {
    super();

    this.permissions = mnPermissions.stream;
  }
}
