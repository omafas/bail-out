/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {FormControl} from '@angular/forms';

import {MnPoolsService} from './mn.pools.service.js';
import {MnWizardService} from './mn.wizard.service.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import template from "./mn.node.storage.config.html";

export {MnNodeStorageConfigComponent};

class MnNodeStorageConfigComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-node-storage-config",
      template,
      inputs: [
        "group"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnWizardService,
    MnPoolsService
  ]}

  constructor(mnWizardService, mnPoolsService) {
    super();
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.postNodeInitHttp = mnWizardService.stream.postNodeInitHttp
    this.postClusterInitHttp = mnWizardService.stream.postClusterInitHttp
  }

  addCbasPathField() {
    var last = this.group.get('storage.cbas_path').length - 1;

    this.group
      .get('storage.cbas_path')
      .push(new FormControl(this.group.get('storage.cbas_path').value[last]));
  }

  removeCbasPathField() {
    var last = this.group.get('storage.cbas_path').length - 1;
    this.group.get('storage.cbas_path').removeAt(last);
  }
}
