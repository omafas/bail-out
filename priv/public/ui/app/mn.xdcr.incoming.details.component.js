/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core'
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {pluck, map} from 'rxjs/operators';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnXDCRService} from './mn.xdcr.service.js';
import {MnFormService} from "./mn.form.service.js";
import {MnHelperService} from "./mn.helper.service.js";
import template from "./mn.xdcr.incoming.details.html";

export {MnXDCRIncomingDetailsComponent};

class MnXDCRIncomingDetailsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-incoming-details",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ]
    })
  ]}

  static get parameters() { return [
    MnPermissions,
    MnXDCRService,
    MnFormService,
    NgbModal,
    MnHelperService
  ]}

  constructor(mnPermissions, mnXDCRService, mnFormService, modalService, mnHelperService) {
    super();

    this.permissions = mnPermissions.stream;
    this.toggler = mnHelperService.createToggle();

    let itemStream = this.mnOnChanges.pipe(pluck("item", "currentValue"));
    this.hasReplications = itemStream.pipe(map(item => !!item.SourceClusterReplSpecs.length));
    this.replications = itemStream.pipe(map(item => (item.SourceClusterReplSpecs || []).sort((rep1, rep2) => rep1.sourceBucketName.localeCompare(rep2.sourceBucketName))));
    this.sourceNodes = itemStream.pipe(map(item => item.SourceClusterNodes));
    this.hasSourceNodes = itemStream.pipe(map(item => !!item.SourceClusterNodes.length));
    this.sourceClusterName = itemStream.pipe(map(item => item.SourceClusterName));
    this.sourceClusterId = itemStream.pipe(map(item => item.SourceClusterUUID));
  }

  ngOnInit() {}

  trackByFn(_, row) {
    return row.id;
  }
}
