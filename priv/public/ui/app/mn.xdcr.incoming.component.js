/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {NEVER} from 'rxjs';
import {
  shareReplay,
  switchMap, startWith, pluck, map
} from 'rxjs/operators';
import {UIRouter} from '@uirouter/angular';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream, DetailsHashObserver} from './mn.core.js';
import template from "./mn.xdcr.incoming.html";

export {MnXDCRIncomingComponent};

class MnXDCRIncomingComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-incoming",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ]
    })
  ]}

  static get parameters() { return [
    MnPermissions,
    UIRouter
  ]}

  constructor(mnPermissions, uiRouter) {
    super();

    this.uiRouter = uiRouter;
    this.permissions = mnPermissions.stream;

    let itemStream = this.mnOnChanges.pipe(pluck("item", "currentValue"));
    this.sourceClusterName = itemStream.pipe(map(item => item.SourceClusterName));
    this.sourceClusterId = itemStream.pipe(map(item => item.SourceClusterUUID));
    this.replicationCount = itemStream.pipe(map(item => item.SourceClusterReplSpecs.length));
  }

  ngOnInit() {
    let detailsHashObserver = new DetailsHashObserver(
      this.uiRouter, this, "xdcrIncomingDetails", this.item.SourceClusterUUID);
    let isDetailsOpened = this.permissions
        .pipe(switchMap((perm) => {
          return perm.cluster.xdcr.settings.read ?
            detailsHashObserver.stream.isOpened : NEVER;
        }),
              startWith(false),
              shareReplay(1));

    this.detailsHashObserver = detailsHashObserver;
    this.isDetailsOpened = isDetailsOpened;
  }
}
