/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {takeUntil} from 'rxjs/operators';
import {Subject} from 'rxjs';
import {UIRouter} from '@uirouter/angular';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream, DetailsHashObserver} from './mn.core.js';
import {MnXDCRAddRefComponent} from "./mn.xdcr.add.ref.component.js";
import {MnXDCRDeleteRefComponent} from "./mn.xdcr.delete.ref.component.js";
import template from "./mn.xdcr.ref.item.html";

export {MnXDCRRefItemComponent};

class MnXDCRRefItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-ref-item",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ]
    })
  ]}

  static get parameters() { return [
    MnPermissions,
    NgbModal,
    UIRouter
  ]}

  constructor(mnPermissions, modalService, uiRouter) {
    super();

    var onAddReference = new Subject();
    onAddReference
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(item => {
        var ref = modalService.open(MnXDCRAddRefComponent);
        ref.componentInstance.item = item;
      });

    var onDeleteReference = new Subject();
    onDeleteReference
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(function (item) {
        var ref = modalService.open(MnXDCRDeleteRefComponent);
        ref.componentInstance.item = item;
      });

    this.uiRouter = uiRouter;
    this.permissions = mnPermissions.stream;
    this.onAddReference = onAddReference;
    this.onDeleteReference = onDeleteReference;
  }

  ngOnInit() {
    this.detailsHashObserver =
      new DetailsHashObserver(this.uiRouter, this, "xdcrDetails", this.item.name);
  }

  generateStatisticsLink(row) {
    return window.location.protocol + '//' +
      row.hostname + '/index.html#/analytics/?statsHostname=' +
      (encodeURIComponent(row.hostname))
  }
}
