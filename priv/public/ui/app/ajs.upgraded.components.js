/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Directive, ElementRef, Injector} from '/ui/web_modules/@angular/core.js';
import {UpgradeComponent} from '/ui/web_modules/@angular/upgrade/static.js';

export {MnDetailStatsDirective, MnMainSpinnerDirective};

class MnDetailStatsDirective extends UpgradeComponent {
  static get annotations() { return [
    new Directive({
      selector: "mn-detail-stats",
      inputs: [
        "mnTitle",
        "bucket",
        "itemId",
        "service",
        "prefix",
        "nodeName"
      ]
    })
  ]}

  static get parameters() { return [
    ElementRef,
    Injector
  ]}

  constructor(elementRef, injector) {
    super('mnDetailStats', elementRef, injector);
  }
}

class MnMainSpinnerDirective extends UpgradeComponent {
  static get annotations() { return [
    new Directive({
      selector: "mn-main-spinner",
      inputs: [
        "mnSpinnerValue"
      ]
    })
  ]}

  static get parameters() { return [
    ElementRef,
    Injector
  ]}

  constructor(elementRef, injector) {
    super('mnMainSpinner', elementRef, injector);
  }
}
