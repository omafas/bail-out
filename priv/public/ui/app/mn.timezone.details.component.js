/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {map} from 'rxjs/operators';
import {MnTimezoneDetailsService} from './mn.timezone.details.service.js';
import template from './mn.timezone.details.html';

export {MnTimezoneDetailsComponent};

class MnTimezoneDetailsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-timezone-details",
      template,
      inputs: [
        'serverTime'
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnTimezoneDetailsService
  ]}

  constructor(mnTimezoneDetailsService) {
    super();

    this.mnTimezoneDetailsService = mnTimezoneDetailsService;
  }

  ngOnInit() {
    this.localGMT = this.mnTimezoneDetailsService.getLocalGMTString();
    this.localTimezoneLabel = this.mnTimezoneDetailsService.getLocalTimezoneLabel();
    this.serverGMTOffset = this.serverTime.pipe(map(this.mnTimezoneDetailsService.getServerGMTOffset));
  }
}
