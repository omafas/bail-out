/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';

export {MnXDCRRepMessageComponent};

class MnXDCRRepMessageComponent {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-rep-message",
      templateUrl: "app/mn.xdcr.rep.message.html",
      inputs: [
        "fromBucket",
        "toBucket",
        "toCluster",
        "isEditMode"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return []}

  constructor() {}
}
