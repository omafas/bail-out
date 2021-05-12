/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnPeriod =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnPeriod, mn.core.MnEventableComponent);

    MnPeriod.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-period.html",
        selector: "mn-period",
        inputs: [
          "group",
          "error",
          "errorGroup"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnPeriod.parameters = [];

    return MnPeriod;

    function MnPeriod() {
      mn.core.MnEventableComponent.call(this);
      this.componentID = Math.random();
    }
  })(window.rxjs);
