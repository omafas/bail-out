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
mn.components.MnBarUsage =
  (function () {
    "use strict";

    MnBarUsageComponent.annotations = [
      new ng.core.Component({
        selector: "mn-bar-usage",
        templateUrl: "app-new/mn-bar-usage.html",
        inputs: [
          "baseInfo",
          "total"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    return MnBarUsageComponent;

    function MnBarUsageComponent() {}
  })();
