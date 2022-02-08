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
mn.components.MnQuerySettingsConfig =
  (function () {
    "use strict";

    MnQuerySettingsConfig.annotations = [
      new ng.core.Component({
        selector: "mn-query-settings-config",
        templateUrl: "app-new/mn-query-settings-config.html",
        inputs: [
          "group"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnQuerySettingsConfig.parameters = [
      mn.services.MnWizard
    ];

    return MnQuerySettingsConfig;

    function MnQuerySettingsConfig(mnWizardService) {
      this.querySettingsHttp = mnWizardService.stream.querySettingsHttp;
    }
  })();
