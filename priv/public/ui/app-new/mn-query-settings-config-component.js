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
