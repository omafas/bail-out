var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnXDCRSettings =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnXDCRSettings, mn.core.MnEventableComponent);

    MnXDCRSettings.annotations = [
      new ng.core.Component({
        selector: "mn-xdcr-settings",
        templateUrl: "app-new/mn-xdcr-settings.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush,
        inputs: [
          "group",
          "type"
        ]
      })
    ];

    MnXDCRSettings.parameters = [
      mn.services.MnXDCR,
      mn.services.MnPools,
      mn.services.MnAdmin
    ];

    return MnXDCRSettings;

    function MnXDCRSettings(mnXDCRService, mnPoolsService, mnAdminService) {
      mn.core.MnEventableComponent.call(this);

      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.compatVersion55 = mnAdminService.stream.compatVersion55;
      this.error = mnXDCRService.stream.postSettingsReplicationsValidation.error;
    }

  })(window.rxjs);
