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
