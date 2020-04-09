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
