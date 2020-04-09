var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnWelcome =
  (function (Rx) {
    "use strict";

    MnWelcomeComponent.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-welcome.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnWelcomeComponent.parameters = [
      mn.services.MnAdmin
    ];

    return MnWelcomeComponent;

    function MnWelcomeComponent(mnAdmin) {
      this.focusFieldSubject = new Rx.BehaviorSubject(true);
      this.prettyVersion = mnAdmin.stream.prettyVersion;
    }
  })(window.rxjs);
