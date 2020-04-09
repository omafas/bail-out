var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnAuth =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnAuthComponent, mn.core.MnEventableComponent);

    MnAuthComponent.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-auth.html",
      })
    ];

    MnAuthComponent.parameters = [
      mn.services.MnForm,
      mn.services.MnAuth,
      window['@uirouter/angular'].UIRouter,
    ];

    return MnAuthComponent;

    function MnAuthComponent(mnFormService, mnAuthService, uiRouter) {
      mn.core.MnEventableComponent.call(this);

      this.focusFieldSubject = new Rx.BehaviorSubject(true);
      this.postUILogin = mnAuthService.stream.postUILogin;

      this.form = mnFormService.create(this)
        .setFormGroup({
          user: ['', ng.forms.Validators.required],
          password: ['', ng.forms.Validators.required]})
        .setPostRequest(this.postUILogin)
        .success(uiRouter.urlRouter.sync.bind(uiRouter.urlRouter));
    }

  })(window.rxjs);
