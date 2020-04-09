var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnRootCertificate =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnRootCertificate, mn.core.MnEventableComponent);

    MnRootCertificate.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-root-certificate.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnRootCertificate.parameters = [
      mn.services.MnSecurity,
      mn.services.MnForm
    ];

    return MnRootCertificate;

    function MnRootCertificate(mnSecurityService, mnFormService) {
      mn.core.MnEventableComponent.call(this);

      this.cert = mnSecurityService.stream.getCertificate;

      this.form = mnFormService.create(this)
        .setFormGroup({pem: ""})
        .setSource(this.cert.pipe(Rx.operators.pluck("cert")));
    }

  })(window.rxjs);
