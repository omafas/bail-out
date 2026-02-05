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
