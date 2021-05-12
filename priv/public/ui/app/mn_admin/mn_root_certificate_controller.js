/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";

import mnRootCertificateService from "./mn_root_certificate_service.js";

export default "mnRootCertificate";

angular
  .module("mnRootCertificate", [
    mnRootCertificateService,
    mnPromiseHelper,
    mnSpinner,
    mnMainSpinner
  ])
  .controller("mnRootCertificateController", mnRootCertificateController);

function mnRootCertificateController(mnRootCertificateService, mnPromiseHelper) {
  var vm = this;

  activate();

  function activate() {
    mnPromiseHelper(vm, mnRootCertificateService.getDefaultCertificate())
      .applyToScope("certificate");
  }
}
