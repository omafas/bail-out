/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "../../web_modules/angular.js";
import mnSpinner from "../components/directives/mn_spinner.js";
import mnCertificatesService from "./mn_certificates_service.js";
import mnHelper from "../components/mn_helper.js";

export default "mnCertificates";

angular
  .module("mnCertificates", [mnCertificatesService, mnSpinner, mnHelper])
  .controller("mnCertController", mnCertController);

function mnCertController($scope, mnCertificatesService, mnPromiseHelper, mnHelper) {
  var vm = this;
  vm.onSubmit = onSubmit;
  vm.reloadState = reloadState;

  activate();

  function onSubmit() {
    if ($scope.mnGlobalSpinnerFlag) {
      return;
    }

    mnPromiseHelper(vm, mnCertificatesService.postClientCertificateSettings(vm.clientCertSettings))
      .showGlobalSpinner()
      .catchErrors()
      .showGlobalSuccess("Client certificate settings saved successfully!");
  }

  function activate() {
    mnPromiseHelper(vm, mnCertificatesService.getClientCertificateSettings())
      .applyToScope("clientCertSettings")
      .onSuccess(function (resp) {
        if ($scope.rbac.cluster.admin.security.write && !vm.clientCertSettings.prefixes.length) {
          vm.clientCertSettings.prefixes.push({delimiter: '', prefix: '', path: 'subject.cn'});
        }
      });

    mnPromiseHelper(vm, mnCertificatesService.getDefaultCertificate())
      .applyToScope("rootCertificate");
  }

  function reloadState() {
    mnHelper.reloadState();
  }
}
