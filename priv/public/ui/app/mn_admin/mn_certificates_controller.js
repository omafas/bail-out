/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import mnSpinner from "../components/directives/mn_spinner.js";
import mnCertificatesService from "./mn_certificates_service.js";
import mnHelper from "../components/mn_helper.js";
import uiBootstrap from "../../web_modules/angular-ui-bootstrap.js";
import mnCertificatesDeleteDialogController from "./mn_certificates_delete_dialog_controller.js";
import mnPoll from "../components/mn_poll.js";

export default "mnCertificates";

angular
  .module("mnCertificates", [mnCertificatesService, mnSpinner, mnHelper, uiBootstrap, mnPoll])
  .controller("mnCertController", ["$scope", "mnCertificatesService", "mnPromiseHelper", "mnHelper", "$uibModal", "mnPoller", mnCertController])
  .controller('mnCertificatesDeleteDialogController', mnCertificatesDeleteDialogController)

function mnCertController($scope, mnCertificatesService, mnPromiseHelper, mnHelper, $uibModal, mnPoller) {
  var vm = this;
  vm.onSubmit = onSubmit;
  vm.reloadState = reloadState;
  vm.showDeleteConfirmation = showDeleteConfirmation;

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
      .onSuccess(function () {
        if ($scope.rbac.cluster.admin.security.write && !vm.clientCertSettings.prefixes.length) {
          vm.clientCertSettings.prefixes.push({delimiter: '', prefix: '', path: 'subject.cn'});
        }
      });

    new mnPoller($scope, function () {
      return mnCertificatesService.getPoolsDefaultTrustedCAs();
    })
      .subscribe("rootCertificate", vm)
      .reloadOnScopeEvent("reloadGetPoolsDefaultTrustedCAs")
      .cycle();

    new mnPoller($scope, function () {
      return mnCertificatesService.getNodeCertificateSettings();
    })
      .subscribe("nodeCertSettings", vm)
      .cycle();
  }

  function showDeleteConfirmation(id) {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_certificates_delete_dialog.html',
      controller: 'mnCertificatesDeleteDialogController as certDeleteDialogCtrl',
      resolve: {
        id: mnHelper.wrapInFunction(id)
      }
    });
  }

  function reloadState() {
    mnHelper.reloadState();
  }
}
