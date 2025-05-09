/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import template from "./mn_encryption_status.html";
import mnTimezoneDetailsDowngradeModule from "../../../mn.timezone.details.downgrade.module.js";

export default 'mnEncryptionStatus';

angular
  .module('mnEncryptionStatus', [mnTimezoneDetailsDowngradeModule])
  .directive('mnEncryptionStatus', mnEncryptionStatusDirective);

function mnEncryptionStatusDirective() {
  var mnEncryptionStatus = {
    restrict: 'E',
    require: ["mnEncryptionStatus"],
    scope: {
      encryptionInfo: "="
    },
    template,
    controller: ["$scope", "$element", "$attrs", "mnTimezoneDetailsServiceDowngrade", mnEncryptionStatusController],
    controllerAs: "vm",
    bindToController: true
  };
  return mnEncryptionStatus;

  function mnEncryptionStatusController($scope, $element, $attrs, mnTimezoneDetailsServiceDowngrade) {
    var vm = this;
    vm.getStatusLabel = getStatusLabel;
    vm.hasIssues = hasIssues;
    vm.mnTimezoneDetailsServiceDowngrade = mnTimezoneDetailsServiceDowngrade;

    function getStatusLabel(status) {
      switch (status) {
        case "encrypted": return "Fully Encrypted";
        case "partiallyEncrypted": return "Partially Encrypted";
        case "unencrypted": return "Not Encrypted";
        case "unknown": return "-";
        default: return status;
      }
    }

    function hasIssues() {
      return !!vm.encryptionInfo?.issues.length;
    }
  }
}
