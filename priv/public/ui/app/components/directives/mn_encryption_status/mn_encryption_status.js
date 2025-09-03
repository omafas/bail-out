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
    scope: {
      encryptionInfo: "=",
      encryptionSettings: "=",
      itemTypeLabel: "@",
      itemType: "@" // 'config' | 'audit' | 'logs'
    },
    template,
    controller: ["$scope", "$element", "$attrs", "$uibModal", "$uibTooltip","mnServersService", "mnTimezoneDetailsServiceDowngrade", mnEncryptionStatusController],
    controllerAs: "vm",
    bindToController: true
  };
  return mnEncryptionStatus;

  function mnEncryptionStatusController($scope, $element, $attrs, $uibModal, $uibTooltip, mnServersService, mnTimezoneDetailsServiceDowngrade) {
    var vm = this;
    vm.getStatusLabel = getStatusLabel;
    vm.hasIssues = hasIssues;
    vm.mnTimezoneDetailsServiceDowngrade = mnTimezoneDetailsServiceDowngrade;
    vm.forceEncryption = forceEncryption;

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

    function forceEncryption() {
      // Close any open uib tooltips
      let tt = angular.element(document.querySelectorAll(".tooltip"));
      Array.from(tt).forEach(e => e.remove());

      var modalInstance = openConfirmationModal();
      // Handle modal result
      modalInstance.result.then(function(result) {
        if (result === 'ok') {
          // User confirmed, call the service
          mnServersService.forceEncryption(vm.itemType)
            .then(function(response) {
            })
            .catch(function(error) {
            });
        }
      }, function(dismissReason) {
        // User cancelled or dismissed the modal
      });
    }

    function openConfirmationModal() {
      const isEncryptionEnabled = vm.encryptionSettings[vm.itemType].encryptionMethod !== 'disabled';

      return $uibModal.open({
        template: `
          <div class="dialog-med">
            <h3 class="panel-header">Confirm Force Encryption</h3>
            <div class="panel-content">
              <div class="row flex-left">
                <span class="icon fa-warning fa-2x red-3"></span>
                <p>
                  Are you sure you want to fully ${isEncryptionEnabled ? 'encrypt' : 'decrypt'} the ${vm.itemType}?
                </p>
              </div>
            </div>
            <div class="panel-footer">
              <button class="btn btn-default" type="button" ng-click="vm.cancel()">Cancel</button>
              <button class="btn btn-primary" type="button" ng-click="vm.ok()">OK</button>
            </div>
          </div>
        `,
        controller: ['$scope', '$uibModalInstance', function($scope, $uibModalInstance) {
          $scope.vm = {
            itemTypeLabel: vm.itemTypeLabel,
            ok: function() {
              $uibModalInstance.close('ok');
            },
            cancel: function() {
              $uibModalInstance.dismiss('cancel');
            }
          };
        }],
        size: 'md',
        backdrop: true,  // Ensure backdrop is enabled
        keyboard: true   // Ensure keyboard navigation is enabled
      });
    }
  }
}
