/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import mnFocus from '../mn_focus.js';
import mnServices from '../mn_services/mn_services.js';

export default 'mnMemoryQuota';

angular
  .module('mnMemoryQuota', [mnServices, mnFocus])
  .directive('mnMemoryQuota', mnMemoryQuotaDirective);

function mnMemoryQuotaDirective() {
  var mnMemoryQuota = {
    restrict: 'A',
    scope: {
      config: '=mnMemoryQuota',
      errors: "=",
      rbac: "=",
      mnIsEnterprise: "="
    },
    templateUrl: 'app/components/directives/mn_memory_quota/mn_memory_quota.html',
    controller: ["$scope", controller]
  };

  return mnMemoryQuota;

  function controller($scope) {
    //hack for avoiding access to $parent scope from child scope via propery "$parent"
    //should be removed after implementation of Controller As syntax
    $scope.mnMemoryQuotaController = $scope;

    $scope.calculateTotalQuota = calculateTotalQuota;

    function getServiceFieldName(service) {
      switch (service) {
      case "kv": return "memoryQuota";
      default: return (service + "MemoryQuota");
      }
    }

    function calculateTotalQuota() {
      return Object
        .keys($scope.config.services.model)
        .reduce(function (total, service) {
          var cfg = $scope.config;
          var fieldName = getServiceFieldName(service);

          if (cfg.displayedServices[service] &&
              cfg.services && cfg.services.model[service] &&
              cfg[fieldName]) {
            return total + (Number(cfg[fieldName]) || 0);
          } else {
            return total;
          }

        }, 0);
    }
  }
}
