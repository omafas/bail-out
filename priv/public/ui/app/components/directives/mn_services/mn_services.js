/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import mnMemoryQuotaService from '../mn_memory_quota/mn_memory_quota_service.js';
import template from "./mn_services.html";

export default 'mnServices';

angular
  .module('mnServices', [mnMemoryQuotaService])
  .directive('mnServices', mnServicesDirective);

function mnServicesDirective() {
  var mnServices = {
    restrict: 'A',
    scope: {
      mnIsDisabled: "=?",
      config: '=mnServices',
      mnIsEnterprise: "="
    },
    template,
    controller: ["mnMemoryQuotaService", controller],
    controllerAs: "mnServicesCtl",
    bindToController: true
  };

  return mnServices;
}

function controller(mnMemoryQuotaService) {
  var vm = this;
  vm.change = mnMemoryQuotaService.handleAltAndClick;
}
