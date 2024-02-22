/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import _ from 'lodash';
import uiBootstrap from 'angular-ui-bootstrap';

import mnPoolDefault from '../components/mn_pool_default.js';
import mnPromiseHelper from "../components/mn_promise_helper.js";
import mnSettingsClusterService from "./mn_settings_cluster_service.js";
import mnMemoryQuota from "../components/directives/mn_memory_quota/mn_memory_quota.js";
import mnStorageMode from "../components/directives/mn_storage_mode/mn_storage_mode.js";

export default 'mnMemoryQuotaDialogController';

angular.module('mnMemoryQuotaDialogController', [
  uiBootstrap,
  mnPoolDefault,
  mnPromiseHelper,
  mnSettingsClusterService,
  mnMemoryQuota,
  mnStorageMode
])
  .controller('mnMemoryQuotaDialogController', ["$q", "$uibModalInstance", "mnPoolDefault", "mnPromiseHelper", "mnSettingsClusterService", "indexSettings", "memoryQuotaConfig", "firstTimeAddedServices", mnMemoryQuotaDialogController]);

function mnMemoryQuotaDialogController($q, $uibModalInstance, mnPoolDefault, mnPromiseHelper, mnSettingsClusterService, indexSettings, memoryQuotaConfig, firstTimeAddedServices) {
  var vm = this;
  vm.config = memoryQuotaConfig;
  vm.isEnterprise = mnPoolDefault.latestValue().value.isEnterprise;
  vm.onSubmit = onSubmit;

  vm.initialIndexSettings = _.clone(indexSettings);
  vm.indexSettings = indexSettings;
  vm.firstTimeAddedServices = firstTimeAddedServices;
  vm.getFirstTimeServiceNames = getFirstTimeServiceNames;

  if (indexSettings.storageMode === "") {
    vm.indexSettings.storageMode = vm.isEnterprise ? "plasma" : "forestdb";
  }

  function onSubmit() {
    if (vm.viewLoading) {
      return;
    }

    var queries = [
      mnPromiseHelper(vm, mnSettingsClusterService.postPoolsDefault(vm.config))
        .catchErrors()
        .getPromise()
    ];

    if (vm.firstTimeAddedServices.index) {
      queries.push(
        mnPromiseHelper(vm, mnSettingsClusterService.postIndexSettings(vm.indexSettings))
          .catchErrors("postIndexSettingsErrors")
          .getPromise()
      );
    }
    var promise = $q.all(queries);

    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .closeOnSuccess()
      .showGlobalSuccess("Memory quota saved successfully!");
  }

  function getFirstTimeServiceNames() {
    var services = [];
    if (firstTimeAddedServices.index)
      services.push("GSI Index");
    if (firstTimeAddedServices.fts)
      services.push("Full Text");
    if (firstTimeAddedServices.cbas)
      services.push("Analytics");
    if (firstTimeAddedServices.eventing)
      services.push("Eventing");

    return services;
  }
}
