/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import memoryQuotaDialogTemplate from "./memory_quota_dialog.html";

export default mnServersModifyServicesController;

mnServersModifyServicesController.$inject = ["$scope", "$rootScope", "$q", "$uibModal", "$ocLazyLoad", "mnServersService", "$uibModalInstance", "mnHelper", "mnPromiseHelper","nodes","mnClusterConfigurationService", "mnPoolDefault", "mnCertificatesService"];
function mnServersModifyServicesController($scope, $rootScope, $q, $uibModal, $ocLazyLoad, mnServersService, $uibModalInstance, mnHelper, mnPromiseHelper, nodes, mnClusterConfigurationService, mnPoolDefault, mnCertificatesService) {
  var vm = this;

  vm.specifyDisk = false;

  vm.modifyServicesConfig = {
    services: {
      model: {
        kv: true,
        index: true,
        n1ql: true,
        fts: true
      }
    },
    credentials: {
      hostname: '',
      user: 'Administrator',
      password: ''
    }
  };
  if ($scope.poolDefault.isEnterprise) {
    vm.modifyServicesConfig.services.model.cbas = true;
    vm.modifyServicesConfig.services.model.eventing = true;
    vm.modifyServicesConfig.services.model.backup = true;
  }
  vm.onSubmit = onSubmit;
  vm.newServicesAdded = newServicesAdded;

  vm.isNodesAvailable = !!nodes;
  if (vm.isNodesAvailable) {
    vm.nodes = nodes;
    vm.nodeServices = getServicesArray().sort((a, b) => a.name.localeCompare(b.name));
    vm.nodeServicesOriginal = getServicesArray().sort((a, b) => a.name.localeCompare(b.name));
    vm.unhealthyNodes = nodes.allNodes.some(node => node.status !== 'healthy');
    if (vm.unhealthyNodes) {
      vm.errors = ['Services may not be modified while there are unhealthy nodes in the cluster.'];
    }
  }

  activate();

  function getServicesArray() {
    return nodes.allNodes.map(node => {
      let services = {};
      node.services.forEach(serviceName => {
        services[serviceName] = true;
      });
      return {
        name: node.configuredHostname,
        otpNode: node.otpNode,
        status: node.status,
        services: services,
      }
    });
  }

  // flatten an array of nodes' services into a single service array
  function flattenServices(nodeServices) {
    let services = {};
    nodeServices.forEach(node => {
      for (let service in node.services) {
        if (service !== 'backup' && node.services[service]) services[service] = true;
      }
    });
    return services;
  }

  // Determine if the user is adding services that were not present before,
  function newServicesAdded() {
    let oldServices = flattenServices(vm.nodeServicesOriginal);
    let newServices = flattenServices(vm.nodeServices);

    let addedServiceArray = Object.keys(newServices).filter(service => !oldServices[service]);
    let firstTimeAddedServices = {count: addedServiceArray.length};
    addedServiceArray.forEach(service => firstTimeAddedServices[service] = true);
    return firstTimeAddedServices;
  }

  function activate() {
    reset();
  }
  function reset() {
    vm.focusMe = true;
  }

  async function showMemoryQuotaDialog(addedServices) {
    await import("./memory_quota_dialog_controller.js");
    await $ocLazyLoad.load({name: 'mnMemoryQuotaDialogController'});
    return $uibModal.open({
      backdrop: 'static',
      template: memoryQuotaDialogTemplate,
      controller: 'mnMemoryQuotaDialogController as memoryQuotaDialogCtl',
      resolve: {
        memoryQuotaConfig: ['mnMemoryQuotaService', function (mnMemoryQuotaService) {
          return mnMemoryQuotaService.memoryQuotaConfig(flattenServices(vm.nodeServices), true, false);
        }],
        indexSettings: ['mnSettingsClusterService', function (mnSettingsClusterService) {
          return mnSettingsClusterService.getIndexSettings();
        }],
        firstTimeAddedServices: function() {
          return addedServices;
        },
      }
    }).result;

  }

  function onSubmit(form) {
    if (vm.viewLoading) {
      return;
    }
    // the rebalance endpoint needs a list of knownNodes and the name of each node running each service
    let config = {
      knownNodes: vm.nodeServices.map(node => node.otpNode).join(","),
      topology: {
        backup: '',
        cbas: '',
        eventing: '',
        fts: '',
        index: '',
        n1ql: '',
      }
    }

    for (let service in config.topology) {
      config.topology[service] = vm.nodeServices.filter(node => node.services[service]).map(node => node.otpNode).join(",");
    }

    // if new services are being added, show the memory quota dialog first
    let firstTimeAddedServices = newServicesAdded();
    let quotaDialogPromise = Promise.resolve();
    if (firstTimeAddedServices.count > 0) {
      quotaDialogPromise = showMemoryQuotaDialog(firstTimeAddedServices);
    }
    // now change the services
    quotaDialogPromise
        .then(() => {/* do nothing */},
              () => {/* needed if they close dialog without saving */})
        .finally(() => {
      let promise = mnServersService.modifyServices(config);

      mnPromiseHelper(vm, promise, $uibModalInstance)
          .showGlobalSpinner()
          .catchErrors()
          .closeOnSuccess()
          .broadcast("reloadServersPoller")
          .showGlobalSuccess("Cluster services modified, rebalance in progress.");
    });
  }

}
