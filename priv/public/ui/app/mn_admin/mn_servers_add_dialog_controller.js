/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnServersAddDialogController;

mnServersAddDialogController.$inject = ["$scope", "$rootScope", "$q", "$uibModal", "mnServersService", "$uibModalInstance", "mnHelper", "mnPromiseHelper", "groups", "mnClusterConfigurationService", "mnPoolDefault", "mnCertificatesService"];
function mnServersAddDialogController($scope, $rootScope, $q, $uibModal, mnServersService, $uibModalInstance, mnHelper, mnPromiseHelper, groups, mnClusterConfigurationService, mnPoolDefault, mnCertificatesService) {
  var vm = this;

  vm.specifyDisk = false;

  vm.addNodeConfig = {
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
    vm.addNodeConfig.services.model.cbas = true;
    vm.addNodeConfig.services.model.eventing = true;
    vm.addNodeConfig.services.model.backup = true;
  }
  vm.isGroupsAvailable = !!groups;
  vm.onSubmit = onSubmit;
  vm.onSelectGroup = onSelectGroup;

  if (vm.isGroupsAvailable) {
    vm.addNodeConfig.selectedGroup = groups.groups[0];
    vm.selectedGroupName = vm.addNodeConfig.selectedGroup.name;
    vm.groups = groups.groups;
    vm.groupNames = vm.groups.map( (group) => group.name);
  }

  activate();

  function activate() {
    reset();
    if ($scope.poolDefault.isEnterprise) {
      mnPromiseHelper(vm, mnCertificatesService.getPoolsDefaultTrustedCAs())
        .applyToScope("certificate");
    }
    mnClusterConfigurationService.getSelfConfig().then(function (selfConfig) {
      var rv = {};
      rv.selfConfig = selfConfig;
      if ($scope.poolDefault.isEnterprise) {
        rv.cbasDirs = selfConfig.storage.hdd[0].cbas_dirs;
      }
      rv.dbPath = selfConfig.storage.hdd[0].path;
      rv.indexPath = selfConfig.storage.hdd[0].index_path;
      rv.eventingPath = selfConfig.storage.hdd[0].eventing_path;
      vm.selfConfig = rv;
    });
  }
  function postDiskStorage(resp) {
    if (resp && resp.data) {
      vm.optNode = resp.data.otpNode;
    }
    var data = {
      path: vm.selfConfig.dbPath,
      index_path: vm.selfConfig.indexPath
    };
    data.eventing_path = vm.selfConfig.eventingPath;
    if ($scope.poolDefault.isEnterprise) {
      data.cbas_path = vm.selfConfig.cbasDirs;
    }
    var promise = mnClusterConfigurationService.postDiskStorage(data, vm.optNode);
    return mnPromiseHelper(vm, promise)
      .catchErrors('postDiskStorageErrors')
      .getPromise();
  }
  function reset() {
    vm.focusMe = true;
  }
  function onSubmit(form) {
    if (vm.viewLoading) {
      return;
    }

    var servicesList = mnHelper.checkboxesToList(vm.addNodeConfig.services.model);

    form.$setValidity('services', !!servicesList.length);

    if (form.$invalid) {
      return reset();
    }
    var promise;
    if (vm.postDiskStorageErrors) {
      if (vm.specifyDisk) {
        promise = postDiskStorage();
      } else {
        $uibModalInstance.close();
      }
    } else {
      promise = mnServersService
        .addServer(vm.addNodeConfig.selectedGroup,
                   vm.addNodeConfig.credentials,
                   servicesList);
      if (vm.specifyDisk) {
        promise = promise.then(postDiskStorage);
      }
    }

    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors()
      .closeOnSuccess()
      .broadcast("reloadServersPoller")
      .broadcast("maybeShowMemoryQuotaDialog", vm.addNodeConfig.services.model)
      .showGlobalSuccess("Server added successfully!");
  }
  function onSelectGroup(selectedOption) {
    vm.addNodeConfig.selectedGroup = vm.groups.find((group) => group.name === selectedOption);
  }
}
