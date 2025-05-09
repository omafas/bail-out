/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnGroupDialogController;

mnGroupDialogController.$inject = ["$rootScope", "$uibModalInstance", "mnUserRolesService", "mnPromiseHelper", "scenarioId", "mnStoreService"];
function mnGroupDialogController($rootScope, $uibModalInstance, mnUserRolesService, mnPromiseHelper, scenarioId, mnStoreService) {
  var vm = this;
  vm.group = {
    name: "",
    desc: "",
    charts: []
  };

  vm.submit = submit;

  function submit() {
    var group = mnStoreService.store("groups").add(vm.group);
    mnStoreService.store("scenarios")
      .share()
      .find(scenario => scenario.id === scenarioId)
      .groups.push(group.id);

    mnPromiseHelper(vm, mnUserRolesService.saveDashboard())
      .showGlobalSpinner()
      .showGlobalSuccess("Group added successfully!")
      .onSuccess(function () {
        $rootScope.$broadcast("scenariosChanged");
        $uibModalInstance.close(group);
      });
  }
}
