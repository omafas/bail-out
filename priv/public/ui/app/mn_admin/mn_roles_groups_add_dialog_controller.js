/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import _ from "lodash";

export default mnRolesGroupsAddDialogController;

mnRolesGroupsAddDialogController.$inject = ["mnUserRolesService", "$uibModalInstance", "mnPromiseHelper", "rolesGroup", "$state"];
function mnRolesGroupsAddDialogController(mnUserRolesService, $uibModalInstance, mnPromiseHelper, rolesGroup, $state) {
  var vm = this;
  vm.rolesGroup = _.clone(rolesGroup) || {};
  vm.rolesGroupID = vm.rolesGroup.id || 'New';
  vm.save = save;
  vm.isEditingMode = !!rolesGroup;
  vm.selectedRoles = {};

  vm.focusError = false;

  activate();

  function applyRoles(roles) {
    if (!roles.length) {
      return;
    }
    roles.forEach(role => {
      if (vm.state.rolesByRole[role.role].params.length) {
        vm.state.selectedRolesConfigs[role.role] =
          vm.state.selectedRolesConfigs[role.role] || [];
        vm.state.selectedRolesConfigs[role.role].push(
          vm.state.rolesByRole[role.role].params.map(param => role[param]).join(":")
        );
      } else {
        vm.state.selectedRoles[role.role] = true;
      }
    });
  }

  function activate() {
    mnUserRolesService.getRoles().then(resp => {
      resp.selectedRolesConfigs = {};
      resp.openedWrappers = {};
      resp.selectedRoles = {};
      vm.state = resp;
      if (vm.rolesGroup) {
        applyRoles(vm.rolesGroup.roles);
      }
    })
  }

  function save() {
    if (vm.form.$invalid) {
      vm.focusError = true;
      return;
    }

    var roles = mnUserRolesService.packRolesToSend(vm.state.selectedRoles,
                                                   vm.state.selectedRolesConfigs);


    mnPromiseHelper(vm, mnUserRolesService.addGroup(vm.rolesGroup, roles, vm.isEditingMode), $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors()
      .broadcast("reloadRolesGroupsPoller")
      .closeOnSuccess()
      .onSuccess(() => $state.go('app.admin.security.roles.groups'))
      .showGlobalSuccess("Group saved successfully!");
  }
}
