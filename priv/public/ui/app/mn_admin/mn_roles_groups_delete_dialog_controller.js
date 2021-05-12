/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnRolesGroupsDeleteDialogController;

function mnRolesGroupsDeleteDialogController(mnUserRolesService, rolesGroup, mnPromiseHelper, $uibModalInstance) {
  var vm = this;
  vm.grolesGroupsId = rolesGroup.id;
  vm.onSubmit = onSubmit;

  function onSubmit() {
    mnPromiseHelper(vm, mnUserRolesService.deleteRolesGroup(rolesGroup), $uibModalInstance)
      .showGlobalSpinner()
      .closeFinally()
      .broadcast("reloadRolesGroupsPoller")
      .showGlobalSuccess("Group deleted successfully!");
  }
}
