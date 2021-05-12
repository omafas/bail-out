/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnUserRolesResetPasswordDialogController;

function mnUserRolesResetPasswordDialogController(mnUserRolesService, $uibModalInstance, mnPromiseHelper, user) {
  var vm = this;

  vm.user = user;
  vm.userID = vm.user.id;
  vm.save = save;

  function save() {
    if (vm.form.$invalid) {
      return;
    }
    mnPromiseHelper(vm, mnUserRolesService.addUser(
      vm.user,
      vm.user.roles.map(function (role) {
        if (role.bucket_name) {
          let params = ([role.bucket_name,
                         role.scope_name,
                         role.collection_name]).filter(v => v);
          return role.role + '[' + mnUserRolesService.packRoleParams(params) + ']';
        } else {
          return role.role;
        }
      }),
      vm.user.groups,
      true,
      true
    ), $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors()
      .broadcast("reloadRolesPoller")
      .closeOnSuccess()
      .showGlobalSuccess("Password reset successfully!");
  }
}
