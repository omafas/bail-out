/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/
import mnUserRolesAddDialogTemplate from "./mn_user_roles_add_dialog.html";
import mnRolesGroupsAddDialogTemplate from "./mn_roles_groups_add_dialog.html";

export default mnRolesController;

mnRolesController.$inject = ["$scope", "poolDefault", "mnHelper", "$uibModal", "permissions", "mnUserRolesService", "mnPoller", "mnPromiseHelper"];
function mnRolesController($scope, poolDefault, mnHelper, $uibModal, permissions, mnUserRolesService, mnPoller, mnPromiseHelper) {
  var vm = this;
  vm.addUser = addUser;
  vm.addRolesGroup = addRolesGroup;

  activate();

  function activate() {
    if (poolDefault.isEnterprise && poolDefault.compat.atLeast65 &&
      (permissions.cluster.admin.security.external.read || permissions.cluster.admin.users.external.read)) {
      new mnPoller($scope, function () {
        return mnUserRolesService.getRbacStatus();
      })
        .subscribe(v => {
          vm.isLdapEnabled = v.data.ldapEnabled;
          vm.isSamlEnabled = v.data.samlEnabled;
          vm.isSaslauthdAuthEnabled = v.data.saslauthdEnabled;
        }, vm)
        .setInterval(10000)
        .reloadOnScopeEvent("reloadLdapSettings")
        .cycle();
    }
  }

  function addUser() {
    $uibModal.open({
      template: mnUserRolesAddDialogTemplate,
      controller: 'mnUserRolesAddDialogController as userRolesAddDialogCtl',
      resolve: {
        user: mnHelper.wrapInFunction(undefined),
        isSaslauthdAuthEnabled: mnHelper.wrapInFunction(vm.isSaslauthdAuthEnabled),
        isLdapEnabled: mnHelper.wrapInFunction(vm.isLdapEnabled),
        isSamlEnabled: mnHelper.wrapInFunction(vm.isSamlEnabled),
        permissions: mnHelper.wrapInFunction(permissions)
      }
    });
  }

  function addRolesGroup() {
    $uibModal.open({
      template: mnRolesGroupsAddDialogTemplate,
      controller: 'mnRolesGroupsAddDialogController as rolesGroupsAddDialogCtl',
      resolve: {
        rolesGroup: mnHelper.wrapInFunction(undefined),
        permissions: mnHelper.wrapInFunction(permissions)
      }
    });
  }

}
