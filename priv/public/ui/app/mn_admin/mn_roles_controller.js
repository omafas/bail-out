/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnRolesController;

mnRolesController.$inject = ["$scope", "poolDefault", "mnHelper", "$uibModal", "permissions", "mnUserRolesService", "mnPoller", "mnPromiseHelper"];
function mnRolesController($scope, poolDefault, mnHelper, $uibModal, permissions, mnUserRolesService, mnPoller, mnPromiseHelper) {
  var vm = this;
  vm.addUser = addUser;
  vm.addRolesGroup = addRolesGroup;
  vm.addLDAP = addLDAP;

  activate();


  function activate() {
    if (poolDefault.saslauthdEnabled) {
      mnPromiseHelper(vm, mnUserRolesService.getSaslauthdAuth())
        .applyToScope(v => vm.isSaslauthdAuthEnabled = v.enabled);
    }

    if (permissions.cluster.admin.security.external.read &&
        poolDefault.compat.atLeast65 && poolDefault.isEnterprise) {
      new mnPoller($scope, function () {
        return mnUserRolesService.getLdapSettings();
      })
        .subscribe(v => vm.isLdapEnabled = v.data.authenticationEnabled, vm)
        .setInterval(10000)
        .reloadOnScopeEvent("reloadLdapSettings")
        .cycle();
    }
  }

  function addUser() {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_user_roles_add_dialog.html',
      controller: 'mnUserRolesAddDialogController as userRolesAddDialogCtl',
      resolve: {
        user: mnHelper.wrapInFunction(undefined),
        isSaslauthdAuthEnabled: mnHelper.wrapInFunction(vm.isSaslauthdAuthEnabled),
        isLdapEnabled: mnHelper.wrapInFunction(vm.isLdapEnabled),
        permissions: mnHelper.wrapInFunction(permissions)
      }
    });
  }

  function addLDAP() {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_add_ldap_dialog.html',
      controller: 'mnAddLDAPDialogController as addLdapDialogCtl'
    });
  }

  function addRolesGroup() {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_roles_groups_add_dialog.html',
      controller: 'mnRolesGroupsAddDialogController as rolesGroupsAddDialogCtl',
      resolve: {
        rolesGroup: mnHelper.wrapInFunction(undefined)
      }
    });
  }

}
