/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import _ from "lodash";

export default mnUserRolesAddDialogController;

mnUserRolesAddDialogController.$inject = ["mnUserRolesService", "$uibModalInstance", "mnPromiseHelper", "user", "isLdapEnabled", "mnPoolDefault", "mnHelper", "$q", "isSaslauthdAuthEnabled", "$state", "permissions"];
function mnUserRolesAddDialogController(mnUserRolesService, $uibModalInstance, mnPromiseHelper, user, isLdapEnabled, mnPoolDefault, mnHelper, $q, isSaslauthdAuthEnabled, $state, permissions) {
  var vm = this;
  vm.user = _.clone(user) || {
    domain: permissions.cluster.admin.security.local.write ? "local" : "external"
  };
  vm.userID = vm.user.id || 'New';
  vm.save = save;
  vm.isEditingMode = !!user;
  vm.isLdapEnabled = isLdapEnabled;
  vm.isSaslauthdAuthEnabled = isSaslauthdAuthEnabled;
  vm.selectedRoles = {};
  vm.selectedGroupsRoles = {};
  vm.selectedGroups = {};
  vm.state = {};

  vm.focusError = false;
  vm.selectedPanel = "roles";
  vm.lookupMembership =  _.debounce(lookupMembership, 500, {leading: true});
  vm.onDomainChanged = onDomainChanged;
  vm.isLookupEnabled = isLookupEnabled;
  vm.onGroupChanged = onGroupChanged;
  vm.getGroupTitle = getGroupTitle;

  activate();

  function onGroupChanged(group) {
    group.roles.forEach(selectGroupsRoles(vm.selectedGroups[group.id], group));
  }

  function getGroupTitle(roles) {
    if (!vm.state.rolesByRole) {
      return;
    }
    return roles && roles.map(function (v) {
      let role = vm.state.rolesByRole[v.role];
      return role.name + (role.params.length ? "[" + role.params.map(param => v[param]).join(":") + "]" : "");
    }).join(', ');
  }

  function onDomainChanged() {
    if (vm.user.domain === "external" && isLdapEnabled) {
      lookupMembership();
    } else {
      clearRoles();
    }
  }

  function lookupMembership() {
    vm.reloadUserRoles = true;
    mnUserRolesService.lookupLDAPUser(vm.user)
      .then(function (user) {
        clearRoles();
        applyUser(user.data);
        vm.isUserAvailable = true;
        vm.reloadUserRoles = false;
      }, function () {
        clearRoles();
        vm.isUserAvailable = false;
        vm.reloadUserRoles = false;
      });
  }

  function isLookupEnabled() {
    return isLdapEnabled && !vm.isEditingMode && (vm.user.domain === 'external');
  }

  function clearRoles() {
    vm.state.selectedRoles = {};
    vm.state.selectedGroupsRoles = {};
    vm.state.selectedRolesConfigs = {};
    vm.state.selectedGroupsRolesConfigs = {};
    vm.selectedGroups = {};
    vm.externalGroups = {};
  }


  function getRolesByType(user, type) {
    return user.roles.filter(function (role) {
      return role.origins.find(function (origin) {
        return origin.type == type;
      });
    });
  }

  function groupsToObject(groups) {
    return groups.reduce(function (acc, group) {
      acc[group] = true;
      return acc;
    }, {});
  }

  function activate() {
    vm.reloadUserRoles = true;
    mnUserRolesService.getRoles().then(resp => {
      resp.selectedRolesConfigs = {};
      resp.openedWrappers = {};
      resp.selectedRoles = {};
      resp.selectedGroupsRoles = {};
      resp.selectedGroupsRolesConfigs = {};
      if (!permissions.cluster.admin.security.admin.write) {
        let administ = resp.folders.find(f => f.name == "Administrative");
        if (administ) {
          administ.roles = administ.roles.filter(r => !r.role.includes("security_admin_") &&
                                                 r.role !== "admin" && r.role !== "ro_admin");
        }
      }
      vm.state = resp;
      applyUser(vm.user);
      vm.reloadUserRoles = false;
    }, () => {
      vm.reloadUserRoles = false;
    });

    if (mnPoolDefault.export.isEnterprise &&
        mnPoolDefault.export.compat.atLeast65) {
      mnUserRolesService.getRolesGroups().then(resp => {
        vm.groups = resp.data;
      });
    }
  }

  function applyUser(user) {
    vm.selectedGroups = groupsToObject(user.groups || []);
    vm.externalGroups = groupsToObject(user.external_groups || []);

    if (user.roles) {
      getRolesByType(user, "user").forEach(role => {
        if (vm.state.rolesByRole[role.role].params.length) {
          vm.state.selectedRolesConfigs[role.role] =
            vm.state.selectedRolesConfigs[role.role] || [];

          vm.state.selectedRolesConfigs[role.role].push(
            mnUserRolesService.getRoleParams(vm.state.rolesByRole, role)
          );
        } else {
          vm.state.selectedRoles[role.role] = true;
        }
      });

      let addRole = selectGroupsRoles(true);
      getRolesByType(user, "group").forEach(addRole);
    }
  }

  function doSelectGroupsRoles(holder, origins, group) {
    holder = holder || [];
    if (origins) {
      origins.forEach(group => {
        if (group.type == "group") {
          holder.push(group.name);
        }
      });
    } else {
      holder.push(group.id);
    }

    return holder;
  }

  function selectGroupsRoles(flag, group) {
    return function (role) {
      if (flag) {
        if (vm.state.rolesByRole[role.role].params.length) {
          let params = mnUserRolesService.getRoleParams(vm.state.rolesByRole, role);
          vm.state.selectedGroupsRolesConfigs[role.role] =
            vm.state.selectedGroupsRolesConfigs[role.role] || {};

          vm.state.selectedGroupsRolesConfigs[role.role][params] =
            doSelectGroupsRoles(vm.state.selectedGroupsRolesConfigs[role.role][params],
                                role.origins, group);
        } else {
          vm.state.selectedGroupsRoles[role.role] =
            doSelectGroupsRoles(vm.state.selectedGroupsRoles[role.role],
                                role.origins, group);
        }
      } else {
        if (vm.state.rolesByRole[role.role].params.length) {
          let params = mnUserRolesService.getRoleParams(vm.state.rolesByRole, role);
          let holder = vm.state.selectedGroupsRolesConfigs[role.role][params];
          holder.splice(holder.indexOf(group.id), 1);
          if (!holder.length) {
            delete vm.state.selectedGroupsRolesConfigs[role.role][params];
          }
        } else {
          let holder = vm.state.selectedGroupsRoles[role.role];
          holder.splice(holder.indexOf(group.id), 1);
        }
      }
    }
  }

  function save() {
    if (vm.form.$invalid) {
      vm.focusError = true;
      return;
    }

    var roles = mnUserRolesService.packRolesToSend(vm.state.selectedRoles,
                                                   vm.state.selectedRolesConfigs);

    mnPromiseHelper(vm, mnUserRolesService.addUser(
      vm.user, roles, mnHelper.checkboxesToList(vm.selectedGroups), vm.isEditingMode,
    ), $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors(function (errors) {
        vm.focusError = !!errors;
        vm.errors = errors;
      })
      .broadcast("reloadRolesPoller")
      .closeOnSuccess()
      .onSuccess(() => $state.go('app.admin.security.roles.user'))
      .showGlobalSuccess("User saved successfully!");
  }
}
