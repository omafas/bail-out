/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnGroupsGroupDialogController;

mnGroupsGroupDialogController.$inject = ["$uibModalInstance", "mnGroupsService", "mnPromiseHelper", "group"];
function mnGroupsGroupDialogController($uibModalInstance, mnGroupsService, mnPromiseHelper, group) {
  var vm = this;

  vm.isEditMode = !!group;
  vm.groupName = group ? group.name : "";
  vm.onSubmit = onSubmit;

  function onSubmit() {
    if (vm.viewLoading) {
      return;
    }

    var promise = vm.isEditMode ? mnGroupsService.updateGroup(vm.groupName, group.uri) :
        mnGroupsService.createGroup(vm.groupName);
    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors()
      .closeOnSuccess()
      .reloadState("app.admin.groups")
      .showGlobalSuccess("Group saved successfully!");
  }
}
