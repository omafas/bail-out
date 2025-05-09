/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnGroupsDeleteDialogController;

mnGroupsDeleteDialogController.$inject = ["$uibModalInstance", "mnGroupsService", "mnPromiseHelper", "group"];
function mnGroupsDeleteDialogController($uibModalInstance, mnGroupsService, mnPromiseHelper, group) {
  var vm = this;

  vm.onSubmit = onSubmit;

  function onSubmit() {
    if (vm.viewLoading) {
      return;
    }

    var promise = mnGroupsService.deleteGroup(group.uri);
    mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors()
      .closeFinally()
      .reloadState("app.admin.groups")
      .showGlobalSuccess("Group deleted successfully!");
  }
}
