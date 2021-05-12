/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnMultipleFailoverDialogController;

function mnMultipleFailoverDialogController(mnServersService, mnPromiseHelper, groups, nodes, $uibModalInstance, $uibModal, mnHelper, allowUnsafe) {
  var vm = this;

  vm.nodes = nodes[allowUnsafe ? "allNodes" : "reallyActive"];
  vm.onSubmit = onSubmit;
  vm.mnGroups = groups;
  vm.mnSelectedNodesHolder = {};
  vm.allowUnsafe = allowUnsafe;

  function doPostFailover(allowUnsafe) {
    var otpNodes = mnHelper.checkboxesToList(vm.mnSelectedNodesHolder);
    var promise = mnServersService.postFailover("startFailover", otpNodes, allowUnsafe);
    return mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors()
      .closeOnSuccess()
      .broadcast("reloadServersPoller");
  }

  function onSubmit() {
    if (allowUnsafe) {
      return doPostFailover(allowUnsafe);
    } else {
      return doPostFailover(allowUnsafe)
        .getPromise()
        .then(null, function (resp) {
          if (resp.status == 504) {
            $uibModalInstance.close();
            return $uibModal.open({
              templateUrl: 'app/mn_admin/mn_multiple_failover_dialog.html',
              controller: 'mnMultipleFailoverDialogController as multipleFailoverDialogCtl',
              resolve: {
                groups: mnHelper.wrapInFunction(groups),
                nodes: mnHelper.wrapInFunction(nodes),
                allowUnsafe: mnHelper.wrapInFunction(true)
              }
            });
          }
        });
    }
  }
}
