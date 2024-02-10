/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/
import template from "./mn_servers_failover_confirmation_dialog.html";

export default mnServersFailOverDialogController;

mnServersFailOverDialogController.$inject = ["mnServersService", "mnPromiseHelper", "node", "$uibModalInstance", "$uibModal"];
function mnServersFailOverDialogController(mnServersService, mnPromiseHelper, node, $uibModalInstance, $uibModal) {
  var vm = this;

  vm.node = node;
  vm.onSubmit = onSubmit;
  vm.isFailOverBtnDisabled = isFailOverBtnDisabled;

  activate();

  function isFailOverBtnDisabled() {
    return !vm.status || !vm.status.confirmation &&
      (vm.status.failOver === 'startFailover') &&
      !(vm.status.down && !vm.status.backfill) && !vm.status.dataless;
  }

  function doPostFailover(allowUnsafe) {
    var promise = mnServersService.postFailover(vm.status.failOver, node.otpNode, allowUnsafe);
    return mnPromiseHelper(vm, promise, $uibModalInstance)
      .showGlobalSpinner()
      .catchGlobalErrors(null, allowUnsafe ? null : 10000)
      .closeFinally()
      .broadcast("reloadServersPoller");
  }

  function onSubmit() {
    doPostFailover()
      .getPromise()
      .then(null, function (resp) {
        if (resp.status == 504) {
          return $uibModal.open({
            template,
          }).result.then(function () {
            return doPostFailover(true);
          });
        }
      });
  }
  function activate() {
    vm.isEventingNode = node.services.includes("eventing");

    mnPromiseHelper(vm, mnServersService.getNodeStatuses(node.hostname))
      .showSpinner()
      .getPromise()
      .then(function (details) {
        if (details) {
          vm.status = details;
        } else {
          $uibModalInstance.close();
        }
      });
  }
}
