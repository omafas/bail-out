/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnServersEjectDialogController;

mnServersEjectDialogController.$inject = ["$rootScope", "$uibModalInstance", "node", "warnings", "mnServersService"];
function mnServersEjectDialogController($rootScope, $uibModalInstance, node, warnings, mnServersService) {
  var vm = this;
  vm.warningFlags = warnings;
  vm.doEjectServer = doEjectServer;

  function doEjectServer() {
    mnServersService.addToPendingEject(node);
    $uibModalInstance.close();
    $rootScope.$broadcast("reloadNodes");
  }
}
