/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnLogsCollectInfoController;

function mnLogsCollectInfoController($scope, mnHelper, mnPromiseHelper, mnPoolDefault, mnLogsCollectInfoService, mnPoller, $state, $uibModal, mnLogRedactionService, permissions, mnGroupsService, mnLogsService) {
  var vm = this;
  vm.stopCollection = stopCollection;
  vm.isNodeDisabled = isNodeDisabled;
  vm.submit = submit;
  vm.showClusterInfoDialog = mnLogsService.showClusterInfoDialog;
  vm.counterSpinner = mnHelper.counterSpinner();

  activate();

  vm.collect = {};
  vm.mnSelectedNodesHolder = {};

  if (mnPoolDefault.latestValue().value.isEnterprise) {
    vm.collect.uploadHost = 'uploads.couchbase.com';
  }

  function isNodeDisabled(node) {
    return node.status === 'unhealthy';
  }

  function activate() {
    if (mnPoolDefault.export.isGroupsAvailable && permissions.cluster.server_groups.read) {
      vm.counterSpinner.increase();
      new mnPoller($scope, mnGroupsService.getGroupsByHostname)
        .subscribe(groups => {
          vm.counterSpinner.decrease();
          vm.getGroupsByHostname = groups;
        })
        .cycle();
    }
    vm.counterSpinner.increase();
    new mnPoller($scope, mnLogsCollectInfoService.getState)
      .subscribe(function (state) {
        vm.counterSpinner.decrease();
        vm.loadingResult = false;
        vm.state = state;
      })
      .reloadOnScopeEvent("mnTasksDetailsChanged")
      .cycle();

    if (permissions.cluster.settings.read &&
        mnPoolDefault.export.compat.atLeast55 &&
        mnPoolDefault.export.isEnterprise) {
      vm.counterSpinner.increase();
      mnLogRedactionService.get().then(function (value) {
        vm.counterSpinner.decrease();
        vm.collect.logRedactionLevel = value.logRedactionLevel;
      });
    }
  }

  function stopCollection() {
    $uibModal.open({
      templateUrl: 'app/mn_admin/mn_logs_collect_info_stop_dialog.html'
    }).result.then(function () {
      vm.disabledStopCollect = true;
      mnPromiseHelper(vm, mnLogsCollectInfoService.cancelLogsCollection())
        .getPromise()['finally'](function () {
          vm.disabledStopCollect = false;
        });
    });
  }
  function submit() {
    vm.collect.nodes = mnHelper.checkboxesToList(vm.mnSelectedNodesHolder);
    mnPromiseHelper(vm, mnLogsCollectInfoService.startLogsCollection(vm.collect))
      .showSpinner()
      .catchErrors()
      .onSuccess(function () {
        vm.loadingResult = true;
        $state.go('app.admin.logs.collectInfo.result');
      });
  }
}
