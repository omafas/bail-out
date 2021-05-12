/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

import mnHelper from "/ui/app/components/mn_helper.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";
import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";

import mnAutoCompactionForm from "/ui/app/components/directives/mn_auto_compaction_form/mn_auto_compaction_form.js";
import mnSettingsAutoCompactionService from "./mn_settings_auto_compaction_service.js";

export default 'mnSettingsAutoCompaction';

angular
  .module('mnSettingsAutoCompaction', [
    mnHelper,
    mnMainSpinner,
    mnPromiseHelper,
    mnAutoCompactionForm,
    mnSettingsAutoCompactionService
  ])
  .controller('mnSettingsAutoCompactionController', mnSettingsAutoCompactionController);

function mnSettingsAutoCompactionController($scope, mnHelper, mnPromiseHelper, mnSettingsAutoCompactionService) {
  var vm = this;

  vm.reloadState = mnHelper.reloadState;
  vm.submit = submit;

  activate();

  function activate() {
    mnPromiseHelper(vm, mnSettingsAutoCompactionService.getAutoCompaction())
      .applyToScope("autoCompactionSettings")
      .onSuccess(function () {
        $scope.$watch('settingsAutoCompactionCtl.autoCompactionSettings', watchOnAutoCompactionSettings, true);
      });
  }
  function watchOnAutoCompactionSettings(autoCompactionSettings) {
    if (!$scope.rbac.cluster.settings.write) {
      return;
    }
    mnPromiseHelper(vm, mnSettingsAutoCompactionService
                    .saveAutoCompaction(autoCompactionSettings, {just_validate: 1}))
      .catchErrors();
  }
  function submit() {
    if (vm.viewLoading) {
      return;
    }
    delete vm.errors;
    mnPromiseHelper(vm, mnSettingsAutoCompactionService.saveAutoCompaction(vm.autoCompactionSettings))
      .showGlobalSpinner()
      .reloadState()
      .catchErrors()
      .showGlobalSuccess("Settings saved successfully!");
  }
}
