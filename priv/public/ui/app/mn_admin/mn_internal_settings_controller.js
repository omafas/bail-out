/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import mnPromiseHelper from "../components/mn_promise_helper.js";
import mnSpinner from "../components/directives/mn_spinner.js";

import mnInternalSettingsService from "./mn_internal_settings_service.js";

export default "mnInternalSettings";

angular
  .module("mnInternalSettings", [
    mnPromiseHelper,
    mnSpinner,
    mnInternalSettingsService
  ])
  .controller("mnInternalSettingsController", ["mnInternalSettingsService", "mnPromiseHelper", "mnPoolDefault", "$uibModalInstance", mnInternalSettingsController]);

function mnInternalSettingsController(mnInternalSettingsService, mnPromiseHelper, mnPoolDefault, $uibModalInstance) {
  var vm = this;

  vm.onSubmit = onSubmit;
  vm.mnPoolDefault = mnPoolDefault.latestValue();

  activate();

  function onSubmit() {
    if (vm.viewLoading) {
      return;
    }
    mnPromiseHelper(vm, mnInternalSettingsService.save(vm.state), $uibModalInstance)
      .showGlobalSpinner()
      .catchErrors()
      .closeOnSuccess()
      .reloadState()
      .showGlobalSuccess("Settings saved successfully!");
  }

  function activate() {
    mnPromiseHelper(vm, mnInternalSettingsService.getState())
      .showSpinner()
      .applyToScope("state");
  }
}
