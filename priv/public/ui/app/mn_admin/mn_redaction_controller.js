/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import mnSpinner from "../components/directives/mn_spinner.js";
import mnLogRedactionService from "./mn_redaction_service.js";

export default "mnRedaction";

angular
  .module("mnRedaction", [
    mnLogRedactionService,
    mnSpinner
  ])
  .controller("mnRedactionController", ["$scope", "mnLogRedactionService", "mnPromiseHelper", mnRedactionController]);

function mnRedactionController($scope, mnLogRedactionService, mnPromiseHelper) {
  var vm = this;
  vm.onSubmit = onSubmit;

  activate();

  function onSubmit() {
    if ($scope.mnGlobalSpinnerFlag) {
      return;
    }

    mnPromiseHelper(vm, mnLogRedactionService.post(vm.logRedactionSettings))
      .showGlobalSpinner()
      .catchErrors()
      .showGlobalSuccess("Settings saved successfully!");
  }

  function activate() {
    mnPromiseHelper(vm, mnLogRedactionService.get())
      .applyToScope("logRedactionSettings");
  }
}
