/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import saveAs from 'file-saver';
import mnPromiseHelper from "../components/mn_promise_helper.js";
import mnStatisticsDescriptionService from "./mn_statistics_description_service.js";

export default "mnDeveloperSettings";

angular
  .module("mnDeveloperSettings", [
    mnStatisticsDescriptionService,
    mnPromiseHelper
  ])
  .controller("mnDeveloperSettingsController", ["mnStatisticsDescriptionService", "mnPromiseHelper", mnDeveloperSettingsController]);

function mnDeveloperSettingsController(mnStatisticsDescriptionService, mnPromiseHelper) {
  var vm = this;

  vm.onDump70 = onDump70;
  vm.onDump65 = onDump65;

  function onDump(version) {
    let promise = mnStatisticsDescriptionService
        .getStatsDump(version)
        .then(saveResult);

    mnPromiseHelper(vm, promise)
      .showGlobalSpinner()
      .catchErrors()
      .showGlobalSuccess("Done!");
  }

  function saveResult(result) {
    var json = JSON.stringify(result, null, 2);
    var file = new Blob([json], {
      type: "application/json",
      name: "stats.json"
    });
    return saveAs(file, "stats.json");
  }

  function onDump70() {
    onDump("7.0");
  }

  function onDump65() {
    onDump("6.5");
  }

}
