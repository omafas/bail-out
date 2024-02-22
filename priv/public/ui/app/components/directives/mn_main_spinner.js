/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import mnSpinner from "./mn_spinner.js";
import mnHelper from "../mn_helper.js";

export default "mnMainSpinnerModule";

angular
  .module('mnMainSpinnerModule', [
    mnSpinner,
    mnHelper
  ])
  .component('mnMainSpinner', {
    bindings: {
      mnSpinnerValue: "<"
    },
    controller: ["$scope", "mnHelper", controller]
  });

function controller($scope, mnHelper) {
  let initialized;
  let vm = this;

  this.$onChanges = onChanges;
  this.$onDestroy = onDestroy;

  function onChanges(v) {
    let value = v.mnSpinnerValue.currentValue;
    if (!initialized && value) {
      initialized = true;
    }
    if (!initialized) {
      return;
    }
    if (value) {
      mnHelper.mainSpinnerCounter.increase();
    } else {
      mnHelper.mainSpinnerCounter.decrease();
    }
  }

  function onDestroy() {
    if (initialized && vm.mnSpinnerValue) {
      mnHelper.mainSpinnerCounter.decrease();
    }
  }
}
