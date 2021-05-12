/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnHelper from "/ui/app/components/mn_helper.js";

export default "mnMainSpinner";

angular
  .module('mnMainSpinner', [mnSpinner, mnHelper])
  .directive('mnMainSpinner', mnMainSpinnerDirective);

function mnMainSpinnerDirective(mnHelper) {
  var directive = {
    restrict: 'A',
    scope: {
      mnMainSpinner: '=',
    },
    controller: controller
  };
  return directive;

  function controller($scope) {
    let initialized;
    $scope.$watch("mnMainSpinner", (mainSpinner) => {
      if (!initialized && mainSpinner) {
        initialized = true;
      }
      if (!initialized) {
        return;
      }
      if (mainSpinner) {
        mnHelper.mainSpinnerCounter.increase();
      } else {
        mnHelper.mainSpinnerCounter.decrease();
      }
    });
    $scope.$on("$destroy", () => {
      if (initialized && $scope.mnMainSpinner) {
        mnHelper.mainSpinnerCounter.decrease();
      }
    });
  }
}
