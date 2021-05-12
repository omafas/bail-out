/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default "mnFilter";

angular
  .module("mnFilter", [])
  .directive("mnFilter", mnFilterDirective);

function mnFilterDirective($window) {
  var mnFilter = {
    restrict: "A",
    scope: {
      config: "=",
      mnDisabled: "=",
      onClose: "&",
      onOpen: "&",
      onReset: "&"
    },
    templateUrl: "app/components/directives/mn_filter/mn_filter.html",
    controller: mnFilterController,
    controllerAs: "mnFilterCtl",
    bindToController: true
  };

  return mnFilter;

  function mnFilterController($scope) {
    var vm = this;

    vm.togglePopup = togglePopup;


    function togglePopup(open) {
      vm.showPopup = open;
      if (vm.showPopup) {
        vm.onOpen && vm.onOpen();
      } else {
        vm.onClose && vm.onClose();
      }
    }
  }
}
