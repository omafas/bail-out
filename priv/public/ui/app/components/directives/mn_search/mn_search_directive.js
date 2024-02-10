/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import template from "./mn_search.html";

export default 'mnSearch';

angular
  .module('mnSearch', [])
  .directive('mnSearch', mnSearchDirective);

function mnSearchDirective() {

  var mnSearch = {
    restrict: 'AE',
    scope: {
      mnSearch: "=",
      mnPlaceholder: "@",
      mnHideButton: "=",
      mnDisabled: "="
    },
    template,
    controller: controller,
    controllerAs: "mnSearchCtl",
    bindToController: true
  };

  return mnSearch;

  function controller() {
    var vm = this;
    vm.hideFilter = hideFilter;
    vm.showFilter = showFilter;

    function hideFilter() {
      vm.mnSearch = "";
      vm.showFilterFlag = false;
    }
    function showFilter() {
      vm.showFilterFlag = true;
      vm.focusFilterField = true;
    }
  }
}
