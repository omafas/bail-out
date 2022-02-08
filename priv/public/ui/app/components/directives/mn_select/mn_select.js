/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

import mnSearch from "../mn_search/mn_search_directive.js";
import mnHelper from "../../mn_helper.js";
import template from "./mn_select.html";

export default "mnSelect";

angular
  .module("mnSelect", [
    mnSearch,
    mnHelper
  ])
  .directive("mnSelect", ["mnHelper", mnSelectDirective]);

function mnSelectDirective(mnHelper) {
  var mnSelect = {
    restrict: "AE",
    require: ["mnSelect", "ngModel"],
    scope: {
      values: "=",
      labels: "=",
      ngModel: "=",
      onSelect: "&",
      mnDisabled: "=",
      valuesMapping: "=?",
      capitalize: "=",
      openOnTop: "=",
      mnHorizontalAlign: "=?",
      mnPlaceholder: "=",
      hasSearch: "=?"
    },
    template,
    link: ($scope, $element, $attributes, controllers) => {
      controllers[0].setNgModelCtl(controllers[1]);
    },
    controller: ["$scope", "$element", "$attrs", mnSelectController],
    controllerAs: "vm",
    bindToController: true
  };

  return mnSelect;

  function mnSelectController($scope, $element, $attrs) {
    var vm = this;
    var searchMinimumOptionsNumber = 10;
    vm.id = mnHelper.generateID();

    var ngModelCtl;
    vm.setNgModelCtl = (ctl) => (ngModelCtl = ctl);

    if (!$attrs['valuesMapping']) {
      vm.valuesMapping = defaultValuesMapping;
    }
    if (!$attrs['mnHorizontalAlign']) {
      vm.mnHorizontalAlign = "left";
    }

    vm.mnSearchValue = "";
    vm.isOpened = false;
    vm.optionClicked = optionClicked;
    vm.clickSearch = clickSearch;
    vm.getPreparedValues = getPreparedValues;
    vm.hasSearchInput = hasSearchInput;

    /**
     * Default values mapping:
     * * if capitalize input flag is true - capitalize the displayed option if it is a string
     * * else leave the option as it is
     * @param option
     * @returns {string}
     */
    function defaultValuesMapping(option) {
      if (vm.capitalize && angular.isString(option) && option) {
        return option.charAt(0).toUpperCase() + option.slice(1);
      }

      return option;
    }

    function getPreparedValues() {
      vm.preparedValues = vm.labels ? vm.labels : (vm.values || []).map(vm.valuesMapping);
      return vm.preparedValues;
    }

    function optionClicked(value, event) {
      if (event && event.key !== 'Enter') {
        return;
      }

      vm.onSelect && vm.onSelect({selectedOption: value});
      ngModelCtl.$setViewValue(value);
      vm.isOpened = false;

      if (vm.hasSearchInput()) {
        vm.mnSearchValue = "";
      }
    }

    function clickSearch(event) {
      event.stopPropagation();
    }

    function hasSearchInput() {
      return (vm.hasSearch && (vm.values || []).length >= searchMinimumOptionsNumber) || false;
    }
  }
}
