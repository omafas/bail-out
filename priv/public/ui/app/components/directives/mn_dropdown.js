/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default "mnDropdown";

angular
  .module('mnDropdown', [])
  .directive('mnDropdown', mnDropdownDirective)
  .directive('mnDropdownItem', mnDropdownItemDirective)

function mnDropdownItemDirective() {
  var mnDropdownItem ={
    require: '^^mnDropdown',
    restrict: 'E',
    scope: {
      mnItem: '='
    },
    link: link
  };

  return mnDropdownItem;

  function link(scope, element, attrs, mnDropdownCtl) {
    element.on("mousedown", onMousedown);
    element.on("click", onItemClick);
    element.on("mouseup", onMouseup);

    scope.$on("$destroy", function () {
      element.off("mousedown", onMousedown);
      element.off("mouseup", onMouseup);
      element.off("click", onItemClick);
    });

    function onItemClick() {
      mnDropdownCtl.onItemClick(scope.mnItem);
    }

    function onMousedown() {
      element.addClass("mousedowm");
    }

    function onMouseup() {
      element.removeClass("mousedowm");
    }

  }
}
function mnDropdownDirective() {
  var mnDropdown = {
    restrict: 'E',
    scope: {
      model: "=?",
      onClose: "&?",
      onSelect: "&?",
      iconClass: "@?"
    },
    transclude: {
      'select': '?innerSelect',
      'header': '?innerHeader',
      'body': 'innerBody',
      'footer': '?innerFooter'
    },
    templateUrl: "app/components/directives/mn_dropdown.html",
    controller: controller
  };

  return mnDropdown;

  function controller($scope, $transclude) {
    $scope.isSlotFilled = $transclude.isSlotFilled;
    this.onItemClick = onItemClick;

    function onItemClick(item) {
      $scope.model && ($scope.model = item);
      $scope.onSelect && $scope.onSelect({scenarioId: item});
    }
  }
}
