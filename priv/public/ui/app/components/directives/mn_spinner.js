/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

export default 'mnSpinner';

angular
  .module('mnSpinner', [])
  .directive('mnSpinner', ["$compile", mnSpinnerDirective]);

function mnSpinnerDirective($compile) {
  var directive = {
    restrict: 'A',
    scope: {
      mnSpinner: '=',
      minHeight: '@',
      opacity: '@'
    },
    link: link
  };

  return directive;

  function link($scope, $element) {
    var spinner = angular.element("<div class=\"spinner\" ng-show=\"mnSpinner\"></div>");
    if ($scope.opacity) {
      spinner.addClass("opacity");
    }
    if ($scope.minHeight) {
      spinner.css({minHeight: $scope.minHeight});
    }
    $element.addClass("relative");
    $element.append($compile(spinner)($scope));
  }
}
