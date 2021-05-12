/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default 'mnFocus';

angular
  .module('mnFocus', [])
  .directive('mnFocus', mnFocusDirective);

function mnFocusDirective($parse) {
  var mnFocus = {
    link: link
  };

  return mnFocus;

  function link($scope, $element, $attrs) {

    if ($attrs.mnFocus === "") {
      return $element[0].focus();
    }

    var getter = $parse($attrs.mnFocus);
    var setter = getter.assign;
    $scope.$watch($attrs.mnFocus, function (focus) {
      focus && $element[0].focus();
    });

    if (setter) {
      var handler = function handler() {
        setter($scope, false);
      }
      $element.on('blur', handler);
      $scope.$on('$destroy', function () {
        $element.off('blur', handler);
      })
    }
  }
}
