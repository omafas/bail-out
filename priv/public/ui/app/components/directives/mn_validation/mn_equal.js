/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

export default 'mnEqual';

angular
  .module('mnEqual', [])
  .directive('mnEqual', mnEqualDirective);

function mnEqualDirective() {
  var mnEqual = {
    restrict: 'A',
    require: 'ngModel',
    link: link
  };
  return mnEqual;

  function link(scope, element, attrs, ctrl) {
    function validate(value) {
      ctrl.$setValidity('mnEqual', (value === undefined ? "" : value) === attrs.mnEqual);
      return value;
    }

    ctrl.$parsers.unshift(validate);
    ctrl.$formatters.push(validate);

    attrs.$observe('mnEqual', function () {
      return validate(ctrl.$viewValue);
    });
  }
}
