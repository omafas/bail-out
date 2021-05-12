/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default "mnMinlength";

angular
  .module('mnMinlength', [])
  .directive('mnMinlength', mnMinlengthDirective);

function mnMinlengthDirective() {
  var mnMinlength = {
    restrict: 'A',
    require: 'ngModel',
    link: link
  };
  return mnMinlength;

  function link(scope, element, attrs, ctrl) {

    ctrl.$parsers.unshift(function (value) {
      var min = attrs.mnMinlength;
      ctrl.$setValidity('mnMinlength', min && value && value.length >= parseInt(min));
      return value;
    });
  }
}
