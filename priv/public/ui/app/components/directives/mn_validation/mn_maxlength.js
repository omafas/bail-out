/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

export default 'mnMaxlength';

angular
  .module('mnMaxlength', [])
  .directive('mnMaxlength', mnMaxlengthDirective);

function mnMaxlengthDirective() {
  var mnMaxlength = {
    restrict: 'A',
    require: 'ngModel',
    link: link
  };
  return mnMaxlength;

  function link(scope, element, attrs, ctrl) {

    ctrl.$parsers.unshift(function (value) {
      var max = attrs.mnMaxlength;
      ctrl.$setValidity('mnMaxlength', max && value && value.length <= parseInt(max));
      return value;
    });
  }
}
