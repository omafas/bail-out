/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

export default 'mnAutocompleteOff';

//this module is hack for avoiding autocomplete only for one filed - password.
//because all major browsers are moving towards ignoring the attribute for password fields.
//http://stackoverflow.com/questions/3868299/is-autocomplete-off-compatible-with-all-modern-browsers/21348793#21348793

angular
  .module('mnAutocompleteOff', [])
  .directive('mnAutocompleteOff', ["$rootScope", mnAutocompleteOff]);

function mnAutocompleteOff($rootScope) {
  var autocompleteOff = {
    link: link
  };

  return autocompleteOff;

  function link($scope, $element, $attr) {
    if ($attr.mnAutocompleteOff === "enforce" || !$rootScope.ENV || $rootScope.ENV.disable_autocomplete) {
      //avoiding autocomplete via additional input
      var input = angular.element('<input style="display:none" readonly disabled autocomplete="off">');
      input.attr("type", $attr.type);
      input.attr("name", $attr.name);
      $element.parent()[0].insertBefore(input[0], $element[0]);
      $element.attr('autocomplete', 'off');

      //avoiding autocomplete via readonly attr
      $element.attr('readonly', true);
      var onFocus = function onFocus() {
        $element.attr('readonly', false);
      }
      $element.on('focus', onFocus);
      $scope.$on("$destroy", function () {
        $element.off('focus', onFocus);
      });
    }
  }
}
