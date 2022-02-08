/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

export default 'mnField';

angular
  .module('mnField', [])
  .directive('mnField', mnFieldDirective);

function mnFieldDirective() {
  var mnFieldDirective = {
    restrict: "AE",
    scope: {
      mnName: "@",
      mnType: "@",
      mnId: "@",
      mnDisabled: "=",
      mnLabel: "@",
      mnErrors: "=?",
      mnModel: "=",
      mnItems: "="
    },
    templateUrl: "app/components/directives/mn_field.html",
    controller: controller,
    controllerAs: "thisCtl"
  };

  return mnFieldDirective;

  function controller() {

  }
}
