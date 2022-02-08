/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

export default 'mnStorageMode';

angular
  .module('mnStorageMode', [])
  .directive('mnStorageMode', mnStorageModeDirective)
  .filter('mnFormatStorageModeError', mnFormatStorageModeError);

function mnFormatStorageModeError() {
  return function (error) {
    if (!error) {
      return;
    }
    var errorCode =
        error.indexOf("Storage mode cannot be set to") > -1 ? 1 :
        error.indexOf("storageMode must be one of") > -1 ? 2 :
        0;
    switch (errorCode) {
    case 1:
      return "please choose another index storage mode";
    case 2:
      return "please choose an index storage mode";
    default:
      return error;
    }
  };
}

function mnStorageModeDirective() {
  var mnStorageMode = {
    restrict: 'E',
    scope: {
      mnIsEnterprise: "=",
      mnModel: "=",
      mnErrors: "=",
      mnCompat: "=?",
      mnPermissions: "=?",
      mnServicesModel: "=?",
      mnInitial: "=?"
    },
    templateUrl: 'app/components/directives/mn_storage_mode/mn_storage_mode.html'
  };

  return mnStorageMode;
}
