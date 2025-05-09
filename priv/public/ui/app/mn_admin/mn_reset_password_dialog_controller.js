/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import mnFilters from "../components/mn_filters.js";
import mnEqual from "../components/directives/mn_validation/mn_equal.js";

import mnAuthService from "../mn_auth/mn_auth_service.js";
import mnResetPasswordDialogService from "./mn_reset_password_dialog_service.js"

export default "mnResetPasswordDialog";

angular
  .module("mnResetPasswordDialog", [
    mnFilters,
    mnEqual,
    mnAuthService,
    mnResetPasswordDialogService
  ])
  .controller("mnResetPasswordDialogController", ["mnResetPasswordDialogService", "mnPromiseHelper", "mnAuthService", "user", mnResetPasswordDialogController]);

function mnResetPasswordDialogController(mnResetPasswordDialogService, mnPromiseHelper, mnAuthService, user) {
  var vm = this;
  vm.submit = submit;
  vm.user = {
    name: user.id
  };

  function submit() {
    if (vm.form.$invalid) {
      return;
    }
    var promise = mnResetPasswordDialogService.post(vm.user);

    mnPromiseHelper(vm, promise)
      .showGlobalSpinner()
      .catchErrors()
      .onSuccess(function () {
        return mnAuthService.logout();
      });
  }
}
