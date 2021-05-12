/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import mnPromiseHelper from "/ui/app/components/mn_promise_helper.js";
import mnSessionService from "/ui/app/components/mn_session.js";
import mnSpinner from "/ui/app/components/directives/mn_spinner.js";
import mnMainSpinner from "/ui/app/components/directives/mn_main_spinner.js";

export default 'mnSession';

angular
  .module('mnSession', [
    mnSessionService,
    mnPromiseHelper,
    mnSpinner,
    mnMainSpinner
  ])
  .controller('mnSessionController', mnSessionController);

function mnSessionController(mnSessionService, mnPromiseHelper) {
  var vm = this;

  vm.submit = submit;

  activate();

  function activate() {
    mnPromiseHelper(vm, mnSessionService.get())
      .applyToScope("state");
  }

  function submit() {
    if (vm.viewLoading) {
      return;
    }
    mnPromiseHelper(vm, mnSessionService.post(vm.state.uiSessionTimeout))
      .catchErrors()
      .showSpinner()
      .showGlobalSuccess("Session settings changed successfully!");
  };
}
