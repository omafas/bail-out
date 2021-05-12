/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default mnDocumentsControlController;

function mnDocumentsControlController($scope, $state, mnPoller, mnDocumentsListService) {
  var vm = this;

  vm.nextPage = nextPage;
  vm.prevPage = prevPage;
  vm.isPrevDisabled = isPrevDisabled;
  vm.isNextDisabled = isNextDisabled;
  vm.onSelectPageLimits = onSelectPageLimits;
  vm.isEmptyState = isEmptyState

  activate();

  function isEmptyState() {
    return !vm.state || vm.state.status;
  }
  function isPrevDisabled() {
    return isEmptyState() || vm.state.pageNumber === 0;
  }
  function isNextDisabled() {
    return isEmptyState() || vm.state.isNextDisabled;
  }
  function nextPage() {
    $state.go('^.list', {
      pageNumber: vm.state.pageNumber + 1
    });
  }
  function prevPage() {
    var prevPage = vm.state.pageNumber - 1;
    prevPage = prevPage < 0 ? 0 : prevPage;
    $state.go('^.list', {
      pageNumber: prevPage
    });
  }
  function onSelectPageLimits(pageLimit) {
    $state.go('^.list', {
      pageLimit: pageLimit
    });
  }
  function activate() {
    var poller = new mnPoller($scope, function () {
      return mnDocumentsListService.getDocumentsListState($state.params);
    })
        .setInterval(10000)
        .subscribe("state", vm)
        .reloadOnScopeEvent("reloadDocumentsPoller");
  }
}
