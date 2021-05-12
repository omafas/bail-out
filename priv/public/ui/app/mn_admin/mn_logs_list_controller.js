/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export {mnLogsListController, moduleCodeFilter};

function mnLogsListController($scope, mnLogsService, mnPoller, $filter, moduleCodeFilter)  {
  var vm = this;
  var openedByIndex = {};
  var textLimit = 1000;

  vm.toggle = toggle;
  vm.textLimit = textLimit;
  vm.isOpened = isOpened;
  vm.isOverLimit = isOverLimit;
  vm.filterField = "";
  vm.filter = filter;

  activate();

  function filter(value) {
    return !vm.filterField || vm.filterField
      .split(" ").reduce((acc, find) =>
                         acc && (Object.values(value).join("").indexOf(find) > -1),
                         true);
  }

  function activate() {
    new mnPoller($scope, mnLogsService.getLogs)
      .subscribe(function (logs) {
        vm.logs = logs.data.list.map(function (row) {
          return {
            module: row.module,
            text: row.text,
            node: row.node,
            code: moduleCodeFilter(row.code),
            time: $filter('date')(row.serverTime, 'mediumTime', 'UTC'),
            date: $filter('date')(row.serverTime, 'd MMM, y', 'UTC'),
            tstamp: row.tstamp
          };
        });
      })
      .setInterval(10000)
      .cycle();
  }
  function getOriginalLogItemIndex(index) {
    //because after orderBy:'serverTime':true we have reversed list
    //but we have to track items by their original index in order
    //to keep details open
    return vm.logs.length - (index + 1);
  }
  function isOverLimit(row) {
    return row.text.length > textLimit;
  }
  function toggle(index) {
    var originalIndex = getOriginalLogItemIndex(index);
    openedByIndex[originalIndex] = !openedByIndex[originalIndex];
  }
  function isOpened(index) {
    return openedByIndex[getOriginalLogItemIndex(index)];
  }
}

function moduleCodeFilter() {
  return function (code) {
    return new String(1000 + parseInt(code)).slice(-3);
  };
}
