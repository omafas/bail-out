/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import _ from "/ui/web_modules/lodash.js";

export default "mnCompaction";

angular
  .module('mnCompaction', [])
  .factory('mnCompaction', mnCompactionFactory);

function mnCompactionFactory($interval, $http) {
  var startedCompactions = {};
  var mnCompaction = {
    registerAsTriggered: registerAsTriggered,
    registerAsTriggeredAndPost: registerAsTriggeredAndPost,
    getStartedCompactions: getStartedCompactions,
    canCompact: canCompact
  };

  activate();

  return mnCompaction;

  function activate() {
    $interval(function () {
      _.each(startedCompactions, mayBeExpired);
    }, 2000);
  }
  function now() {
    return new Date().valueOf();
  }
  function mayBeExpired(desc, key) {
    if (desc.staredAt < now()) {
      desc.undoBody();
      delete startedCompactions[key];
    }
  }
  function registerAsTriggered(url, undoBody) {
    if (startedCompactions[url]) {
      mayBeExpired(startedCompactions[url], url);
    } else {
      startedCompactions[url] = {
        url: url,
        undoBody: undoBody || new Function(),
        staredAt: now() + 10000
      };
    }
  }
  function registerAsTriggeredAndPost(url, undoBody) {
    mnCompaction.registerAsTriggered(url, undoBody);
    return $http.post(url);
  }
  function getStartedCompactions() {
    return startedCompactions;
  }
  function canCompact(url) {
    var desc = startedCompactions[url];
    return desc ? desc.staredAt <= now() : true;
  }
}
