/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import uiBootstrap from "angular-ui-bootstrap";
import ngClipboard from "ngclipboard";
import template from "./mn_cluster_info_dialog.html";

export default "mnLogsService";

angular
  .module('mnLogsService', [uiBootstrap, ngClipboard])
  .service('mnLogsService', ["$http", "$rootScope", "$uibModal", mnLogsServiceFactory]);

function mnLogsServiceFactory($http, $rootScope, $uibModal) {
  var mnLogsService = {
    getLogs: getLogs,
    showClusterInfoDialog: showClusterInfoDialog
  };

  return mnLogsService;


  function getLogs() {
    return $http.get('/logs');
  }

  function getClusterInfo() {
    return $http.get('/pools/default/terseClusterInfo?all=true');
  }

  function showClusterInfoDialog() {
    return getClusterInfo().then(function (resp) {
      var scope = $rootScope.$new();
      scope.info = JSON.stringify(resp.data, null, 2);
      return $uibModal.open({
        template,
        scope: scope
      });
    });
  }
}
