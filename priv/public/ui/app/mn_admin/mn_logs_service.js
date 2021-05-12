/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiBootstrap from "/ui/web_modules/angular-ui-bootstrap.js";
import ngClipboard from "/ui/libs/ngclipboard.js";

export default "mnLogsService";

angular
  .module('mnLogsService', [uiBootstrap, ngClipboard])
  .service('mnLogsService', mnLogsServiceFactory);

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
        templateUrl: 'app/mn_admin/mn_cluster_info_dialog.html',
        scope: scope
      });
    });
  }
}
