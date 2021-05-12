/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import _ from "/ui/web_modules/lodash.js";

import mnFilters from "/ui/app/components/mn_filters.js";
import mnPoolDefault from "/ui/app/components/mn_pool_default.js";
import mnTasksDetails from "/ui/app/components/mn_tasks_details.js";

import mnServersService from "./mn_servers_service.js";

export default "mnLogsCollectInfoService";

angular
  .module('mnLogsCollectInfoService', [
    mnFilters,
    mnPoolDefault,
    mnTasksDetails,
    mnServersService,
  ])
  .service('mnLogsCollectInfoService', mnLogsCollectInfoServiceFactory);

function mnLogsCollectInfoServiceFactory($http, $q, mnServersService, mnTasksDetails, mnStripPortHTMLFilter, mnPoolDefault) {
  var mnLogsCollectInfoService = {
    startLogsCollection: startLogsCollection,
    cancelLogsCollection: cancelLogsCollection,
    getState: getState
  };

  return mnLogsCollectInfoService;

  function startLogsCollection(collect) {
    var data = {};
    data.nodes = collect.nodes.join(',');
    if (collect.upload) {
      data.uploadHost = collect.uploadHost;
      data.customer = collect.customer;
      data.ticket = collect.ticket;
      if (collect.uploadProxy) {
        data.uploadProxy = collect.uploadProxy;
      }
      if (collect.bypassReachabilityChecks) {
        data.bypassReachabilityChecks = collect.bypassReachabilityChecks;
      }
    }
    if (collect.enableLogDir) {
      data.logDir = collect.logDir;
    }
    if (collect.enableTmpDir) {
      data.tmpDir = collect.tmpDir;
    }
    if (mnPoolDefault.export.compat.atLeast55 &&
        mnPoolDefault.export.isEnterprise) {
      data.logRedactionLevel = collect.logRedactionLevel;
    }
    return $http.post('/controller/startLogsCollection', data);
  }
  function cancelLogsCollection() {
    return $http.post('/controller/cancelLogsCollection');
  }
  function getState() {
    var queries = [
      mnServersService.getNodes(),
      mnTasksDetails.get()
    ];
    return $q.all(queries).then(function (resp) {
      var nodes = _.clone(resp[0].allNodes, true);
      var tasks = resp[1].tasks;
      var logRedaction = resp[2];
      var task = _.detect(tasks, function (taskInfo) {
        return taskInfo.type === "clusterLogsCollection";
      });
      if (!task) {
        return {
          nodesByStatus: {},
          nodeErrors: [],
          status: 'idle',
          perNode: {},
          nodes: nodes
        };
      }

      task = JSON.parse(JSON.stringify(task));

      var perNodeHash = task.perNode;
      var perNode = [];

      var cancallable = "starting started startingUpload startedUpload".split(" ");

      _.each(perNodeHash, function (ni, nodeName) {
        var node = _.detect(nodes, function (n) {
          return n.otpNode === nodeName;
        });

        ni.nodeName = (node === undefined) ? nodeName.replace(/^.*?@/, '') : mnStripPortHTMLFilter(node.hostname, nodes);
        perNode.push(ni);
        // possible per-node statuses are:
        //      starting, started, failed, collected,
        //      startingUpload, startedUpload, failedUpload, uploaded

        if (task.status == 'cancelled' && cancallable.indexOf(ni.status) >= 0) {
          ni.status = 'cancelled';
        }
      });

      var nodesByStatus = _.groupBy(perNode, 'status');

      var nodeErrors = _.compact(_.map(perNode, function (ni) {
        var rv;
        var error;
        if (ni.uploadOutput || ni.collectionOutput) {
          error = [];
          ni.uploadOutput && error.push(ni.uploadOutput);
          ni.collectionOutput && error.push(ni.collectionOutput);
          return {nodeName: ni.nodeName, error: error};
        }
      }));

      task.nodesByStatus = nodesByStatus;
      task.nodeErrors = nodeErrors;
      task.nodes = nodes;

      return task
    });
  }
}
