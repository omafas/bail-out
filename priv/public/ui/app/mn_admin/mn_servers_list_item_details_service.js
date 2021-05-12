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

export default 'mnServersListItemDetailsService';

angular
  .module('mnServersListItemDetailsService', [])
  .factory('mnServersListItemDetailsService', mnServersListItemDetailsFactory);

function mnServersListItemDetailsFactory($http) {
  var mnServersListItemDetailsService = {
    getNodeDetails: getNodeDetails,
    getNodeTasks: getNodeTasks,
    getBaseConfig: getBaseConfig
  };

  return mnServersListItemDetailsService;

  function getValue(value) {
    return parseFloat(Array.isArray(value) ?
                      value.slice().reverse().find(stat => stat != null) : value);
  }

  function getBaseConfig(title, used, freeOrQuota, isQuota) {
    used = getValue(used);
    freeOrQuota = getValue(freeOrQuota);
    if (Number.isNaN(used) || Number.isNaN(freeOrQuota)) {
      return;
    }
    return {
      items: [{
        name: title,
        value: used
      }, {
        name: 'remaining',
        value: isQuota ? used > freeOrQuota ? 0 : freeOrQuota - used : freeOrQuota
      }]
    };
  }

  function getNodeTasks(node, tasks) {
    if (!tasks || !node) {
      return;
    }
    var rebalanceTask = tasks.tasksRebalance.status === 'running' && tasks.tasksRebalance;
    return {
      warmUpTasks: _.filter(tasks.tasksWarmingUp, function (task) {
        return task.node === node.otpNode;
      }),
      detailedProgress: rebalanceTask.detailedProgress && rebalanceTask.detailedProgress.perNode && rebalanceTask.detailedProgress.perNode[node.otpNode]
    };
  }

  function getNodeDetails(node) {
    return $http({method: 'GET', url: '/nodes/' + encodeURIComponent(node.otpNode)}).then(function (resp) {
      return {
        details: resp.data
      };
    });
  }
}
