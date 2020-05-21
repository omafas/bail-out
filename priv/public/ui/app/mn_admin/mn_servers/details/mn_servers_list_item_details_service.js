(function () {
  angular
    .module('mnServersListItemDetailsService', [])
    .factory('mnServersListItemDetailsService', mnServersListItemDetailsFactory);

  function mnServersListItemDetailsFactory($http, $q) {
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
          value: isQuota ? freeOrQuota - used : freeOrQuota
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
})();
