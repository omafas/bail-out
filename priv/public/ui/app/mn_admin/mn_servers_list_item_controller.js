/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import _ from "lodash";
import mnServersEjectDialogTemplate from "./mn_servers_eject_dialog.html";
import mnServersFailoverDialogTemplate from "./mn_servers_failover_dialog.html";

export default mnServersListItemController;

mnServersListItemController.$inject = ["$scope", "$rootScope", "$uibModal", "mnServersService", "mnMemoryQuotaService", "mnGsiService", "mnPromiseHelper", "mnPermissions", "mnPoolDefault"];
function mnServersListItemController($scope, $rootScope, $uibModal, mnServersService, mnMemoryQuotaService, mnGsiService, mnPromiseHelper, mnPermissions, mnPoolDefault) {
  var vm = this;

  vm.cancelEjectServer = cancelEjectServer;
  vm.cancelFailOverNode = cancelFailOverNode;
  vm.reAddNode = reAddNode;
  vm.failOverNode = failOverNode;
  vm.ejectServer = ejectServer;
  vm.disableRemoveBtn = disableRemoveBtn;
  vm.isFailOverDisabled = isFailOverDisabled;
  vm.hasMinorCertWarning = hasMinorCertWarning;
  vm.hasMajorCertWarning = hasMajorCertWarning;

  var ramUsageConf = {};
  var swapUsageConf = {};
  var cpuUsageConf = {};
  var statisticsStateParams = {};

  activate();

  function activate() {
    $scope.$watch("node", onNodeUpdate, true);
    $scope.$watchGroup(['node', 'adminCtl.tasks'], function (values) {
      vm.getRebalanceProgress = getRebalanceProgress(values[0], values[1]);
    });
  }
  function onNodeUpdate(node) {
    vm.isNodeUnhealthy = isNodeUnhealthy(node);
    vm.isNodeInactiveFailed = isNodeInactiveFailed(node);
    vm.isLastActiveData = isLastActiveData(node);
    vm.isNodeInactiveAdded = isNodeInactiveAdded(node);
    vm.couchDiskUsage = couchDiskUsage(node);

    vm.isKVNode = isKVNode(node);

    vm.getStatisticsStateParams = getStatisticsStateParams(node);
    vm.getRamUsageConf = getRamUsageConf(node);
    vm.getSwapUsageConf = getSwapUsageConf(node);
    vm.getCpuUsageConf = getCpuUsageConf(node);
  }
  function isKVNode(node) {
    return node.services.indexOf("kv") > -1;
  }
  function getStatisticsStateParams(node) {
    statisticsStateParams.statsHostname = node.hostname;
    return statisticsStateParams;
  }
  function getRamUsageConf(node) {
    var total = node.memoryTotal;
    var free = node.memoryFree;
    var used = total - free;

    ramUsageConf.exist = (total > 0) && _.isFinite(free);
    ramUsageConf.value = used / total * 100;

    return ramUsageConf;
  }
  function getSwapUsageConf(node) {
    var swapTotal = node.systemStats.swap_total;
    var swapUsed = node.systemStats.swap_used;
    swapUsageConf.exist = swapTotal > 0 && _.isFinite(swapUsed);
    swapUsageConf.value = (swapUsed / swapTotal) * 100;
    return swapUsageConf;
  }
  function getCpuUsageConf(node) {
    var cpuRate = node.systemStats.cpu_utilization_rate;
    cpuUsageConf.exist = _.isFinite(cpuRate);
    cpuUsageConf.value = Math.floor(cpuRate * 100) / 100;
    return cpuUsageConf;
  }
  function isFailOverDisabled(node) {
    return isLastActiveData(node) || ($scope.adminCtl.tasks && $scope.adminCtl.tasks.inRecoveryMode);
  }
  function disableRemoveBtn(node) {
    return isLastActiveData(node) || isActiveUnhealthy(node) || ($scope.adminCtl.tasks && $scope.adminCtl.tasks.inRecoveryMode);
  }
  function isLastActiveData(node) {
    return $scope.serversCtl.nodes.reallyActiveData.length === 1 && isKVNode(node);
  }
  function isNodeInactiveAdded(node) {
    return node.clusterMembership === 'inactiveAdded';
  }
  function isNodeUnhealthy(node) {
    return node.status === 'unhealthy';
  }
  function isActive(node) {
    return node.clusterMembership === 'active';
  }
  function isNodeInactiveFailed(node) {
    return node.clusterMembership === 'inactiveFailed';
  }
  function couchDiskUsage(node) {
    return node.interestingStats['couch_docs_actual_disk_size'] +
      node.interestingStats['couch_views_actual_disk_size'] +
      node.interestingStats['couch_spatial_disk_size'];
  }
  function getRebalanceProgress(node, tasks) {
    return tasks && (tasks.tasksRebalance.perNode && tasks.tasksRebalance.perNode[node.otpNode]
                     ? tasks.tasksRebalance.perNode[node.otpNode].progress : 0 );
  }
  function isActiveUnhealthy(node) {
    return (isActive(node) || isNodeInactiveFailed(node)) && isNodeUnhealthy(node);
  }
  function ejectServer(node) {
    if (isNodeInactiveAdded(node)) {
      mnPromiseHelper(vm, mnServersService.ejectNode({otpNode: node.otpNode}))
        .showErrorsSensitiveSpinner()
        .broadcast("reloadServersPoller");
      return;
    }

    var promise = mnServersService.getNodes().then(function (nodes) {
      var warnings = {
        isLastIndex: mnMemoryQuotaService.isOnlyOneNodeWithService(nodes.allNodes, node.services, 'index', true),
        isLastQuery: mnMemoryQuotaService.isOnlyOneNodeWithService(nodes.allNodes, node.services, 'n1ql', true),
        isLastBackup: mnMemoryQuotaService.isOnlyOneNodeWithService(nodes.allNodes, node.services, 'backup', true),
        isLastFts: mnMemoryQuotaService.isOnlyOneNodeWithService(nodes.allNodes, node.services, 'fts', true),
        isLastEventing: mnMemoryQuotaService.isOnlyOneNodeWithService(nodes.allNodes, node.services, 'eventing', true),
        isKv: _.indexOf(node.services, 'kv') > -1
      };
      if (mnPoolDefault.export.isEnterprise) {
        warnings.isLastCBAS = mnMemoryQuotaService.isOnlyOneNodeWithService(nodes.allNodes, node.services, 'cbas', true);
        warnings.isLastBackup = mnMemoryQuotaService.isOnlyOneNodeWithService(nodes.allNodes, node.services, 'backup', true);
      }
      return mnPermissions.export.cluster.collection['.:.:.'].n1ql.index.read ? mnGsiService.getIndexStatus().then(function (indexStatus) {
        warnings.isThereIndex = !!_.find(indexStatus.indexes, function (index) {
          return _.indexOf(index.hosts, node.hostname) > -1;
        });
        warnings.isThereReplica = warnings.isThereIndex;
        return warnings;
      }) : warnings;
    }).then(function (warnings) {
      if (_.some(_.values(warnings))) {
        $uibModal.open({
          template: mnServersEjectDialogTemplate,
          controller: 'mnServersEjectDialogController as serversEjectDialogCtl',
          resolve: {
            warnings: function () {
              return warnings;
            },
            node: function () {
              return node;
            }
          }
        });
      } else {
        mnServersService.addToPendingEject(node);
        $rootScope.$broadcast("reloadNodes");
      }
    });

    mnPromiseHelper(vm, promise);
  }
  function failOverNode(node) {
    $uibModal.open({
      template: mnServersFailoverDialogTemplate,
      controller: 'mnServersFailOverDialogController as serversFailOverDialogCtl',
      resolve: {
        node: function () {
          return node;
        }
      }
    });
  }
  function reAddNode(type, otpNode) {
    mnPromiseHelper(vm, mnServersService.reAddNode({
      otpNode: otpNode,
      recoveryType: type
    }))
      .broadcast("reloadServersPoller")
      .showErrorsSensitiveSpinner();
  }
  function cancelFailOverNode(otpNode) {
    mnPromiseHelper(vm, mnServersService.cancelFailOverNode({
      otpNode: otpNode
    }))
      .broadcast("reloadServersPoller")
      .showErrorsSensitiveSpinner();
  }
  function cancelEjectServer(node) {
    mnServersService.removeFromPendingEject(node);
    $rootScope.$broadcast("reloadNodes");
  }

  function hasMajorCertWarning(node) {
    let nodeCertificate = $scope.serversCtl.nodeCertificates &&
      $scope.serversCtl.nodeCertificates[node.configuredHostname];

    return nodeCertificate && nodeCertificate.highestSeverity > 3;
  }

  function hasMinorCertWarning(node) {
    let nodeCertificate = $scope.serversCtl.nodeCertificates &&
      $scope.serversCtl.nodeCertificates[node.configuredHostname];

    return nodeCertificate && (nodeCertificate.highestSeverity == 3);
  }
}
