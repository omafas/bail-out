/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import mnStatsDesc from "./mn_statistics_description.js";

export default mnServersListItemDetailsController;

function mnServersListItemDetailsController($scope, mnServersListItemDetailsService, mnPromiseHelper, mnStatisticsNewService, mnPermissions, mnPoolDefault, mnOrderServicesFilter, mnEllipsisiseOnLeftFilter) {
  var vm = this;

  vm.isEnterprise = $scope.poolDefault.isEnterprise;
  vm.getServiceQuota = getServiceQuota;
  vm.getServicePath = getServicePath;
  vm.isPathPresent = isPathPresent;
  vm.filterQuotaServices = filterQuotaServices;

  $scope.$watchCollection(() => ({
    stats: $scope.mnUIStats,
    server: vm.server
  }), (values) => {
    if (values.stats && vm.server) {
      updateBarChartData();
    }
  });

  //should be replaced with MnPoolsService.stream.quotaServices in future
  vm.quotaServices =
    (vm.isEnterprise ?
     ["kv", "index", "fts", "cbas", "eventing"] :
     ["kv", "index", "fts"]).reduce((acc, name) => {
       acc[name] = true;
       return acc;
     }, {});

  vm.availableServices =
      $scope.node.services.reduce((acc, name) => {
        acc[name] = true;
        return acc;
      }, {});

  $scope.$watch('node', function (node) {
    mnPromiseHelper(vm, mnServersListItemDetailsService.getNodeDetails(node))
      .applyToScope("server");
  });

  $scope.$watchGroup(['node', 'adminCtl.tasks'], function (values) {
    vm.tasks = mnServersListItemDetailsService.getNodeTasks(values[0], values[1]);
  });

  let statsNames = [
    '@index.index_memory_used',
    '@fts.fts_num_bytes_used_ram',
    '@cbas.cbas_heap_used',
    '@cbas.cbas_disk_used'
  ];

  statsNames =
    mnPoolDefault.export.compat.atLeast70 ? statsNames.map(mnStatsDesc.mapping65) : statsNames;
  vm.getLatestStat =
    mnPoolDefault.export.compat.atLeast70 ? getLatestStat70 : getLatestStat;

  if (mnPoolDefault.export.compat.atLeast70 ||
      $scope.node.clusterMembership !== "inactiveAdded") {
    $scope.serversCtl.mnServersStatsPoller.subscribeUIStatsPoller({
      node: $scope.node.hostname || "all",
      zoom: 3000,
      step: 1,
      bucket: mnPermissions.export.bucketNames['.stats!read'] &&
        mnPermissions.export.bucketNames['.stats!read'][0],
      stats: statsNames
    }, $scope);
  }

  function filterQuotaServices(service) {
    return vm.quotaServices[service];
  }

  function getServiceQuota(service) {
    if (!vm.server || !vm.server.details.storageTotals.ram) {
      return;
    }
    switch (service) {
    case "kv":
      return vm.server.details.storageTotals.ram.quotaTotal;
    default:
      return vm.server.details[service + "MemoryQuota"] * 1024 * 1024;
    }
  }

  function isPathPresent(service) {
    if (!vm.server || !vm.server.details.storage.hdd[0]) {
      return;
    }
    switch (service) {
    case "kv":
      return !!vm.server.details.storage.hdd[0].path;
    case "cbas":
      return !!vm.server.details.storage.hdd[0].cbas_dirs;
    default:
      return !!vm.server.details.storage.hdd[0][service + "_path"];
    }
  }

  function getServicePath(service) {
    if (!vm.server || !vm.server.details.storage.hdd[0]) {
      return;
    }
    switch (service) {
    case "kv":
      return mnEllipsisiseOnLeftFilter(vm.server.details.storage.hdd[0].path, 100);
    case "cbas":
      return vm.server.details.storage.hdd[0].cbas_dirs.map(dir => {
        return mnEllipsisiseOnLeftFilter(dir, 100);
      }).join(" | ");
    default:
      return mnEllipsisiseOnLeftFilter(vm.server.details.storage.hdd[0][service + "_path"], 100);
    }
  }

  function getLatestStat(statName, stats) {
    return stats.stats[statName] && stats.stats[statName][$scope.node.hostname];
  }

  function getLatestStat70(statName, stats) {
    let stat = stats.stats[statName];
    return stat && stat[$scope.node.hostname] &&
      stat[$scope.node.hostname].values.map(([_, v])=> v);
  }

  function updateBarChartData() {
    var details = vm.server.details;
    var ram = details.storageTotals.ram;
    var hdd = details.storageTotals.hdd;
    var stats = $scope.mnUIStats;

    let memoryUsages = [];
    let diskUsages = [];

    mnOrderServicesFilter(details.services).forEach(serviceName => {
      if (!vm.quotaServices[serviceName]) {
        return;
      }
      switch (serviceName) {
      case "kv":
        memoryUsages.push(
          mnServersListItemDetailsService.getBaseConfig(
            'quota allocated to buckets',
            ram ? ram.quotaUsedPerNode : 0,
            ram ? ram.quotaTotalPerNode : 0, true),
          mnServersListItemDetailsService.getBaseConfig(
            'data service used',
            ram ? ram.usedByData : 0,
            ram ? ram.quotaTotalPerNode : 0, true));
        diskUsages.push(mnServersListItemDetailsService.getBaseConfig(
          'data service',
          hdd ? hdd.usedByData : 0,
          hdd ? hdd.free : 0));
        break;
      case "index":
        memoryUsages.push(
          mnServersListItemDetailsService.getBaseConfig(
            'index service used',
            vm.getLatestStat(statsNames[0], stats),
            getServiceQuota(serviceName), true));
        break;
      case "fts":
        memoryUsages.push(
          mnServersListItemDetailsService.getBaseConfig(
            'search service used',
            vm.getLatestStat(statsNames[1], stats),
            getServiceQuota(serviceName), true));
        break;
      case "cbas":
        memoryUsages.push(
          mnServersListItemDetailsService.getBaseConfig(
            'analytics service used',
            vm.getLatestStat(statsNames[2], stats),
            getServiceQuota(serviceName), true));
        diskUsages.push(mnServersListItemDetailsService.getBaseConfig(
          "analytics service",
          vm.getLatestStat(statsNames[3], stats),
          hdd ? hdd.free : 0))
        break;
      }
      //should we add eventigMemoryQuota as well?

      //and other services disk?
      //{name: 'couch_views_actual_disk_size', label: "views"},
      //{name: 'index/disk_size', label: "indexes"},
      //{name: 'fts/num_bytes_used_disk', label: "analytics"},
    });

    vm.diskUsages = diskUsages;
    vm.memoryUsages = memoryUsages;

  }

}
