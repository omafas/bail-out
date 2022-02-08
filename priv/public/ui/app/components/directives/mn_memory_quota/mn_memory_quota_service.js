/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import _ from 'lodash';

import mnHelper from "../../mn_helper.js";
import mnPoolDefault from "../../mn_pool_default.js";

export default "mnMemoryQuotaService";

angular
  .module('mnMemoryQuotaService', [mnPoolDefault, mnHelper])
  .factory('mnMemoryQuotaService', ["$http", "$window", "mnPoolDefault", "mnHelper", "IEC", mnMemoryQuotaServiceFactory]);

function mnMemoryQuotaServiceFactory($http, $window, mnPoolDefault, mnHelper, IEC) {
  var mnMemoryQuotaService = {
    prepareClusterQuotaSettings: prepareClusterQuotaSettings,
    isOnlyOneNodeWithService: isOnlyOneNodeWithService,
    memoryQuotaConfig: memoryQuotaConfig,
    getFirstTimeAddedServices: getFirstTimeAddedServices,
    handleAltAndClick: handleAltAndClick
  };

  return mnMemoryQuotaService;

  function prepareClusterQuotaSettings(currentPool, displayedServices, calculateMaxMemory, calculateTotal) {
    var ram = currentPool.storageTotals.ram;
    if (calculateMaxMemory === undefined) {
      calculateMaxMemory = displayedServices.kv;
    }
    var rv = {
      calculateTotal: calculateTotal,
      displayedServices: displayedServices,
      Minmemorysize: ram ? Math.max(256, Math.floor(ram.quotaUsedPerNode / IEC.Mi)) : 0,
      totalMemorySize: ram ? Math.floor(ram.total/IEC.Mi) : 0,
      memoryQuota: ram ? Math.floor(ram.quotaTotalPerNode/IEC.Mi) : 0
    };

    rv.indexMemoryQuota = currentPool.indexMemoryQuota || 256;
    rv.ftsMemoryQuota = currentPool.ftsMemoryQuota || 256;

    if (currentPool.compat.atLeast55 && mnPoolDefault.export.isEnterprise) {
      rv.cbasMemoryQuota = currentPool.cbasMemoryQuota || 256;
      rv.eventingMemoryQuota = currentPool.eventingMemoryQuota || 256;
    }
    if (calculateMaxMemory) {
      rv.maxMemorySize = ram ? mnHelper.calculateMaxMemorySize(ram.total / IEC.Mi) : 0;
    } else {
      rv.maxMemorySize = false;
    }

    return rv;
  }
  function getFirstTimeAddedServices(interestedServices, selectedServices, allNodes) {
    var rv = {
      count: 0
    };
    angular.forEach(interestedServices, function (interestedService) {
      if (selectedServices[interestedService] && mnMemoryQuotaService.isOnlyOneNodeWithService(allNodes, selectedServices, interestedService)) {
        rv[interestedService] = true;
        rv.count++;
      }
    });
    return rv;
  }
  function isOnlyOneNodeWithService(nodes, services, service, isTakenIntoAccountPendingEject) {
    var nodesCount = 0;
    _.each(nodes, function (node) {
      nodesCount += (_.indexOf(node.services, service) > -1 && !(isTakenIntoAccountPendingEject && node.pendingEject));
    });
    return nodesCount === 1 && services && (angular.isArray(services) ? (_.indexOf(services, service) > -1) : services[service]);
  }
  function memoryQuotaConfig(displayedServices, calculateMaxMemory, calculateTotal) {
    return mnPoolDefault.get().then(function (poolsDefault) {
      return mnMemoryQuotaService.prepareClusterQuotaSettings(poolsDefault, displayedServices, calculateMaxMemory, calculateTotal);
    });
  }

  function toggleServices(service, config, bool) {
    Object
      .keys(config.services.model)
      .forEach(service1 => {
        config.services.model[service1] = bool;
      });
  }

  function isThereOther(service, config) {
    return Object
      .keys(config.services.model)
      .some(service1 => config.services.model[service1] && service1 !== service);
  }

  function handleAltAndClick(service, config, $event) {
    if (!$event.altKey) {
      return;
    }

    //if label has attribute for then it triggers additional click event on input.
    //as if it's real user click, so we should intercept click event here
    $event.preventDefault();

    toggleServices(service, config, !isThereOther(service, config));

    config.services.model[service] = true;
  }

}
