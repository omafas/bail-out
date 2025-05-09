/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import _ from "lodash";
import mnFilters from "../components/mn_filters.js";

import mnServersService from "./mn_servers_service.js";
import mnStatisticsNewService from "./mn_statistics_service.js";

export default "mnAnalyticsService";

angular
  .module('mnAnalyticsService', [mnServersService, mnFilters, mnStatisticsNewService])
  .factory('mnAnalyticsService', ["$http", "$q", "mnServersService", "mnCloneOnlyDataFilter", "mnFormatQuantityFilter", "mnParseHttpDateFilter", "timeUnitToSeconds", "mnStatisticsNewService", mnAnalyticsServiceFactory]);

function mnAnalyticsServiceFactory(mnServersService) {
  var mnAnalyticsService = {
    prepareNodesList: prepareNodesList
  };

  return mnAnalyticsService;

  function prepareNodesList(params) {
    return mnServersService.getNodes().then(function (nodes) {
      var rv = {};
      rv.nodesNames = _(nodes.active).filter(function (node) {
        return !(node.clusterMembership === 'inactiveFailed') && !(node.status === 'unhealthy');
      }).pluck("hostname").value();
      rv.nodesNames.unshift("All Server Nodes (" + rv.nodesNames.length + ")");
      rv.nodesNames.selected = params.statsHostname === "all" ? rv.nodesNames[0] : params.statsHostname;
      return rv;
    });
  }
}
