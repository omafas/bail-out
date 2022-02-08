/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";

import mnStatsDescription from "./mn_statistics_description.js";
import mnPoolDefault from "../components/mn_pool_default.js";

export default "mnStatisticsDescriptionService";

angular
  .module('mnStatisticsDescriptionService', [
    mnPoolDefault
  ])
  .factory('mnStatisticsDescriptionService', ["$http", "$q", "mnPoolDefault", mnStatisticsDescriptionFactory]);

function mnStatisticsDescriptionFactory($http, $q, mnPoolDefault) {
  return {
    getStats: getStats,
    getKvGroups: getKvGroups,
    getStatsDescriptions: getStatsDescriptions,
    getStatsDump : getStatsDump
  };

  function getStats() {
    return mnPoolDefault.export.compat.atLeast70 ?
      mnStatsDescription["7.0"].stats :
      mnStatsDescription["6.5"].stats;
  }

  function getKvGroups() {
    return mnPoolDefault.export.compat.atLeast70 ?
      mnStatsDescription["7.0"].kvGroups :
      mnStatsDescription["6.5"].kvGroups;
  }

  function getStatsDescriptions(version) {
    return mnStatsDescription[version].stats;
  }

  function doGetStatMapping(section, stat) {
    return $http({
      url: '/_statsMapping/' + section + '/' + stat,
      method: 'GET'
    }).then(function (resp) {
      return resp.data;
    });
  }

  function doGetMetricsJSON() {
    return $http({
      url: '/ui/metrics/metrics.json',
      method: 'GET'
    }).then(function (resp) {
      var metrics = resp.data;
      var result = {};
      metrics.forEach(function (metric) {
        var name = metric['__name__'];
        if (name) {
          result[name] = metric;
        }
      });
      return result;
    });
  }

  function getStatMapping(section, statName) {
    if (section == '@xdcr-') {
      section += 'sourcebucketname';
      if (statName != 'replication_changes_left' && statName != 'percent_completeness') {
        statName = 'replications/*/sourcebucketname/targetbucketname/' + statName;
      }
    }
    else if (section == '@fts-') {
      section += 'bucketname';
      statName = '/fts/' + statName;
    }
    else if (section == '@index-') {
      section += 'bucketname';
      statName = '/index/' + statName;
    }
    else if (section == '@kv-') {
      section = 'bucketname';
    }
    else if (section.endsWith('-')) {
      section += 'bucketname';
    }
    return doGetStatMapping(section, statName);
  }

  function getUiStats(version) {
    var promises = [];

    var makeStat = function (sectionName, statName, perItem, statDetails) {
      var stat = {};
      stat["ui-name"] = statName;
      stat["group"] = sectionName;
      stat["per-item"] = perItem;
      for (var k in statDetails) {
        stat[k] = statDetails[k];
      }
      if (version == "6.5") {
        let localStat = stat;
        promises.push(
          getStatMapping(sectionName, statName).then(function (resp) {
            localStat["mapping"] = resp;
          }));
      }
      return stat;
    };
    var result = [];
    var statsDesc = getStatsDescriptions(version);
    for (var group in statsDesc) {
      var statsGroup = statsDesc[group];
      for (var key in statsGroup) {
        if (key == "@items") {
          for (var statName in statsGroup[key]) {
            result.push(makeStat(group, statName, true, statsGroup[key][statName]));
          }
        }
        else {
          result.push(makeStat(group, key, false, statsGroup[key]));
        }
      }
    }
    return $q.all(promises).then(function () {
      return result;
    });
  }

  function getByMetricName(uiStats) {
    var result = {};
    uiStats.forEach(function (stat) {
      var metric = stat['metric'];
      if (metric) {
        var name = metric['name'];
        if (name) {
          result[name] = stat;
        }
      }
    });
    return result;
  }

  function getStatsDump(version) {
    return $q.all([getUiStats(version), doGetMetricsJSON()]).then(function (resp) {
      var uiStats = resp[0];
      var metrics = resp[1];
      if (version == "6.5") {
        return uiStats;
      }
      var statsByName = getByMetricName(resp[0]);
      var result = [];
      for (var metricName in metrics) {
        var metric = metrics[metricName];
        var uiStat = statsByName[metricName];
        if (uiStat) {
          metric['ui'] = uiStat;
        }
        result.push(metric);
      }
      return result;
    });
  }
}
