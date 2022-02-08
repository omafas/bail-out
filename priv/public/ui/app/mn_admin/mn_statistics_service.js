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
import {timeFormat} from "d3-time-format";
import {timeMinute,
        timeHour,
        timeDay,
        timeMonth,
        timeYear} from "d3-time";

import mnPoll from "../components/mn_poll.js";
import mnStoreService from "../components/mn_store_service.js";
import mnPoolDefault from "../components/mn_pool_default.js";
import mnPermissions from "../components/mn_pool_default.js";

import mnServersService from "./mn_servers_service.js"
import mnStatisticsDescriptionService from "./mn_statistics_description_service.js";
import mnStatisticsDescription from "./mn_statistics_description.js";

export default "mnStatisticsNewService";

angular
  .module('mnStatisticsNewService', [
    mnServersService,
    mnPoll,
    mnStatisticsDescriptionService,
    mnStoreService,
    mnPoolDefault,
    mnPermissions
  ])
  .factory('mnStatisticsNewService', ["$http", "mnServersService", "mnPoller", "$rootScope", "mnStatisticsDescriptionService", "mnStoreService", "mnPoolDefault", "mnPermissions", mnStatisticsNewServiceFactory]);

function mnStatisticsNewServiceFactory($http, mnServersService, mnPoller, $rootScope, mnStatisticsDescriptionService, mnStoreService, mnPoolDefault, mnPermissions) {
  var rootScope = $rootScope.$new();

  var formatSecond = timeFormat("%-I:%M:%S%p");
  var formatMinute = timeFormat("%-I:%M%p");
  var formatHour = timeFormat("%-I%p");
  var formatDayMonth = timeFormat("%b %-d");
  var formatYear = timeFormat("%Y");

  var mnStatisticsNewService = {
    prepareNodesList: prepareNodesList,
    export: {
      scenario: {}
    },
    descriptionPathsToStatNames: descriptionPathsToStatNames,
    descriptionPathToStatName: descriptionPathToStatName,
    defaultZoomInterval: defaultZoomInterval,

    copyScenario: copyScenario,
    deleteScenario: deleteScenario,
    deleteGroup: deleteGroup,
    deleteChart: deleteChart,
    doAddPresetScenario: doAddPresetScenario,

    getStatsDirectory: getStatsDirectory,
    get70StatUniqueName: get70StatUniqueName,
    readByPath: readByPath,
    getStatsUnits: getStatsUnits,
    getStatsTitle: getStatsTitle,
    getStatsDesc: getStatsDesc,
    buildChartConfig: buildChartConfig,
    tickMultiFormat: multiFormat,
    packStatsConfig: packStatsConfig,
    postStatsRange: postStatsRange,
    postStats: postStats,
    createStatsPoller: createStatsPoller,
    mnAdminStatsPoller: createStatsPoller(rootScope),
    getChartStep: function (zoom) {
      if (mnPoolDefault.export.compat.atLeast70) {
        return rangeZoomToStep(zoom) * 1000;
      } else {
        return zoomToStep(zoom) * 1000;
      }
    },
    getChartStart: function (zoom) {
      if (mnPoolDefault.export.compat.atLeast70) {
        return rangeZoomToSec(zoom) * 1000;
      } else {
        return zoomToMS(zoom);
      }
    }
  };

  return mnStatisticsNewService;

  function switchToFullStatInfo(config, originConfig) {
    config.step = rangeZoomToStep(originConfig.zoom);
    config.timeWindow = Math.max(config.step * 2, 360);
    config.start = 0 - rangeZoomToSec(originConfig.zoom);
    if (originConfig.zoom == "minute") {
      //in order to make sure that we recieve 12 samples UI
      //should send a bit less seconds
      //e.g. start = - N * step + 1 (- 12 * 10  + 1 = -119)
      config.start += 1;
    }
  }

  function switchToSingleStat(config, originConfig) {
    let step = rangeZoomToStep(originConfig.zoom);
    config.start = -step;
    config.step = step;
    config.timeWindow = Math.max(step * 2, 360);
  }

  function createStatsPoller(scope) {
    var perChartConfig = [];
    var perChartStatsPath = [];
    var perChartScopes = [];
    var perChartOriginConfig = [];
    var currentPerChartScopes = [];
    var counter = 0;
    let heartbeat =
        new mnPoller(scope, function () {
          counter++;
          currentPerChartScopes = [...perChartScopes];
          return mnPoolDefault.export.compat.atLeast70 ?
            postStatsRange([...perChartConfig]) :
            postStats([...perChartConfig]);
        })
        .setInterval(function (resp) {
          return (resp && resp.interval) || 10000;
        })
        .subscribe(function (value) {
          if (!value.data) {
            return;
          }
          if (mnPoolDefault.export.compat.atLeast70) {
            currentPerChartScopes.forEach(scope => delete scope["mnUIStats"]);
            currentPerChartScopes.forEach(unpack70Stats(value));
          } else {
            currentPerChartScopes.forEach(function (scope, i) {
              scope["mnUIStats"] = value.data[i];
            });
          }
        })
        .onResum(function () {
          counter = 0;
          if (mnPoolDefault.export.compat.atLeast70) {
            perChartConfig.forEach(function (config, i) {
              switchToFullStatInfo(config, perChartOriginConfig[i]);
            });
          }
        });

    return {
      subscribeUIStatsPoller: subscribeUIStatsPoller,
      heartbeat: heartbeat,
      isThisInitCall: () => counter == 1
    };

    function subscribeUIStatsPoller(config, scope) {
      if (!mnPermissions.export.cluster.collection['.:.:.'].stats.read) {
        return;
      }
      let config1 = packStatsConfig(config);

      function register(config2, statPath) {
        if (mnPoolDefault.export.compat.atLeast70) {
          perChartStatsPath.push(statPath);
          delete config2.statPath;
        }

        perChartOriginConfig.push(config);
        perChartConfig.push(config2);
        perChartScopes.push(scope);

        heartbeat.throttledReload();
      }
      function omit(config2) {
        var index = perChartConfig.indexOf(config2);
        perChartConfig.splice(index, 1);
        perChartScopes.splice(perChartScopes.indexOf(scope), 1);
        perChartOriginConfig.splice(perChartScopes.indexOf(config), 1);

        if (mnPoolDefault.export.compat.atLeast70) {
          perChartStatsPath.splice(index, 1);
        }

        if (!perChartConfig.length) {
          heartbeat.stop();
        }
      }
      config1.forEach((config2) => {
        let statPath = config2.statPath;
        register(config2, statPath);
        scope.$on("$destroy", function () {
          omit(config2);
        });
      });
    }

    function unpack70Stats(resp) {
      return function (scope, i) {
        if (!resp.data[i]) {
          return;
        }
        var config = perChartConfig[i];
        if (!config) {
          return;
        }
        switchToSingleStat(config, perChartOriginConfig[i]);
        var statPath = perChartStatsPath[i];
        var data = resp.data[i].data[0];
        scope["mnUIStats"] = scope["mnUIStats"] || {
          stats: {},
          endTimestamp: resp.data[i].endTimestamp * 1000,
          startTimestamp: resp.data[i].startTimestamp * 1000
        };
        var maybeScopeHasStat = scope["mnUIStats"].stats[statPath] || {};
        if (!config.nodesAggregation) {
          scope["mnUIStats"].stats[statPath] =
            resp.data[i].data.reduce((acc, data) => {
              acc[data.metric.nodes[0]] = data;
              return acc;
            }, maybeScopeHasStat);
        } else {
          scope["mnUIStats"].stats[statPath] =
            maybeScopeHasStat;
          maybeScopeHasStat[config.nodesAggregation ? "aggregate" : data.nodes[0]] = data;
        }
      }
    }
  }

  function buildChartConfig(stats, statName, currentNode, title, unit, axis, previousData, isThisInitCall, start, step) {
    currentNode = currentNode == "all" ? "aggregate" : currentNode;
    var perNodeStats = stats.stats[statName] && stats.stats[statName][currentNode];
    var values = [];

    if (perNodeStats) {
      if (mnPoolDefault.export.compat.atLeast70) {
        if (previousData && !isThisInitCall) {
          perNodeStats.values = [perNodeStats.values.pop()];
          values = previousData.values;
        }

        perNodeStats.values.forEach(([ts, v]) => {
          values.push([ts * 1000, Number(v)]);
        });

        if (!isThisInitCall && (values.length > start/step)) {
          values = values.slice(values.length - start/step);
          previousData && (previousData.values = values);
        }
      } else {
        stats.timestamps.forEach((ts, i) => {
          var v = perNodeStats[i];
          var convertValue = v === null ? undefined : v;
          values.push([ts, convertValue]);
        });
      }
    }

    let yMin = 0;
    let yMax = 1;
    values.forEach(v => {
      yMin = yMin > v[1] ? v[1] : yMin;
      yMax = yMax < v[1] ? v[1] : yMax;
    });

    return {
      endTimestamp: stats.endTimestamp,
      startTimestamp: previousData && !isThisInitCall ? previousData.startTimestamp + step : stats.startTimestamp,
      type: 'line',
      unit: unit,
      yAxis: axis,
      yMin: yMin,
      yMax: yMax,
      key: title,
      values: values
    };
  }

  function defaultZoomInterval(zoom) {
    return mnPoolDefault.export.compat.atLeast70 ? function () {
      return rangeZoomToStep(zoom) * 1000;
    } : function (resp) {
      return resp.interval || (function () {
        switch (zoom) {
        case "minute": return 1000;
        default: return 15000;
        }
      })();
    }
  }

  // Define filter conditions
  function multiFormat(date) {
    return (timeMinute(date) < date ? formatSecond
            : timeHour(date) < date ? formatMinute
            : timeDay(date) < date ? formatHour
            : timeMonth(date) < date ? formatDayMonth
            : timeYear(date) < date ? formatDayMonth
            : formatYear)(date);
  }

  function getStatsDirectory(bucket, params) {
    //we are using this end point in new stats ui in order to tie ddocs names with ddocs stats
    //via ddocs signatures
    params = params || {
      adde: '"all"',
      adda: '"all"',
      addi: '"all"',
      addf: '"all"',
      addq: "1"
    };
    return $http({
      url: "/pools/default/buckets/" + bucket + "/statsDirectory",
      method: 'GET',
      params: params
    });
  }

  function deleteChart(chartID) {
    var group = mnStoreService.store("groups").getByIncludes(chartID, "charts");
    group.charts.splice(group.charts.indexOf(chartID), 1);
    mnStoreService.store("charts").delete(chartID);
  }

  function deleteGroup(groupID) {
    var scenario = mnStoreService.store("scenarios").getByIncludes(groupID, "groups");
    scenario.groups.splice(scenario.groups.indexOf(groupID), 1);
    var group = mnStoreService.store("groups").get(groupID);
    group.charts.forEach(function (chartID) {
      mnStoreService.store("charts").delete(chartID);
    });
    mnStoreService.store("groups").delete(groupID);
  }

  function deleteScenario(scenarioID) {
    var scenario = mnStoreService.store("scenarios").get(scenarioID);
    mnStoreService.store("scenarios").deleteItem(scenario);
    scenario.groups.forEach(function (groupID) {
      var group = mnStoreService.store("groups").get(groupID);
      mnStoreService.store("groups").deleteItem(group);
      group.charts.forEach(function (chartID) {
        mnStoreService.store("charts").delete(chartID);
      });
    });
  }

  function copyScenario(scenario, copyFrom) {
    scenario = mnStoreService.store("scenarios").add(scenario);
    scenario.groups = (copyFrom.groups || []).map(function (groupID) {
      var groupToCopy = mnStoreService.store("groups").get(groupID);
      var copiedGroup = mnStoreService.store("groups").add(groupToCopy);
      copiedGroup.preset = false;
      copiedGroup.charts = (copiedGroup.charts || []).map(function (chartID) {
        var chartToCopy = mnStoreService.store("charts").get(chartID);
        var copiedChart = mnStoreService.store("charts").add(chartToCopy);
        copiedChart.preset = false;
        return copiedChart.id;
      });
      return copiedGroup.id;
    });
  }

  function getStatsTitle(stats) {
    return Object.keys(stats).map(function (descPath) {
      var desc = mnStatisticsNewService.readByPath(descPath);
      return desc ? desc.title : descPath.split(".").pop();
    }).join(", ");
  }

  function getStatsDesc(stats) {
    return Object.keys(stats).map(function (descPath) {
      var desc = mnStatisticsNewService.readByPath(descPath);
      if (desc) {
        return "<b>" + desc.title + "</b><p>" + desc.desc + "</p>";
      } else {
        return "<b>" + descPath.split(".").pop() + "</b>" +
          "<p>There is no such stat name anymore. Edit the chart in order to remove it.</p>";
      }
    }).join("");
  }

  function getStatsUnits(stats) {
    var units = {};
    Object.keys(stats).forEach(function (descPath) {
      if (!stats[descPath]) {
        return;
      }
      var desc = mnStatisticsNewService.readByPath(descPath);
      if (desc) {
        units[desc.unit] = true;
      }
    });
    return units;
  }

  function readByPath(descPath) {
    var paths = descPath.split('.');
    var statsDesc = mnStatisticsDescriptionService.getStats();
    var i;

    for (i = 0; i < paths.length; ++i) {
      if (statsDesc[paths[i]] == undefined) {
        return undefined;
      } else {
        statsDesc = statsDesc[paths[i]];
      }
    }
    return statsDesc;
  }

  function isPerBucketStat(path) {
    return path.split(".")[0].includes("-");
  }

  function postStats(perChartConfig) {
    return $http({
      url: '/_uistats',
      method: 'POST',
      mnHttp: {
        group: "global",
        isNotForm: true
      },
      data: perChartConfig
    });
  }

  function postStatsRange(perChartConfig) {
    return $http({
      url: '/pools/default/stats/range/',
      method: 'POST',
      mnHttp: {
        group: "global",
        isNotForm: true
      },
      data: perChartConfig
    });
  }

  function getServiceNameFromDescriptionPath(descPath) {
    var splitted = descPath.split(".");
    return splitted[0].substring(1, splitted[0].length-1);
  }

  function descriptionPathToStatName(descPath, items) {
    if (mnPoolDefault.export.compat.atLeast70) {
      return descPath;
    } else {
      let splitted = descPath.split(".");
      let service = getServiceNameFromDescriptionPath(descPath);
      let maybeItem = descPath.includes("@items") && ((items || {})[service])
      return (maybeItem || "") + splitted[splitted.length - 1];
    }
  }

  function getStatsProp(stats) {
    return Array.isArray(stats) ? stats : Object.keys(stats);
  }

  function descriptionPathsToStatNames(stats, items) {
    return getStatsProp(stats).map(function (descPath) {
      return descriptionPathToStatName(descPath, items);
    });
  }


  function zoomToMS(zoom) {
    switch (zoom) {
    case "minute": return 60000;
    case "hour": return 3600000;
    case "day": return 86400000;
    case "week": return 604800000;
    case "month": return 2628000000;
    default: return zoom ? zoom : 60000;
    }
  }

  function zoomToStep(zoom) {
    if (Number.isFinite(zoom)) {
      return zoom;
    }
    return zoomToMS(zoom) / 60000;
  }

  function rangeZoomToSec(zoom) {
    switch(zoom){
    case "minute": return zoomToMS(zoom) / 1000 * 2;
    default: return zoomToMS(zoom) / 1000;
    }
  }

  function rangeZoomToStep(zoom) {
    switch (zoom) {
    case "minute": return 10;
    case "hour":
    case "day":
    case "week":
    case "month": return zoomToMS(zoom) / 1000 / 100; //100 - how many steps we have
    default: return zoom ? (zoom / 1000) : 10;
    }
  }

  function get70StatUniqueName(cfg) {
    return cfg.metric.name + (cfg.applyFunctions ? ("_" + cfg.applyFunctions.join("")) : "");
  }

  function packStatsConfig(config, doNotAssignStatPath) {
    let cfg = {};

    if (config.node !== "all") {
      cfg.nodes = [config.node];
    }

    if (mnPoolDefault.export.compat.atLeast70) {
      let rv = [];

      switchToFullStatInfo(cfg, config);

      getStatsProp(config.stats).forEach(statPath => {
        let statDesc = readByPath(statPath);
        if (!statDesc) {
          return;
        }
        let cfg1 = Object.assign(doNotAssignStatPath ? {} : {statPath: statPath}, cfg);
        let service = getServiceNameFromDescriptionPath(statPath);
        cfg1.metric = Object.assign({}, statDesc.metric);
        if (isPerBucketStat(statPath)) {
          if (statDesc.bucket === null) {
            delete cfg1.metric.bucket;
          } else {
            cfg1.metric[statDesc.bucketLabel || "bucket"] = config.bucket;
          }
        }
        if (config.scope) {
          cfg1.metric.scope = config.scope;
        }
        if (config.collection) {
          cfg1.metric.collection = config.collection;
        }
        if (config.node == "all" && !config.specificStat) {
          cfg1.nodesAggregation = statDesc.nodesAggregation;
        }
        if (statDesc.applyFunctions || config.applyFunctions) {
          cfg1.applyFunctions = statDesc.applyFunctions || config.applyFunctions;
        }
        if (statPath.includes("@items")) {
          cfg1.metric[service] = config.items[service];
        }
        let httpParamsModifier = mnStatisticsDescription.maybeGetLabelsModifier(service);
        if (httpParamsModifier) {
          cfg1 = httpParamsModifier(cfg1);
        }
        cfg1.metric = Object.keys(cfg1.metric).map(labelName => {
          let rv = {};
          let labelValue = cfg1.metric[labelName];
          if (labelValue) {
            rv.label = labelName;
            rv.value = labelValue;
          }
          let operator = mnStatisticsDescription.maybeGetLabelOperator(statPath+"."+labelName);
          if (operator) {
            rv.operator = operator;
          }
          return rv;
        });
        if (config.alignTimestamps) {
          cfg1.alignTimestamps = true;
        }
        rv.push(cfg1);
      });
      return rv;
    } else {
      cfg.step = config.step || zoomToStep(config.zoom);
      cfg.startTS = 0 - zoomToMS(config.zoom);
      cfg.stats = descriptionPathsToStatNames(config.stats, config.items);
      cfg.bucket = config.bucket;
      if (config.node == "all" && !config.specificStat) {
        cfg.aggregate = true;
      }
      return [cfg];
    }
  }

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

  function doAddPresetScenario() {
    presetScenario().forEach(function (scenario) {
      scenario.preset = true;
      scenario.groups = scenario.groups.map(function (group) {
        group.preset = true;
        group.charts = group.charts.map(function (chart) {
          chart.preset = true;
          chart = mnStoreService.store("charts").add(chart);
          return chart.id;
        });
        group = mnStoreService.store("groups").add(group);
        return group.id;
      });
      mnStoreService.store("scenarios").add(scenario);
    });
  }

  function presetScenario() {
    return [{
      name: "Cluster Overview",
      uiid: "mn-cluster-overview",
      desc: "Stats showing the general health of your cluster. Customize and/or make your own dashboard with \"new dashboard... \" below.",
      groups: [{
        name: "Cluster Overview",
        uiid: "mn-cluster-overview-group",
        charts: [{
          stats: {"@kv-.ops": true,
                  "@query.query_requests": true,
                  "@fts-.@items.total_queries": true,
                  "@kv-.ep_tmp_oom_errors": true,
                  "@kv-.ep_cache_miss_rate": true,
                  "@kv-.cmd_get": true,
                  "@kv-.cmd_set": true,
                  "@kv-.delete_hits": true
                 },
          size: "medium",
          specificStat: false // false for multi-stat chart
        }, {
          stats: {"@kv-.mem_used": true,
                  "@kv-.ep_mem_low_wat": true,
                  "@kv-.ep_mem_high_wat": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.curr_items": true,
                  "@kv-.vb_replica_curr_items": true,
                  "@kv-.vb_active_resident_items_ratio": true,
                  "@kv-.vb_replica_resident_items_ratio": true,
                "@kv-.couch_docs_fragmentation": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.disk_write_queue": true,
                  "@kv-.couch_docs_actual_disk_size": true},
          size: "small",
          specificStat: false
        }, {
          stats: {"@kv-.ep_dcp_replica_items_remaining": true},
          size: "small",
          specificStat: false
        }, {
          stats: {"@kv-.ep_data_read_failed": true,
                  "@kv-.ep_data_write_failed": true,
                  "@query.query_errors": true,
                  "@fts-.@items.total_queries_error": true,
                  "@eventing.eventing/failed_count": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@query.query_requests_250ms": true,
                  "@query.query_requests_500ms": true,
                  "@query.query_requests_1000ms": true,
                  "@query.query_requests_5000ms": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@xdcr-.replication_changes_left": true,
                  "@index-.@items.num_docs_pending+queued": true,
                  "@fts-.@items.num_mutations_to_index": true},
          size: "medium",
          specificStat: false
        }]
      }, {
        name: "Node Resources",
        charts: [{
          stats: {"@system.cpu_utilization_rate": true},
          size: "medium",
          specificStat: true // for single-stat chart
        }, {
          stats: {"@system.rest_requests": true},
          size: "medium",
          specificStat: true
        }, {
          stats: {"@system.mem_actual_free": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@system.swap_used": true},
          size: "medium",
          specificStat: true
        }]
      }]
    },{  // 2nd scenario starts here with the comma ///////////////////////

      name: "All Services",
      uiid: "mn-all-services",
      desc: "Most common stats, arranged per service. Customize and/or make your own dashboard with \"new dashboard... \" below.",
      groups: [{
        name: "Data (Docs/Views/XDCR)",
        uiid: "mn-all-services-data-group",
        charts: [{
          stats: {"@kv-.mem_used": true,
                  "@kv-.ep_mem_low_wat": true,
                  "@kv-.ep_mem_high_wat": true,
                  "@kv-.ep_kv_size": true,
                  "@kv-.ep_meta_data_memory": true,
                  "@kv-.vb_active_resident_items_ratio": true},
          size: "medium",
          specificStat: false // false for multi-stat chart
        }, {
          stats: {"@kv-.ops": true,
                  "@kv-.ep_cache_miss_rate": true,
                  "@kv-.cmd_get": true,
                  "@kv-.cmd_set": true,
                  "@kv-.delete_hits": true,
                  "@kv-.ep_num_ops_set_meta": true
                 },
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.ep_dcp_views+indexes_items_remaining": true,
                  "@kv-.ep_dcp_cbas_items_remaining": true,
                  "@kv-.ep_dcp_replica_items_remaining": true,
                  "@kv-.ep_dcp_xdcr_items_remaining": true,
                  "@kv-.ep_dcp_eventing_items_remaining": true,
                  "@kv-.ep_dcp_other_items_remaining": true,
                  "@xdcr-.replication_changes_left": true
                 },
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.ep_bg_fetched": true,
                  "@kv-.ep_data_read_failed": true,
                  "@kv-.ep_data_write_failed": true,
                  "@kv-.ep_ops_create": true,
                  "@kv-.ep_ops_update": true
                 },
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.ep_diskqueue_items": true},
          size: "small",
          specificStat: true
        }]
      }, {
        name: "Query",
        charts: [{
          stats: {"@query.query_requests_1000ms": true,
                  "@query.query_requests_500ms": true,
                  "@query.query_requests_5000ms": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@query.query_selects": true,
                  "@query.query_requests": true,
                  "@query.query_warnings": true,
                  "@query.query_invalid_requests": true,
                  "@query.query_errors": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@query.query_avg_req_time": true,
                  "@query.query_avg_svc_time": true},
          size: "small",
          specificStat: false
        }, {
          stats: {"@query.query_avg_result_count": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@query.query_avg_response_size": true},
          size: "small",
          specificStat: false
        }]
      }, {
        name: "Index",
        charts: [{
          stats: {"@index-.index/num_rows_returned": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@index-.@items.num_docs_pending+queued": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@index-.index/data_size": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@index-.index/disk_size": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@index.index_ram_percent": true,
                  "@index.index_remaining_ram": true,
                  "@index-.index/data_size": true,
                  "@index-.index/disk_size": true},
          size: "medium",
          specificStat: false
        }]
      }, {
        name: "Search",
        charts: [{
          stats: {"@fts-.fts/num_bytes_used_disk": true,
                  "@fts.fts_num_bytes_used_ram": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@fts-.@items.total_queries": true,
                  "@fts-.@items.total_queries_error": true,
                  "@fts-.@items.total_queries_slow": true,
                  "@fts-.@items.total_queries_timeout": true,
                  "@fts.fts_total_queries_rejected_by_herder": true},
          size: "medium",
          specificStat: false
        }]
      }, {
        name: "Analytics",
        enterprise: true,
        charts: [{
          stats: {"@cbas-.cbas/incoming_records_count": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas-.cbas_failed_to_parse_records_count": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas-.cbas/failed_at_parser_records_count_total": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_heap_used": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_heap_memory_committed_bytes": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_thread_count": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_disk_used": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_io_reads": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_io_writes": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_system_load_average": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_pending_merge_ops": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@cbas.cbas_pending_flush_ops": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@system.sysproc_mem_resident_java_cbas": true},
          size: "small",
          specificStat: true
        }]
      }, {
        name: "Eventing",
        enterprise: true,
        charts: [{
          stats: {"@eventing.eventing/failed_count": true,
                  "@eventing.eventing/timeout_count": true},
          size: "small",
          specificStat: false
        }]
      },  {
        name: "XDCR",
        charts: [{
          stats: {"@xdcr-.replication_changes_left": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@xdcr-.@items.changes_left": true},
          size: "small",
          specificStat: true
        }, {
          stats: {"@xdcr-.@items.wtavg_docs_latency": true,
                  "@xdcr-.@items.wtavg_meta_latency": true},
          size: "small",
          specificStat: false
        }, {
          stats: {"@xdcr-.@items.docs_failed_cr_source": true,
                  "@xdcr-.@items.xdcr_docs_failed_cr_target_total": true,
                  "@xdcr-.@items.docs_filtered": true},
          size: "small",
          specificStat: false
        }]
      }, {
        name: "vBucket Resources",
        charts: [{
          stats: {"@kv-.vb_active_num": true,
                  "@kv-.vb_replica_num": true,
                  "@kv-.vb_pending_num": true,
                  "@kv-.ep_vb_total": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.curr_items": true,
                  "@kv-.vb_replica_curr_items": true,
                  "@kv-.vb_pending_curr_items": true,
                  "@kv-.curr_items_tot": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.vb_active_resident_items_ratio": true,
                  "@kv-.vb_replica_resident_items_ratio": true,
                  "@kv-.vb_pending_resident_items_ratio": true,
                  "@kv-.ep_resident_items_rate": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.vb_active_ops_create": true,
                  "@kv-.vb_replica_ops_create": true,
                  "@kv-.vb_pending_ops_create": true,
                  "@kv-.ep_ops_create": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.vb_active_eject": true,
                  "@kv-.vb_replica_eject": true,
                  "@kv-.vb_pending_eject": true,
                  "@kv-.ep_num_value_ejects": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.vb_active_itm_memory": true,
                  "@kv-.vb_replica_itm_memory": true,
                  "@kv-.vb_pending_itm_memory": true,
                  "@kv-.ep_kv_size": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.vb_active_meta_data_memory": true,
                  "@kv-.vb_replica_meta_data_memory": true,
                  "@kv-.vb_pending_meta_data_memory": true,
                  "@kv-.ep_meta_data_memory": true},
          size: "medium",
          specificStat: false
        }]
      }, {
        name: "DCP Queues",
        charts: [{
          stats: {"@kv-.ep_dcp_views+indexes_count": true,
                  "@kv-.ep_dcp_cbas_count": true,
                  "@kv-.ep_dcp_replica_count": true,
                  "@kv-.ep_dcp_xdcr_count": true,
                  "@kv-.ep_dcp_eventing_count": true,
                  "@kv-.ep_dcp_other_count": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.ep_dcp_views+indexes_producer_count": true,
                  "@kv-.ep_dcp_cbas_producer_count": true,
                  "@kv-.ep_dcp_replica_producer_count": true,
                  "@kv-.ep_dcp_xdcr_producer_count": true,
                  "@kv-.ep_dcp_eventing_producer_count": true,
                  "@kv-.ep_dcp_other_producer_count": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.ep_dcp_views+indexes_items_remaining": true,
                  "@kv-.ep_dcp_cbas_items_remaining": true,
                  "@kv-.ep_dcp_replica_items_remaining": true,
                  "@kv-.ep_dcp_xdcr_items_remaining": true,
                  "@kv-.ep_dcp_eventing_items_remaining": true,
                  "@kv-.ep_dcp_other_items_remaining": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.ep_dcp_views+indexes_items_sent": true,
                  "@kv-.ep_dcp_cbas_items_sent": true,
                  "@kv-.ep_dcp_replica_items_sent": true,
                  "@kv-.ep_dcp_xdcr_items_sent": true,
                  "@kv-.ep_dcp_eventing_items_sent": true,
                  "@kv-.ep_dcp_other_items_sent": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.ep_dcp_views+indexes_total_bytes": true,
                  "@kv-.ep_dcp_cbas_total_bytes": true,
                  "@kv-.ep_dcp_replica_total_bytes": true,
                  "@kv-.ep_dcp_xdcr_total_bytes": true,
                  "@kv-.ep_dcp_eventing_total_bytes": true,
                  "@kv-.ep_dcp_other_total_bytes": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.ep_dcp_views+indexes_backoff": true,
                  "@kv-.ep_dcp_cbas_backoff": true,
                  "@kv-.ep_dcp_replica_backoff": true,
                  "@kv-.ep_dcp_xdcr_backoff": true,
                  "@kv-.ep_dcp_eventing_backoff": true,
                  "@kv-.ep_dcp_other_backoff": true},
          size: "medium",
          specificStat: false
        }]
      }, {
        name: "Disk Queues",
        charts: [{
          stats: {"@kv-.ep_diskqueue_fill": true,
                  "@kv-.ep_diskqueue_drain": true,
                  "@kv-.ep_diskqueue_items": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.vb_active_queue_fill": true,
                  "@kv-.vb_active_queue_drain": true,
                  "@kv-.vb_active_queue_size": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.vb_replica_queue_fill": true,
                  "@kv-.vb_replica_queue_drain": true,
                  "@kv-.vb_replica_queue_size": true},
          size: "medium",
          specificStat: false
        }, {
          stats: {"@kv-.vb_pending_queue_fill": true,
                  "@kv-.vb_pending_queue_drain": true,
                  "@kv-.vb_pending_queue_size": true},
          size: "medium",
          specificStat: false
        }]
      }]
    }]
  }
}
