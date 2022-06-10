/*
  Copyright 2020-Present Couchbase, Inc.

  Use of this software is governed by the Business Source License included in
  the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
  file, in accordance with the Business Source License, use of this software
  will be governed by the Apache License, Version 2.0, included in the file
  licenses/APL2.txt.
*/

import angular from "angular";
import _ from "lodash";

import mnPools from "../components/mn_pools.js";
import mnFilters from "../components/mn_filters.js";
import mnPermissions from "../components/mn_permissions.js";
import mnTasksDetails from "../components/mn_tasks_details.js";
import mnPoolDefault from "../components/mn_pool_default.js";

import mnBucketsService from "./mn_buckets_service.js";
import mnAnalyticsService from "./mn_analytics_service.js";
import mnViewsListService from "./mn_views_list_service.js";
import mnSettingsClusterService from "./mn_settings_cluster_service.js";
import mnSettingsAutoFailoverService from "./mn_settings_auto_failover_service.js";
import mnSettingsAutoCompactionService from "./mn_settings_auto_compaction_service.js";
import mnGsiService from "./mn_gsi_service.js";
import mnAuditService from "./mn_audit_service.js";
import mnUserRolesService from "./mn_user_roles_service.js";
import mnXDCRService from "./mn_xdcr_service.js";

export default 'mnSettingsNotificationsService';

angular.module('mnSettingsNotificationsService', [
  mnPools,
  mnFilters,
  mnPermissions,
  mnTasksDetails,
  mnPoolDefault,
  mnBucketsService,
  mnAnalyticsService,
  mnViewsListService,
  mnSettingsClusterService,
  mnSettingsAutoFailoverService,
  mnSettingsAutoCompactionService,
  mnGsiService,
  mnAuditService,
  mnUserRolesService,
  mnXDCRService
]).factory('mnSettingsNotificationsService', ["$http", "mnPoolDefault", "mnBucketsService", "mnPools", "$q", "$window", "mnAnalyticsService", "mnViewsListService", "mnGsiService", "mnAuditService", "mnMBtoBytesFilter", "mnPermissions", "mnSettingsClusterService", "mnSettingsAutoFailoverService", "mnSettingsAutoCompactionService", "mnTasksDetails", "mnXDCRService", "mnUserRolesService", "mnStatsServiceDowngraded", function ($http, mnPoolDefault, mnBucketsService, mnPools, $q, $window, mnAnalyticsService, mnViewsListService, mnGsiService, mnAuditService, mnMBtoBytesFilter, mnPermissions, mnSettingsClusterService, mnSettingsAutoFailoverService, mnSettingsAutoCompactionService, mnTasksDetails, mnXDCRService, mnUserRolesService, mnStatsServiceDowngraded) {
  var mnSettingsNotificationsService = {};

  function sumWithoutNull(array, average) {
    if (!array) {
      return 0;
    }
    array = _.without(array, null);
    if (!array.length) {
      return 0;
    }
    var rv = _.reduce(array, function (memo, num) {
      return memo + num;
    }, 0);
    return average ? rv / array.length : rv;
  }



  function buildPhoneHomeThingy(source) {
    var bucketsList = source[0];
    var perBucketStats = source[1];
    var pools = source[2];
    var poolsDefault = source[3];
    var indexStatus = source[4];
    var auditSettings = source[5];
    var indexSettings = source[6];
    var autoFailoverSettings = source[7];
    var autoCompactionSettings = source[8];
    var remotes = source[9];
    var xdcr_tasks = source[10];
    var eventing = source[11];
    var analytics = source[12];
    var ldapSettings = source[13];


    function getAvgPerItem(items, filter) {
      var avgs = [];
      _.each(items, function (item, key) {
        if (filter(key)) {
          avgs.push(sumWithoutNull(item, true));
        }
      });
      return avgs;
    }
    function precision(val) {
      return Number(val.toFixed(5));
    }

    function getHourFromWeek(value) {
      return value / 11520 * 60;
    }

    function calculateAvgWeekAndHour(stats, name, avg) {
      var weekName = name + "_last_week";
      var hourName = name + "_last_hour";
      if (stats.istats[weekName].length) {
        stats.istats[weekName] = sumWithoutNull(stats.istats[weekName], avg);
        stats.istats[hourName] = getHourFromWeek(stats.istats[weekName]);
        stats.istats[weekName] = precision(stats.istats[weekName]);
        stats.istats[hourName] = precision(stats.istats[hourName]);
      } else {
        stats.istats[weekName] = 0;
        stats.istats[hourName] = 0;
      }
    }

    function setPerBucketStat(stats, name, value) {
      if (value) {
        var weekName = name + "_last_week";
        stats.istats[weekName] = stats.istats[weekName].concat(value);
      }
    }

    let ram = poolsDefault.storageTotals.ram;
    let hdd = poolsDefault.storageTotals.hdd;

    var stats = {
      cluster_settings: {},
      packageVariant: pools.packageVariant,
      version: pools.implementationVersion,
      componentsVersion: pools.componentsVersion,
      uuid: pools.uuid,
      isOperatorManaged: !!poolsDefault.nodes.find((node) => {
        //should match to 1234-0000.1234.my-namespace.svc
        var levels = node.otpNode.split("@")[1].split(".");
        if ((levels.length == 4) && (levels[3] == "svc")) {
          return levels[0].startsWith(levels[1] + "-");
        }
        return false;
      }),
      numNodes: poolsDefault.nodes.length, //Total number of nodes
      isEnterpriseEdition: pools.isEnterprise,
      adminLDAPEnabled : poolsDefault.saslauthdEnabled ||
        ldapSettings && ldapSettings.data.authenticationEnabled,
      ram: {
        total: ram ? ram.total : 0,
        quotaTotal: ram ? ram.quotaTotal : 0,
        quotaUsed: ram ? ram.quotaUsed : 0 ,
        indexMemoryQuota: mnMBtoBytesFilter(poolsDefault.indexMemoryQuota)
      },
      hdd: {
        total: hdd ? hdd.total : 0,
        quotaTotal: hdd ? hdd.quotaTotal : 0,
        used: hdd ? hdd.used : 0,
        usedByData: hdd ? hdd.usedByData : 0
      },
      buckets: { //Number of buckets
        total: bucketsList.length,
        magma: bucketsList.byType.membase
          .filter((bucket) => bucket.storageBackend === "magma").length,
        couchstore: bucketsList.byType.membase
          .filter((bucket) => bucket.storageBackend === "couchstore").length,
        membase: bucketsList.byType.membase.length,
        memcached: bucketsList.byType.memcached.length,
        ephemeral: bucketsList.byType.ephemeral.length
      },
      counters: poolsDefault.counters,
      nodes: {
        os: [],
        uptime: [],
        istats: [],
        services: {} //Services running and counts
      },
      xdcr: {},
      browser: $window.navigator.userAgent
    };

    // get XDCR remote cluster info
    if (remotes && remotes.filtered) {
      stats.xdcr.remotes = remotes.filtered.map(function (remote) {
        return {
          name: remote.name,
          uuid: remote.uuid
        };
      });
    }

    // also get information about each XDCR replication
    if (xdcr_tasks && xdcr_tasks.tasksXDCR) {
      stats.xdcr.replications = xdcr_tasks.tasksXDCR.map(function (task) {
        return {
          source_bucket: task.source,
          dest_bucket: task.target.split('buckets/')[1],
          dest_cluster: task.id.split('/')[0],
          filter: task.filterExpression,
          protocol: task.replicationType,
          status: task.status
        };
      });
    }

    // eventing - functions and nodes
    if (eventing) {
      stats.eventing = {num_eventing_nodes: eventing.num_eventing_nodes};
      if (eventing.apps) {
        stats.eventing.num_apps = eventing.apps.length;
        stats.eventing.apps = eventing.apps.map(function (app) {
          return {
            composite_status: app.composite_status,
            num_deployed_nodes: app.num_deployed_nodes
          };
        });
      }
    }

    // analytics
    stats.analytics = analytics;
    var i;
    for(i in poolsDefault.nodes) {
      stats.nodes.os.push(poolsDefault.nodes[i].os);
      stats.nodes.uptime.push(poolsDefault.nodes[i].uptime);
      stats.nodes.istats.push(poolsDefault.nodes[i].interestingStats);
      var servicesContainerName = poolsDefault.nodes[i].services.sort().join(',');
      if (!stats.nodes.services[servicesContainerName]) {
        stats.nodes.services[servicesContainerName] = 0;
      }
      stats.nodes.services[servicesContainerName] ++;
    }

    if (mnPoolDefault.export.compat.atLeast70) {
      let dayStats = perBucketStats[0];
      let hourStats = perBucketStats[1];
      stats.istats = {};

      ([
        "avg_ops",
        "avg_cmd_set",
        "avg_query_requests",
        "total_avg_view_accesses",
        "total_avg_index_num_rows_returned",
        "vb_active_sync_write_committed_count",
        "total_curr_items_tot",
        "kv_vb_sync_write_accepted_count"
      ]).map(function (stat, index) {
        let day = dayStats[index].data[0];
        let hour = hourStats[index].data[0];
        stats.istats[stat + "_last_day"] = Number((day && day.values[1]) || 0);
        stats.istats[stat + "_last_hour"] = Number((hour && hour.values[1]) || 0);
      });

    } else {
      stats.istats = {
        avg_ops_last_week: [], // Average ops / sec last week
        avg_cmd_set_last_week: [], // Average sets / sec last week
        avg_query_requests_last_week: [], //Average N1QL queries / sec last week
        total_avg_view_accesses_last_week: [], //Average view reads / sec last week
        total_avg_index_num_rows_returned_last_week: [], //Average scans/sec last week
        vb_active_sync_write_committed_count_last_week: [],
        total_indexes: 0, //Number of total indexes
        total_curr_items_tot: 0, //Total number of items across all buckets
        total_fts_indexes: 0
      };
      let interestedBuckets = bucketsList.byType.membase.concat(bucketsList.byType.ephemeral);
      _.each(perBucketStats, function (perBucketStat, index) {
        var bucketName = interestedBuckets[index].name;
        var statsInfo = perBucketStats[index].data;
        if (!statsInfo) {
          return;
        }
        var bucketStats = statsInfo.stats["@kv-" + bucketName];
        var indexStats = statsInfo.stats["@index-" + bucketName];
        var queriesStats = statsInfo.stats["@query"];
        var ftsStats = statsInfo.stats["@fts-" + bucketName];

        if (ftsStats) {
          stats.istats.total_fts_indexes += _.keys(_.reduce(ftsStats, function (result, value, key) {
            key = key.split("/");
            if (key.length === 3) {
              result[key[1]] = true;
            }
            return result;
          }, {})).length;
        }

        var avgNumRowsReturnedPerIndex = getAvgPerItem(indexStats, function (key) {
          key = key.split("/");
          return key.length === 3 && key[2] === "num_rows_returned" && key[0] === "index";
        });
        var avgViewAccessesPerView = getAvgPerItem(bucketStats, function (key) {
          key = key.split("/");
          return key.length === 3 && key[2] === "accesses" && key[0] === "views";
        });

        setPerBucketStat(stats, "avg_ops",
                         bucketStats.ops);
        setPerBucketStat(stats, "avg_cmd_set",
                         bucketStats.cmd_set);
        setPerBucketStat(stats, "vb_active_sync_write_committed_count",
                         bucketStats.vb_active_sync_write_committed_count);
        setPerBucketStat(stats, "total_avg_view_accesses",
                         bucketStats && avgViewAccessesPerView);
        setPerBucketStat(stats, "total_avg_index_num_rows_returned",
                         indexStats && avgNumRowsReturnedPerIndex);

        stats.istats.avg_query_requests_last_week =
          (queriesStats && queriesStats.query_requests) || []; //is not per bucket
        stats.istats.total_curr_items_tot += bucketStats.curr_items_tot ?
          bucketStats.curr_items_tot[bucketStats.curr_items_tot.length - 1] : 0;
      });
      calculateAvgWeekAndHour(stats, "avg_ops", true);
      calculateAvgWeekAndHour(stats, "avg_cmd_set", true);
      calculateAvgWeekAndHour(stats, "avg_query_requests", true);
      calculateAvgWeekAndHour(stats, "vb_active_sync_write_committed_count", true);
      calculateAvgWeekAndHour(stats, "total_avg_view_accesses");
      calculateAvgWeekAndHour(stats, "total_avg_index_num_rows_returned");
    }

    if (indexStatus) {
      stats.istats.total_indexes = indexStatus.indexes.length;
    }

    if (autoCompactionSettings) {
      stats.cluster_settings.compaction = {
        database_trigger_percent_enabled: !!autoCompactionSettings.databaseFragmentationThreshold.percentageFlag,
        database_trigger_percent_level: autoCompactionSettings.databaseFragmentationThreshold.percentage,
        database_trigger_size_enabled: !!autoCompactionSettings.databaseFragmentationThreshold.sizeFlag,
        database_trigger_size_MB: autoCompactionSettings.databaseFragmentationThreshold.size,
        view_trigger_percent_enabled: !!autoCompactionSettings.viewFragmentationThreshold.percentageFlag,
        view_trigger_percent_level: autoCompactionSettings.viewFragmentationThreshold.percentage,
        view_trigger_size_enabled: !!autoCompactionSettings.viewFragmentationThreshold.sizeFlag,
        view_trigger_size_MB: autoCompactionSettings.viewFragmentationThreshold.size,
        compaction_trigger_time_based_enabled: !!autoCompactionSettings.allowedTimePeriodFlag,
        compaction_trigger_time_based_start_time: {
          hour: autoCompactionSettings.allowedTimePeriod.fromHour,
          minute: autoCompactionSettings.allowedTimePeriod.fromMinute
        },
        index_trigger_percent_enabled: !autoCompactionSettings.indexCircularCompactionFlag,
        index_trigger_percent_level: autoCompactionSettings.indexFragmentationThreshold.percentage,
        index_trigger_circular_reuse_enabled: autoCompactionSettings.indexCircularCompactionFlag,
        index_trigger_circular_reuse_days: autoCompactionSettings.indexCircularCompactionDaysOfWeek,
        index_trigger_circular_reuse_start_time: {
          hour: autoCompactionSettings.indexCircularCompaction.fromHour,
          minute: autoCompactionSettings.indexCircularCompaction.fromMinute
        }
      };
    }
    if (autoFailoverSettings) {
      stats.cluster_settings.enable_auto_failover = autoFailoverSettings.enabled;
      stats.cluster_settings.failover_timeout = autoFailoverSettings.timeout;
    }
    if (indexSettings) {
      stats.cluster_settings.index_storage_mode = indexSettings.storageMode;
    }
    if (auditSettings) {
      stats.adminAuditEnabled = auditSettings.auditdEnabled;
    }

    return stats;
  }

  mnSettingsNotificationsService.buildPhoneHomeThingy = function (mnHttpParams) {
    return $q.all([
      mnBucketsService.getBucketsByType(mnHttpParams),
      mnPools.get(mnHttpParams),
      mnPoolDefault.get(undefined, mnHttpParams)
    ]).then(function (resp) {
      var buckets = resp[0];
      var pools = resp[1];
      var poolDefault = resp[2];

      var queries = [
        $q.when(buckets)
      ];
      if (mnPoolDefault.export.compat.atLeast70) {
        //avg_ops_last_week
        let avgCommonSettingsDay = {
          nodesAggregation: "sum",
          applyFunctions: ["rate", "sum"],
          start: -1,
          timeWindow: "1d"
        };
        let avgCommonSettingsHour = Object.assign({}, avgCommonSettingsDay, {
          timeWindow: "1h"
        });
        let interestingStats = [{
          metric: [{label: "name", value: "kv_ops"}]//avg_ops
        }, {
          metric: [{label: "name", value: "kv_ops"},
                   {label: "op", value: "set"}]//avg_cmd_set / kv_cmd_set
        }, {
          metric: [{label: "name", value: "n1ql_requests"}]//avg_query_requests
        }, {
          metric: [{label: "name", value: "couch_views_ops"}]//total_avg_view_accesses
        }, {
          metric: [{label: "name", value: "index_num_rows_returned"}]//total_avg_index_num_rows_returned
        }, {
          metric: [{label: "name", value: "kv_vb_sync_write_committed_count"},
                   {label: "state", value: "active"}]//vb_active_sync_write_committed_count_last_week
        }, {
          applyFunctions: ["sum"],
          metric: [{label: "name", value: "kv_curr_items_tot"}]//kv_curr_items_tot
        }, {
          applyFunctions: ["sum"],
          metric: [{label: "name", value: "kv_vb_sync_write_accepted_count"},
                   {label: "state", value: "active"}]//kv_vb_sync_write_accepted_count
        }];
        let dayStatsConfigs = interestingStats.map(metric => {
          return Object.assign({}, avgCommonSettingsDay, metric);
        });
        let hoursStatsConfigs = interestingStats.map(metric => {
          return Object.assign({}, avgCommonSettingsHour, metric);
        });

        queries.push($q.all([
          mnStatsServiceDowngraded.postStatsRange(dayStatsConfigs).toPromise(),
          mnStatsServiceDowngraded.postStatsRange(hoursStatsConfigs).toPromise()
        ]));
      } else {
        var perBucketQueries =
            buckets.byType.membase.concat(buckets.byType.ephemeral).map(function (bucket) {
              return mnAnalyticsService.doGetStats({
                $stateParams: {
                  zoom: "week",
                  bucket: bucket.name
                }
              }, mnHttpParams);
            });
        queries.push($q.all(perBucketQueries));
      }
      queries.push(pools);
      queries.push(poolDefault);

      if (mnPermissions.export.cluster.collection['.:.:.'].n1ql.index.read) {
        queries[4] = mnGsiService.getIndexStatus(mnHttpParams);
      }
      if (mnPools.export.isEnterprise && mnPermissions.export.cluster.admin.security.read) {
        queries[5] = mnAuditService.getAuditSettings();
      }
      if (mnPermissions.export.cluster.settings.indexes.read) {
        queries[6] = mnSettingsClusterService.getIndexSettings();
      }
      if (mnPermissions.export.cluster.settings.read) {
        queries[7] = mnSettingsAutoFailoverService.getAutoFailoverSettings();
      }
      if (mnPermissions.export.cluster.settings.read) {
        queries[8] = mnSettingsAutoCompactionService.getAutoCompaction();
      }

      // collect info about XDCR
      if (mnPermissions.export.cluster.xdcr.remote_clusters.read) {
        queries[9] = mnXDCRService.getReplicationState().then(null, () => ({}))
      }

      if (mnPermissions.export.cluster.tasks.read) {
        queries[10] = mnTasksDetails.get(mnHttpParams);
      }

      // do we have an eventing service? If so, see how it is used
      if (poolDefault.nodes.some(function(node) {
        return(_.indexOf(node.services, 'eventing') > -1);
      })) {
        queries[11] = mnSettingsNotificationsService.getEventingData();
      }

      // do we have an analytics service? If so, get some information about it.
      if (poolDefault.nodes.some(function(node) {
        return _.indexOf(node.services, 'cbas') > -1;
      })) {
        queries[12] = mnSettingsNotificationsService.getCbasData();
      }

      if (poolDefault.compat.atLeast65 && poolDefault.isEnterprise &&
          mnPermissions.export.cluster.admin.security.external.read) {
        queries[13] = mnUserRolesService.getLdapSettings();
      }
      return $q.all(queries).then(buildPhoneHomeThingy);
    });
  };

  mnSettingsNotificationsService.getCbasData = function() {
    return $http.post('/_p/cbas/query/service',
                      {statement:
                       'with user_datasets as (select value d from Metadata.`Dataset` d ' +
                       'where d.DataverseName <> "Metadata") select ' +
                       '(select value count(*) from user_datasets d group by d.BucketName) as datasets_per_bucket, ' +
                       '(select value count(*) from user_datasets d group by d.DataverseName) as datasets_per_dataverse, ' +
                       '(select value count(distinct d.UUID) from Metadata.`Bucket` d where d.IsRunning) as connected_buckets;'}
                      ,{ headers: {'ignore-401': 'true', 'Analytics-Priority': '-1'}})
      .then(function (resp) {
        if (resp && resp.data && _.isArray(resp.data.results) && resp.data.results[0])
          return(resp.data.results[0]);
      })
      .catch(angular.noop);
  };

  mnSettingsNotificationsService.getEventingData = function() {
    return $http.get('/_p/event/api/v1/status')
      .then(function (resp) {if (resp && resp.data) return resp.data})
      .catch(angular.noop);
  };

  mnSettingsNotificationsService.getUpdates = function (data, mnHttpParams) {
    return $http({
      method: 'JSONP',
      mnHttp: mnHttpParams,
      url: 'https://ph.couchbase.net/v2',
      timeout: 8000,
      params: {launchID: data.launchID, version: data.version}
    });
  };

  mnSettingsNotificationsService.maybeCheckUpdates = function (mnHttpParams) {
    return mnSettingsNotificationsService.getSendStatsFlag(mnHttpParams).then(function (sendStatsData) {
      sendStatsData.enabled = sendStatsData.sendStats;
      if (!sendStatsData.sendStats) {
        return sendStatsData;
      } else {
        return mnPools.get(mnHttpParams).then(function (pools) {
          return mnSettingsNotificationsService.getUpdates({
            launchID: pools.launchID,
            version: pools.implementationVersion
          }, mnHttpParams).then(function (resp) {
            return _.extend(_.clone(resp.data), sendStatsData);
          }, function () {
            return sendStatsData;
          });
        });
      }
    })
  };

  mnSettingsNotificationsService.saveSendStatsFlag = function (flag) {
    return $http.post("/settings/stats", {sendStats: flag});
  };
  mnSettingsNotificationsService.getSendStatsFlag = function (mnHttpParams) {
    return $http({
      method: "GET",
      url: "/settings/stats",
      mnHttp: mnHttpParams
    }).then(function (resp) {
      return resp.data;
    });
  };


  return mnSettingsNotificationsService;
}]);
