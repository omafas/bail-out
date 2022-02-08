/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import _ from 'lodash';
import {min as d3Min, max as d3Max} from 'd3-array';
import uiRouter from '@uirouter/angularjs';

import mnFilters from "../components/mn_filters.js";
import mnHelper from "../components/mn_helper.js";

import mnD3Service from "./mn_d3_service.js";
import mnStatisticsNewService from "./mn_statistics_service.js";
import mnMultiChartDirective from "./mn_multi_chart_directive.js";

import mnPoolDefault from "../components/mn_pool_default.js";
import template from "./mn_statistics_chart_directive.html";

export default "mnStatisticsChart";

angular
  .module('mnStatisticsChart', [
    uiRouter,
    mnFilters,
    mnHelper,
    mnD3Service,
    mnStatisticsNewService,
    mnPoolDefault
  ])
  .directive("mnStatisticsChart", ["mnStatisticsNewService", "mnFormatQuantityFilter", "$state", "mnTruncateTo3DigitsFilter", "mnHelper", "mnPoolDefault", mnStatisticsNewChartDirective])
  .directive("mnMultiChart", mnMultiChartDirective);

function mnStatisticsNewChartDirective(mnStatisticsNewService, mnFormatQuantityFilter, $state, mnTruncateTo3DigitsFilter, mnHelper, mnPoolDefault) {
  return {
    restrict: 'AE',
    template,
    scope: {
      statsPoller: "=?",
      syncScope: "=?",
      config: "=",
      mnD3: "=?",
      bucket: "@",
      zoom: "@",
      node: "@?",
      items: "=?",
      api: "=?"
    },
    controller: controller
  };

  function controller($scope) {
    if (!$scope.config) {
      return;
    }

    var units;
    var options;
    let poller = $scope.statsPoller || mnStatisticsNewService.mnAdminStatsPoller;
    let step = mnStatisticsNewService.getChartStep($scope.zoom);
    let start = mnStatisticsNewService.getChartStart($scope.zoom);

    if (!_.isEmpty($scope.config.stats)) {
      units = mnStatisticsNewService.getStatsUnits($scope.config.stats);
      $scope.title = mnStatisticsNewService.getStatsTitle($scope.config.stats);
      $scope.desc = mnStatisticsNewService.getStatsDesc($scope.config.stats);
      activate();
    }

    function activate() {
      initConfig();
      subscribeToMultiChartData();
    }

    function subscribeToMultiChartData() {
      poller.subscribeUIStatsPoller({
        bucket: $scope.bucket,
        node: $scope.node || "all",
        stats: $scope.config.stats,
        items: $scope.items,
        zoom: $scope.zoom,
        specificStat: $scope.config.specificStat,
        alignTimestamps: true
      }, $scope);


      $scope.$watch("mnUIStats", onMultiChartDataUpdate);
    }

    function getChartSize(size) {
      switch (size) {
      case "tiny": return 62;
      case "small": return 102;
      case "medium": return 122;
      case "large": return 312;
      case "extra": return 432;
      default: return 122;
      }
    }

    function initConfig() {
      options = {
        step: step,
        start: start,
        isPauseEnabled: !!$scope.syncScope,
        enableAnimation: mnPoolDefault.export.compat.atLeast70 && $scope.zoom == "minute",
        is70Cluster: mnPoolDefault.export.compat.atLeast70,
        chart: {
          margin: $scope.config.margin || {top: 10, right: 36, bottom: 18, left: 44},
          height: getChartSize($scope.config.size),
          tooltip: {valueFormatter: formatValue},
          useInteractiveGuideline: true,
          yAxis: [],
          xAxis: {
            tickFormat: function (d) {
              return mnStatisticsNewService.tickMultiFormat(new Date(d));
            }
          },
          noData: "Stats are not found or not ready yet"
        }
      };

      Object.keys(units).forEach(function (unit, index) {
        units[unit] = index;
        options.chart.yAxis[index] = {};
        options.chart.yAxis[index].unit = unit;
        options.chart.yAxis[index].tickFormat = function (d) {
          return formatValue(d, unit);
        };
        options.chart.yAxis[index].domain = getScaledMinMax;
      });

      if ($scope.mnD3) {
        Object.assign(options.chart, $scope.mnD3);
      }

      $scope.options = options;
    }

    function formatValue(d, unit) {
      switch (unit) {
      case "percent": return mnTruncateTo3DigitsFilter(d) + "%";
      case "bytes": return mnFormatQuantityFilter(d, 1024);
      case "bytes/sec": return mnFormatQuantityFilter(d, 1024) + "/s";
      case "second": return mnFormatQuantityFilter(d, 1000);
      case "millisecond": return mnFormatQuantityFilter(d / 1000, 1000) + "s"
      case "millisecond/sec": return mnFormatQuantityFilter(d / 1000, 1000) + "/s";
      case "microsecond": return mnFormatQuantityFilter(d / 1000000, 1000) + "s"
      case "nanoseconds": return mnFormatQuantityFilter(d / 1000000000, 1000) + "s";
      case "number": return mnFormatQuantityFilter(d, 1000);
      case "number/sec": return mnFormatQuantityFilter(d, 1000) + "/s";
      default: return mnFormatQuantityFilter(d, 1000);
      }
    }

    function getScaledMinMax(chartData) {
      var min = d3Min(chartData, function (line) {return line.yMin/1.005;});
      var max = d3Max(chartData, function (line) {return line.yMax;});
      if (chartData[0] && chartData[0].unit == "bytes") {
        return [min <= 0 ? 0 : roundDownBytes(min), max == 0 ? 1 : roundUpBytes(max)];
      } else {
        return [min <= 0 ? 0 : roundDown(min), max == 0 ? 1 : roundUp(max)];
      }
    }

    // make 2nd digit either 0 or 5
    function roundUp(num) {
      var mag = Math.pow(10,Math.floor(Math.log10(num)));
      return(mag*Math.ceil(2*num/mag)/2);
    }

    function roundDown(num) {
      var mag = Math.pow(10,Math.floor(Math.log10(num)));
      return(mag*Math.floor(2*num/mag)/2);
    }

    function roundUpBytes(num) { // round up 3rd digit to 0
      var mag = Math.trunc(Math.log2(num)/10);
      var base_num = num/Math.pow(2,mag*10); // how many KB, MB, GB, TB, whatever
      var mag10 = Math.pow(10,Math.floor(Math.log10(base_num))-1);
      return Math.ceil(base_num/mag10) * mag10 * Math.pow(2,mag*10);
    }

    function roundDownBytes(num) {
      var mag = Math.trunc(Math.log2(num)/10);
      var base_num = num/Math.pow(2,mag*10);
      var mag10 = Math.pow(10,Math.floor(Math.log10(base_num))-1);
      return Math.floor(base_num/mag10) * mag10 * Math.pow(2,mag*10);
    }

    function onMultiChartDataUpdate(stats) {
      if (!stats) {
        return;
      }

      if (stats.status == 404) {
        $scope.options = {
          chart: {
            notFound: true,
            height: getChartSize($scope.config.size),
            margin : {top: 0, right: 0, bottom: 0, left: 0},
            type: 'multiChart',
            noData: "Stats are not found or not ready yet"
          }
        };
        $scope.chartData = [];
        return;
      }

      var chartData = [];

      if ($scope.config.specificStat) {
        var descPath = Object.keys($scope.config.stats)[0];
        var desc = mnStatisticsNewService.readByPath(descPath);
        if (!desc) {
          return;
        }
        var statName = Object.keys(stats.stats)[0];
        var nodes;
        if ($scope.node == "all") {
          nodes = Object.keys(stats.stats[statName] || {});
          if (!nodes.length) {
            nodes = mnPoolDefault.export.nodes.map(n => n.hostname);
          }
        } else {
          nodes = [$scope.node];
        }

        nodes.forEach((nodeName, i) => {
          var previousData = $scope.chartData && $scope.chartData[i];
          chartData.push(
            mnStatisticsNewService.buildChartConfig(stats, statName, nodeName,
                                                    nodeName, desc.unit, units[desc.unit],
                                                    previousData, poller.isThisInitCall(),
                                                    start, step))
        });
      } else {
        Object.keys($scope.config.stats).forEach(function (descPath, i) {
          var desc = mnStatisticsNewService.readByPath(descPath);
          if (!desc) {
            return;
          }

          var statName =
              mnStatisticsNewService.descriptionPathToStatName(descPath, $scope.items);
          var previousData = $scope.chartData && $scope.chartData[i];
          chartData.push(
            mnStatisticsNewService.buildChartConfig(stats, statName, $scope.node,
                                                    desc.title, desc.unit, units[desc.unit],
                                                    previousData, poller.isThisInitCall(),
                                                    start, step));

        });
      }

      if ($scope.chartData) {
        $scope.chartData.forEach(function (v, i) {
          if (!chartData[i]) {
            return;
          }
          chartData[i].disabled = v.disabled;
        });
      }

      $scope.chartData = chartData;
    }
  }
}
