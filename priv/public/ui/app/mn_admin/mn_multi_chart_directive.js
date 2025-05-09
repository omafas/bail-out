/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/
import angular from "angular";
import _ from "lodash"

export default mnMultiChartDirective;

mnMultiChartDirective.$inject = ["$window", "mnD3Service"];
function mnMultiChartDirective($window, mnD3Service) {
  return {
    restrict: 'AE',
    scope: {
      data: "=?",
      options: "=?",
      api: "=?",
      syncScope: "=?",
      statsPoller: "=?"
    },
    controller: ["$element", "$scope", "$timeout", controller]
  };

  function controller($element, $scope, $timeout) {
    var chart = new mnD3Service.mnD3Tooltip($scope.options, $element, function () {
      angular.element($window).on('resize', chart.throttledResize);
      chart.resize();
      if ($scope.syncScope) {
        syncTooltipsAndPauseCharts();
      }
      if ($scope.options.chart.showFocus) {
        addFocusChart();
      }
      if (this.cht.showLegends) {
        this.drawLegends();
      }
    });

    $scope.$watch("data", chart.updateData.bind(chart));

    $scope.$on("$destroy", function () {
      angular.element($window).off('resize', chart.throttledResize);
      chart.destroy();
    });

    if ($scope.api) {
      $scope.api.chart = chart;
    }

    function addFocusChart() {
      var focusChartOpt = _.cloneDeep($scope.options);
      focusChartOpt.chart.height = 80;
      focusChartOpt.chart.hideTicks = true;
      focusChartOpt.chart.showFocus = false;
      var chartF = new mnD3Service.mnD3Focus(focusChartOpt, $element, chart);
      angular.element($window).on('resize', chartF.throttledResize);

      $scope.$watch("data", chartF.updateData.bind(chartF));

      chart.rootEl.on("toggleLegend", function () {
        chartF.updateData(chartF.data);
      });

      if ($scope.api) {
        $scope.api.chartF = chartF;
      }

      $scope.$on("$destroy", function () {
        angular.element($window).off('resize', chartF.throttledResize);
        chartF.destroy();
      });
    }

    function syncTooltipsAndPauseCharts() {
      let resumeStatsTimer;
      let chartNode = chart.tipBox.node();
      let cancelStatsTimer = () => $scope.syncScope.$broadcast("mnStatsCancelTimer");

      let throttledSync = _.throttle(function (e) {
        if (e.bubbles) {
          $scope.syncScope.$broadcast("syncTooltips", {element: $element,
                                                       event: e,
                                                       chart: chart});
        }
      }, 60, {leading: true});

      let throttledPause = _.throttle(function (e) {
        if (e.bubbles) {
          cancelStatsTimer();
          if (!$scope.statsPoller.heartbeat.isPaused) {
            $scope.statsPoller.heartbeat.pause();
          }
        }
      }, 10, {leading: true});

      let throttledResume = _.throttle(function (e) {
        if ($scope.statsPoller.heartbeat.isPaused && e.bubbles) {
          resumeStatsTimer && $timeout.cancel(resumeStatsTimer);
          resumeStatsTimer = $timeout(function () {
            $scope.statsPoller.heartbeat.resume();
          }, 1000);
        }
      }, 10, {leading: true});

      angular.element(chartNode).on("mousemove mouseup mousedown mouseout", throttledSync);
      angular.element(chartNode).on("mouseenter", throttledPause);
      angular.element(chartNode).on("mouseout", throttledResume);

      document.addEventListener('visibilitychange', cancelStatsTimer);

      $scope.$on("mnStatsCancelTimer", function () {
        resumeStatsTimer && $timeout.cancel(resumeStatsTimer);
      });

      $scope.$on("$destroy", function () {
        angular.element(chartNode).off("mousemove mouseup mousedown mouseout", throttledSync);
        angular.element(chartNode).off("mouseenter", throttledPause);
        angular.element(chartNode).off("mouseout", throttledResume);
        document.removeEventListener('visibilitychange', cancelStatsTimer);
      });

      $scope.$on("syncTooltips", function (e, source) {
        if (source.element[0] !== $element[0]) {
          var sourcePos = source.chart.tipBoxRect;
          var elementPos = chart.tipBoxRect;
          var sourceGraphRelativeX = source.event.clientX - sourcePos.x;
          var sourceGraphRelativeY = source.event.clientY - sourcePos.y;

          var interpolateX = sourcePos.width / sourceGraphRelativeX;
          var clientX = elementPos.x + (elementPos.width / interpolateX);

          var interpolateY = sourcePos.height / sourceGraphRelativeY;
          var clientY = elementPos.y + (elementPos.height / interpolateY);

          source.chart.disableTooltip(false);
          chart.disableTooltip(true);

          chart.tipBox.node().dispatchEvent(createEvent(
            source.event.type,
            clientX,
            clientY
          ));
        }
      });
    }

    function createEvent(type, clientX, clientY){
      var event = new MouseEvent(type, {
        view: $window,
        bubbles: false,
        cancelable: true,
        clientX: clientX,
        clientY: clientY
      });
      return event;
    }
  }
}
