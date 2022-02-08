/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import {fromEvent, Subject, timer} from 'rxjs';
import {tap, switchMap, takeUntil} from 'rxjs/operators';
import _ from 'lodash';
import saveAs from 'file-saver';
import mnDeveloperSettingsTemplate from "./mn_developer_settings.html";
import mnInternalSettingsTemplate from "./mn_internal_settings.html";

export default mnAdminController;

mnAdminController.$inject = ["$scope", "$rootScope", "$state", "$window", "$uibModal", "mnAlertsService", "poolDefault", "mnPromiseHelper", "pools", "mnPoller", "mnEtagPoller", "mnAuthService", "mnTasksDetails", "mnPoolDefault", "mnSettingsAutoFailoverService", "formatProgressMessageFilter", "mnPrettyVersionFilter", "mnLostConnectionService", "mnPermissions", "mnPools", "whoami", "mnBucketsService", "$q", "mnSettingsClusterService", "$ocLazyLoad", "$injector", "mnAdminService", "mnHelper", "mnSessionService"];
function mnAdminController($scope, $rootScope, $state, $window, $uibModal, mnAlertsService, poolDefault, mnPromiseHelper, pools, mnPoller, mnEtagPoller, mnAuthService, mnTasksDetails, mnPoolDefault, mnSettingsAutoFailoverService, formatProgressMessageFilter, mnPrettyVersionFilter, mnLostConnectionService, mnPermissions, mnPools, whoami, mnBucketsService, $q, mnSettingsClusterService, $ocLazyLoad, $injector, mnAdminService, mnHelper, mnSessionService) {
  var vm = this;

  vm.poolDefault = poolDefault;
  vm.launchpadId = pools.launchID;
  vm.implementationVersion = pools.implementationVersion;
  vm.logout = mnAuthService.logout;
  vm.resetAutoFailOverCount = resetAutoFailOverCount;
  vm.isProgressBarClosed = true;
  vm.toggleProgressBar = toggleProgressBar;
  vm.filterTasks = filterTasks;
  vm.showResetPasswordDialog = showResetPasswordDialog;
  vm.postCancelRebalanceRetry = postCancelRebalanceRetry;
  vm.showClusterInfoDialog = showClusterInfoDialog;
  vm.isDeveloperPreview = pools.isDeveloperPreview;
  vm.mainSpinnerCounter = mnHelper.mainSpinnerCounter;

  $rootScope.mnGlobalSpinnerFlag = false;

  vm.user = whoami;

  vm.$state = $state;

  vm.enableInternalSettings = $state.params.enableInternalSettings;
  vm.enableDeveloperSettings = $state.params.enableDeveloperSettings;
  vm.runInternalSettingsDialog = runInternalSettingsDialog;
  vm.runDeveloperSettingsDialog = runDeveloperSettingsDialog;
  vm.lostConnState = mnLostConnectionService.getState();

  vm.clientAlerts = mnAlertsService.clientAlerts;
  vm.alerts = mnAlertsService.alerts;
  vm.closeAlert = mnAlertsService.removeItem;
  vm.setHideNavSidebar = mnPoolDefault.setHideNavSidebar;
  vm.postStopRebalance = postStopRebalance;
  vm.closeCustomAlert = closeCustomAlert;
  vm.enableCustomAlert = enableCustomAlert;

  vm.getRebalanceReport = getRebalanceReport;

  $rootScope.implementationVersion = pools.implementationVersion;
  $rootScope.rbac = mnPermissions.export;
  $rootScope.poolDefault = mnPoolDefault.export;
  $rootScope.pools = mnPools.export;
  $rootScope.buckets = mnBucketsService.export;

  let mnOnDestroy = new Subject();
  $scope.$on("$destroy", function () {
    mnOnDestroy.next();
    mnOnDestroy.complete();
  });

  function disableHoverEventDuringScroll() {
    let bodyElement = angular.element(document.querySelector("body"));

    fromEvent(bodyElement, "scroll")
      .pipe(tap(() => bodyElement.addClass("mn-scroll-active")),
            switchMap(() => timer(200)),
            takeUntil(mnOnDestroy))
      .subscribe(() => bodyElement.removeClass("mn-scroll-active"));
  }

  disableHoverEventDuringScroll();

  activate();

  function closeCustomAlert(alertName) {
    vm.clientAlerts[alertName] = true;
  }

  function enableCustomAlert(alertName) {
    vm.clientAlerts[alertName] = false;
  }

  function postCancelRebalanceRetry(id) {
    mnSettingsClusterService.postCancelRebalanceRetry(id);
  }

  async function showClusterInfoDialog() {
    await import('./mn_logs_service.js');
    await $ocLazyLoad.load({name: 'mnLogsService'});
    var mnLogsService = $injector.get('mnLogsService');
    mnLogsService.showClusterInfoDialog();
  }

  async function showResetPasswordDialog() {
    vm.showUserDropdownMenu = false;
    await import('./mn_reset_password_dialog_controller.js');
    await $ocLazyLoad.load({name: 'mnResetPasswordDialog'});
    var mnResetPasswordDialogService = $injector.get('mnResetPasswordDialogService');
    mnResetPasswordDialogService.showDialog(whoami);
  }

  async function postStopRebalance() {
    await import('./mn_servers_service.js');
    await $ocLazyLoad.load({name: 'mnServersService'});
    var mnServersService = $injector.get('mnServersService');
    return mnPromiseHelper(vm, mnServersService.stopRebalanceWithConfirm())
      .broadcast("reloadServersPoller");
  }

  function runDeveloperSettingsDialog() {
    import('./mn_developer_settings_controller.js')
      .then(function () {
        $ocLazyLoad.load({name: 'mnDeveloperSettings'});
        $uibModal.open({
          template: mnDeveloperSettingsTemplate,
          controller: "mnDeveloperSettingsController as devSettingsCtl"
        });
      });
  }

  function runInternalSettingsDialog() {
    import('./mn_internal_settings_controller.js')
      .then(function () {
        $ocLazyLoad.load({name: 'mnInternalSettings'});
        $uibModal.open({
          template: mnInternalSettingsTemplate,
          controller: "mnInternalSettingsController as internalSettingsCtl"
        });
      });
  }

  function toggleProgressBar() {
    vm.isProgressBarClosed = !vm.isProgressBarClosed;
  }

  function filterTasks(runningTasks, includeRebalance) {
    return (runningTasks || []).filter(function (task) {
      return formatProgressMessageFilter(task, includeRebalance);
    });
  }

  function resetAutoFailOverCount() {
    var queries = [
      mnSettingsAutoFailoverService.resetAutoFailOverCount({group: "global"}),
      mnSettingsAutoFailoverService.resetAutoReprovisionCount({group: "global"})
    ];

    mnPromiseHelper(vm, $q.all(queries))
      .reloadState()
      .showSpinner('resetQuotaLoading')
      .catchGlobalErrors('Unable to reset Auto-failover quota!')
      .showGlobalSuccess("Auto-failover quota reset successfully!");
  }

  function getRebalanceReport() {
    mnTasksDetails.getRebalanceReport().then(function(report) {
      var file = new Blob([JSON.stringify(report,null,2)],{type: "application/json", name: "rebalanceReport.json"});
      saveAs(file,"rebalanceReport.json");
    });
  }

  function activate() {
    mnSessionService.activate(mnOnDestroy);

    new mnPoller($scope, function () {
      return mnBucketsService.findMoxiBucket();
    })
      .subscribe("moxiBucket", vm)
      .reloadOnScopeEvent(["reloadBucketStats"])
      .cycle();

    if (mnPermissions.export.cluster.settings.read) {
      new mnPoller($scope, function () {
        return mnSettingsAutoFailoverService.getAutoFailoverSettings();
      })
        .setInterval(10000)
        .subscribe("autoFailoverSettings", vm)
        .reloadOnScopeEvent(["reloadServersPoller", "rebalanceFinished"])
        .cycle();
    }

    if (mnPermissions.export.cluster.settings.read) {
      loadAndRunLauchpad($ocLazyLoad, $injector, vm);
    }

    new mnEtagPoller($scope, function (previous) {
      return mnPoolDefault.get({
        etag: previous ? previous.etag : "",
        waitChange: 10000
      }, {group: "global"});
    }, true).subscribe(function (resp, previous) {

      if (previous && (resp.thisNode.clusterCompatibility !=
                       previous.thisNode.clusterCompatibility)) {
        $window.location.reload();
      }

      mnAdminService.stream.getPoolsDefault.next(resp);

      if (!_.isEqual(resp, previous)) {
        $rootScope.$broadcast("mnPoolDefaultChanged");
      }

      if (Number(localStorage.getItem("uiSessionTimeout")) !== (resp.uiSessionTimeout * 1000)) {
        $rootScope.$broadcast("newSessionTimeout", resp.uiSessionTimeout);
      }

      vm.tabName = resp.clusterName;

      if (previous && !_.isEqual(resp.nodes, previous.nodes)) {
        $rootScope.$broadcast("nodesChanged", [resp.nodes, previous.nodes]);
      }

      if (previous && previous.buckets.uri !== resp.buckets.uri) {
        $rootScope.$broadcast("reloadBucketStats");
      }

      if (previous && previous.trustedCAsURI !== resp.trustedCAsURI) {
        $rootScope.$broadcast("reloadGetPoolsDefaultTrustedCAs");
      }

      if (previous && previous.serverGroupsUri !== resp.serverGroupsUri) {
        $rootScope.$broadcast("serverGroupsUriChanged");
      }

      if (previous && previous.indexStatusURI !== resp.indexStatusURI) {
        $rootScope.$broadcast("indexStatusURIChanged");
      }

      if (!_.isEqual(resp.alerts, (previous || {}).alerts || [])) {
        loadAndRunPoorMansAlertsDialog($ocLazyLoad, $injector, resp);
      }

      var version = mnPrettyVersionFilter(pools.implementationVersion);
      $rootScope.mnTitle = vm.tabName + (version ? (' - ' + version) : '');

      if (previous && previous.tasks.uri != resp.tasks.uri) {
        $rootScope.$broadcast("reloadTasksPoller");
      }

      if (previous && previous.checkPermissionsURI != resp.checkPermissionsURI) {
        $rootScope.$broadcast("reloadPermissions");
      }
    })
        .cycle();

    if (mnPermissions.export.cluster.tasks.read) {
      if (pools.isEnterprise && poolDefault.compat.atLeast65) {
        new mnPoller($scope, function () {
          return mnSettingsClusterService.getPendingRetryRebalance({group: "global"});
        })
            .setInterval(function (resp) {
              return resp.data.retry_after_secs ? 1000 : 3000;
            })
            .subscribe(function (resp) {
              vm.retryRebalance = resp.data;
            }).cycle();
      }

      var tasksPoller = new mnPoller($scope, function (prevTask) {
        return mnTasksDetails.getFresh({group: "global"})
          .then(function (tasks) {
            if (poolDefault.compat.atLeast65) {
              if (tasks.tasksRebalance.status == "notRunning") {
                if (!tasks.tasksRebalance.masterRequestTimedOut &&
                    prevTask && (tasks.tasksRebalance.lastReportURI !=
                                 prevTask.tasksRebalance.lastReportURI)) {
                  mnTasksDetails.clearRebalanceReportCache(prevTask.tasksRebalance.lastReportURI);
                }
                if (mnPermissions.export.cluster.admin.logs.read) {
                  return mnTasksDetails.getRebalanceReport(tasks.tasksRebalance.lastReportURI)
                    .then(function (rv) {
                      if (rv.data.stageInfo) {
                        tasks.tasksRebalance.stageInfo = rv.data.stageInfo;
                        tasks.tasksRebalance.completionMessage = rv.data.completionMessage;
                      }
                      return tasks;
                    });
                }
              }
              return tasks;
            }
            return tasks;
          });
      })
          .setInterval(function (result) {
            return (_.chain(result.tasks).pluck('recommendedRefreshPeriod').compact().min().value() * 1000) >> 0 || 10000;
          })
          .subscribe(function (tasks, prevTask) {
            vm.showTasksSpinner = false;
            if (!_.isEqual(tasks, prevTask)) {
              $rootScope.$broadcast("mnTasksDetailsChanged");
            }

            var isRebalanceFinished =
                tasks.tasksRebalance && tasks.tasksRebalance.status !== 'running' &&
                prevTask && prevTask.tasksRebalance && prevTask.tasksRebalance.status === "running";
            if (isRebalanceFinished) {
              $rootScope.$broadcast("rebalanceFinished");
            }

            if (!vm.isProgressBarClosed &&
                !filterTasks(tasks.running).length &&
                !tasks.tasksRebalance.stageInfo &&
                prevTask && filterTasks(prevTask.running).length) {
              vm.isProgressBarClosed = true;
            }

            var stageInfo = {
              services: {},
              startTime: null,
              completedTime: {
                status: true
              }
            };
            var serverStageInfo = tasks.tasksRebalance.stageInfo ||
                (tasks.tasksRebalance.previousRebalance &&
                 tasks.tasksRebalance.previousRebalance.stageInfo);

            if (serverStageInfo) {
              var services = Object
                  .keys(serverStageInfo)
                  .sort(function (a, b) {
                    if (!serverStageInfo[a].timeTaken) {
                      return 1;
                    }
                    if (!serverStageInfo[b].startTime) {
                      return -1;
                    }
                    if (new Date(serverStageInfo[a].startTime) >
                        new Date(serverStageInfo[b].startTime)) {
                      return 1;
                    } else {
                      return -1;
                    }
                  });

              stageInfo.services = services
                .map(function(key) {
                  var value = serverStageInfo[key];
                  value.name = key;
                  var details = Object
                      .keys(value.details || {})
                  // .sort(function (a, b) {
                  //   return new Date(value.details[a].startTime) -
                  //     new Date(value.details[b].startTime);
                  // });

                  value.details = details.map(function (bucketName) {
                    value.details[bucketName].name = bucketName;
                    return value.details[bucketName];
                  });

                  if (value.startTime) {
                    if (!stageInfo.startTime ||
                        stageInfo.startTime > new Date(value.startTime)) {
                      stageInfo.startTime = new Date(value.startTime);
                    }
                  }
                  if (value.completedTime) {
                    value.completedTime = new Date(value.completedTime);
                    if (!stageInfo.completedTime.time ||
                        (stageInfo.completedTime.time < value.completedTime)) {
                      stageInfo.completedTime.time = new Date(value.completedTime);
                    }
                  } else {
                    stageInfo.completedTime.status = false;
                  }
                  return value;
                });

              tasks.tasksRebalance.stageInfo = stageInfo;
            }

            if (tasks.inRebalance) {
              if (!prevTask) {
                vm.isProgressBarClosed = false;
              } else {
                if (!prevTask.tasksRebalance ||
                    prevTask.tasksRebalance.status !== "running") {
                  vm.isProgressBarClosed = false;
                }
              }
            }

            if (tasks.tasksRebalance.errorMessage && mnAlertsService.isNewAlert({id: tasks.tasksRebalance.statusId})) {
              mnAlertsService.setAlert("error", tasks.tasksRebalance.errorMessage, null, tasks.tasksRebalance.statusId);
            }
            vm.tasks = tasks;
          }, vm)
          .cycle();
    }

    $scope.$on("reloadPermissions", function () {
      mnPermissions.throttledCheck();
    });

    $scope.$on("reloadTasksPoller", function (event, params) {
      if (!params || !params.doNotShowSpinner) {
        vm.showTasksSpinner = true;
      }
      if (tasksPoller) {
        tasksPoller.reload(true);
      }
    });

    $scope.$on("reloadBucketStats", function () {
      mnBucketsService.clearCache();
      mnBucketsService.getBucketsByType();
    });
    $rootScope.$broadcast("reloadBucketStats");
  }
}


async function loadAndRunPoorMansAlertsDialog($ocLazyLoad, $injector, resp) {
  await import("./mn_poor_mans_alerts_controller.js");
  await $ocLazyLoad.load({name: 'mnPoorMansAlerts'});
  var mnPoorMansAlertsService = $injector.get('mnPoorMansAlertsService');
  mnPoorMansAlertsService.maybeShowAlerts(resp);
}

async function loadAndRunLauchpad($ocLazyLoad, $injector, vm) {
  await import("./mn_settings_notifications_service.js");
  await $ocLazyLoad.load({name: 'mnSettingsNotificationsService'});
  var mnSettingsNotificationsService = $injector.get('mnSettingsNotificationsService');

  vm.updates = await mnSettingsNotificationsService.maybeCheckUpdates({group: "global"});
  if (vm.updates.sendStats) {
    vm.launchpadSource = await mnSettingsNotificationsService
      .buildPhoneHomeThingy({group: "global"})
  }
}
