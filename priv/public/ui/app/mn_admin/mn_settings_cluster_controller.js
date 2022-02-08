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

import mnHelper from "../components/mn_helper.js";
import mnSpinner from "../components/directives/mn_spinner.js";
import mnMainSpinner from "../components/directives/mn_main_spinner.js";
import mnField from "../components/directives/mn_field_directive.js";
import mnPromiseHelper from "../components/mn_promise_helper.js";
import mnMemoryQuota from "../components/directives/mn_memory_quota/mn_memory_quota.js";
import mnStorageMode from "../components/directives/mn_storage_mode/mn_storage_mode.js";

import mnPoolDefault from "../components/mn_pool_default.js";

import mnSettingsClusterService from "./mn_settings_cluster_service.js";
import mnXDCRService from "./mn_xdcr_service.js";
import mnMemoryQuotaService from "../components/directives/mn_memory_quota/mn_memory_quota_service.js";
import mnClusterConfigurationService from "../mn_wizard/mn_cluster_configuration/mn_cluster_configuration_service.js";

export default 'mnSettingsCluster';

angular.module('mnSettingsCluster', [
  mnHelper,
  mnSpinner,
  mnMainSpinner,
  mnField,
  mnPromiseHelper,
  mnMemoryQuota,
  mnStorageMode,
  mnPoolDefault,
  mnMemoryQuotaService,
  mnXDCRService,
  mnSettingsClusterService,
  mnClusterConfigurationService,
]).controller('mnSettingsClusterController', ["$scope", "$q", "$uibModal", "mnPoolDefault", "mnMemoryQuotaService", "mnSettingsClusterService", "mnHelper", "mnPromiseHelper", "mnClusterConfigurationService", "mnXDCRService", mnSettingsClusterController]);

function mnSettingsClusterController($scope, $q, $uibModal, mnPoolDefault, mnMemoryQuotaService, mnSettingsClusterService, mnHelper, mnPromiseHelper, mnClusterConfigurationService, mnXDCRService) {
  var vm = this;
  vm.saveVisualInternalSettings = saveVisualInternalSettings;
  vm.reloadState = mnHelper.reloadState;
  vm.itemsSelect = [...Array(65).keys()].slice(1);

  activate();

  $scope.$watch('settingsClusterCtl.memoryQuotaConfig', _.debounce(function (memoryQuotaConfig) {
    if (!memoryQuotaConfig || !$scope.rbac.cluster.pools.write) {
      return;
    }
    var promise = mnSettingsClusterService.postPoolsDefault(vm.memoryQuotaConfig, true);
    mnPromiseHelper(vm, promise)
      .catchErrors("memoryQuotaErrors");
  }, 500), true);

  $scope.$watch('settingsClusterCtl.indexSettings', _.debounce(function (indexSettings, prevIndexSettings) {
    if (!indexSettings || !$scope.rbac.cluster.settings.indexes.write || !(prevIndexSettings && !_.isEqual(indexSettings, prevIndexSettings))) {
      return;
    }
    var promise = mnSettingsClusterService.postIndexSettings(vm.indexSettings, true);
    mnPromiseHelper(vm, promise)
      .catchErrors("indexSettingsErrors");
  }, 500), true);

  let submitted;

  function saveSettings() {
    if (!isFormInitialized() || submitted) {
      return;
    }
    submitted = true;
    var queries = [];
    var promise1 = mnPromiseHelper(vm, mnSettingsClusterService.postPoolsDefault(vm.memoryQuotaConfig, false, vm.clusterName))
        .catchErrors("memoryQuotaErrors")
        .onSuccess(function () {
          vm.initialMemoryQuota = vm.memoryQuotaConfig.indexMemoryQuota;
        })
        .getPromise();
    var promise2;
    var promise3;
    var promise5;
    var promise6;
    var promise7;
    var promise8;
    var promise9;
    var promise10;

    queries.push(promise1);

    promise6 = mnPromiseHelper(vm,
                               mnXDCRService.postSettingsReplications(vm.replicationSettings))
      .catchErrors("replicationSettingsErrors")
      .getPromise();

    queries.push(promise6);

    if (!_.isEqual(vm.indexSettings, vm.initialIndexSettings) && $scope.rbac.cluster.settings.indexes.write) {
      promise2 = mnPromiseHelper(vm, mnSettingsClusterService.postIndexSettings(vm.indexSettings))
        .catchErrors("indexSettingsErrors")
        .applyToScope("initialIndexSettings")
        .getPromise();

      queries.push(promise2);
    }

    if (mnPoolDefault.export.compat.atLeast55 && $scope.rbac.cluster.settings.write) {
      let settings = [
        "queryTmpSpaceDir", "queryTmpSpaceSize", "queryPipelineBatch", "queryPipelineCap",
        "queryScanCap", "queryTimeout", "queryPreparedLimit", "queryCompletedLimit",
        "queryCompletedThreshold", "queryLogLevel", "queryMaxParallelism",
        "queryN1QLFeatCtrl"
      ];
      if (mnPoolDefault.export.compat.atLeast70) {
        settings = settings.concat(["queryTxTimeout", "queryMemoryQuota"]);
        if (mnPoolDefault.export.isEnterprise) {
          settings.push("queryUseCBO");
        }
      }
      promise3 = mnPromiseHelper(
        vm,
        mnClusterConfigurationService.postQuerySettings(
          settings.reduce(function (acc, key) {
            acc[key] = vm.querySettings[key];
            return acc;
          }, {})))
        .catchErrors("querySettingsErrors")
        .getPromise();

      promise5 = mnPromiseHelper(vm, mnClusterConfigurationService.postCurlWhitelist(
        vm.querySettings.queryCurlWhitelist,
        vm.initialCurlWhitelist
      ))
        .catchErrors("curlWhitelistErrors")
        .onSuccess(prepareQueryCurl)
        .getPromise();

      queries.push(promise3, promise5);
    }

    if (mnPoolDefault.export.isEnterprise &&
        mnPoolDefault.export.compat.atLeast65 &&
        $scope.rbac.cluster.settings.write) {
      promise7 = mnPromiseHelper(vm, mnSettingsClusterService
                                 .postSettingsRetryRebalance(vm.retryRebalanceCfg))
        .catchErrors("retryRebalanceErrors")
        .getPromise();
      queries.push(promise7);
    }

    if (mnPoolDefault.export.compat.atLeast66 &&
        $scope.rbac.cluster.settings.write) {
      promise9 = mnPromiseHelper(vm, mnSettingsClusterService
                                 .postSettingsRebalance(vm.settingsRebalance))
        .catchErrors("settingsRebalanceErrors")
        .getPromise();
      queries.push(promise9);
    }

    if (mnPoolDefault.export.compat.atLeast71 &&
        $scope.rbac.cluster.settings.write) {
      promise10 = mnPromiseHelper(vm, mnSettingsClusterService
                                 .postSettingsAnalytics(vm.settingsAnalytics))
        .catchErrors("settingsAnalyticsErrors")
        .getPromise();
      queries.push(promise10);
    }

    if ($scope.rbac.cluster.admin.memcached.write) {
      promise8 = mnPromiseHelper(vm, mnSettingsClusterService.postMemcachedSettings({
        num_reader_threads: packThreadValue('reader'),
        num_writer_threads: packThreadValue('writer'),
        num_storage_threads: packThreadValue('storage')
      }))
        .catchErrors("dataServiceSettingsErrors")
        .getPromise();
      queries.push(promise8);
    }

    queries = queries.concat(mnSettingsClusterService.getSubmitCallbacks().map(function (cb) {
      return cb();
    }));

    var promiseAll = $q.all(queries)
        .finally(() => (submitted = false));
    mnPromiseHelper(vm, promiseAll)
      .showGlobalSpinner()
      .reloadState()
      .showGlobalSuccess("Settings saved successfully!");
  }
  function packThreadValue(type) {
    switch (vm[type + 'Threads']) {
    case "fixed": return vm[type + 'ThreadsFixed'];
    default: return vm[type + 'Threads'];
    }
  }
  function unpackThreadValue(value) {
    switch (typeof value) {
    case "string": return value;
    case "number": return "fixed";
    default: return "default";
    }
  }
  function unpackThreadsCount(value) {
    switch (typeof value) {
    case "number": return value;
    default: return 4;
    }
  }
  function saveVisualInternalSettings() {
    if (vm.clusterSettingsLoading) {
      return;
    }
    if ((!vm.indexSettings || vm.indexSettings.storageMode === "forestdb") && vm.initialMemoryQuota != vm.memoryQuotaConfig.indexMemoryQuota) {
      $uibModal.open({
        templateUrl: 'app/mn_admin/mn_settings_cluster_confirmation_dialog.html'
      }).result.then(saveSettings);
    } else {
      saveSettings();
    }
  }
  function maybeSetInititalValue(array, value) {
    if (array.length === 0) {
      array.push(value);
    }
  }
  function prepareQueryCurl(querySettings) {
    var queryCurl = querySettings.queryCurlWhitelist;
    queryCurl.allowed_urls = queryCurl.allowed_urls || [];
    queryCurl.disallowed_urls = queryCurl.disallowed_urls || [];
    maybeSetInititalValue(queryCurl.allowed_urls, "");
    maybeSetInititalValue(queryCurl.disallowed_urls, "");
    vm.initialCurlWhitelist = _.cloneDeep(queryCurl);
    vm.querySettings = querySettings;
  }
  function isFormInitialized() {
    let compat = mnPoolDefault.export.compat;
    let cluster = $scope.rbac.cluster;
    return (vm.clusterName != void 0) && (vm.initialMemoryQuota != void 0) &&
      ((compat.atLeast55 && cluster.settings.read) ? (vm.querySettings != void 0) : true) &&
      (cluster.xdcr.settings.read ? (vm.replicationSettings != void 0) : true) &&
      (cluster.admin.memcached.read ? (vm.readerThreads != void 0) : true) &&
      ((compat.atLeast66 && cluster.settings.read) ? (vm.settingsRebalance != void 0) : true) &&
      ((compat.atLeast65 && mnPoolDefault.export.isEnterprise && cluster.settings.read) ?
       (vm.retryRebalanceCfg != void 0) : true) &&
      (cluster.settings.indexes.read ? (vm.indexSettings != void 0) : true) &&
      mnSettingsClusterService.getInitChecker().every(v => v());

  }
  function activate() {
    mnSettingsClusterService.clearSubmitCallbacks();
    mnSettingsClusterService.clearInitChecker();

    mnPromiseHelper(vm, mnPoolDefault.get())
      .applyToScope(function (resp) {
        vm.clusterName = resp.clusterName;
      });

    if (mnPoolDefault.export.compat.atLeast55 && $scope.rbac.cluster.settings.read) {
      mnPromiseHelper(vm, mnClusterConfigurationService.getQuerySettings())
        .onSuccess(prepareQueryCurl);
    }

    var services = {
      kv: true,
      index: true,
      fts: true,
      n1ql: true
    };

    if (mnPoolDefault.export.isEnterprise) {
      services.cbas = mnPoolDefault.export.compat.atLeast55;
      services.eventing = mnPoolDefault.export.compat.atLeast55;
      services.backup = mnPoolDefault.export.compat.atLeast70;
    }

    if ($scope.rbac.cluster.xdcr.settings.read) {
      mnXDCRService.getSettingsReplications().then(function (rv) {
        vm.replicationSettings = rv.data;
      });
    }

    if (mnPoolDefault.export.compat.atLeast71 && $scope.rbac.cluster.settings.read) {
      mnSettingsClusterService.getSettingsAnalytics()
        .then(settings => vm.settingsAnalytics = settings);
    }

    if ($scope.rbac.cluster.admin.memcached.read) {
      mnSettingsClusterService.getMemcachedSettings().then(function (rv) {
        vm.readerThreads = unpackThreadValue(rv.data.num_reader_threads);
        vm.writerThreads = unpackThreadValue(rv.data.num_writer_threads);
        vm.storageThreads = unpackThreadValue(rv.data.num_storage_threads);
        vm.readerThreadsFixed = unpackThreadsCount(rv.data.num_reader_threads);
        vm.writerThreadsFixed = unpackThreadsCount(rv.data.num_writer_threads);
        vm.storageThreadsFixed = unpackThreadsCount(rv.data.num_storage_threads);
      });
    }

    if (mnPoolDefault.export.compat.atLeast66 &&
        $scope.rbac.cluster.settings.read) {
      mnSettingsClusterService.getSettingsRebalance().then(settings => {
        vm.settingsRebalance = settings.data;
      });
    }

    if (mnPoolDefault.export.compat.atLeast65 &&
        mnPoolDefault.export.isEnterprise &&
        $scope.rbac.cluster.settings.read) {
      mnSettingsClusterService.getSettingsRetryRebalance().then(function (data) {
        vm.retryRebalanceCfg = data;

        if (!$scope.rbac.cluster.settings.write) {
          return;
        }

        $scope.$watch('settingsClusterCtl.retryRebalanceCfg', _.debounce(function (values) {
          mnPromiseHelper(vm, mnSettingsClusterService
                          .postSettingsRetryRebalance(values, {just_validate: 1}))
            .catchErrors("retryRebalanceErrors");
        }, 500, {leading: true}), true);
      });
    }

    mnPromiseHelper(vm, mnMemoryQuotaService.memoryQuotaConfig(services, false, false))
      .applyToScope(function (resp) {
        vm.initialMemoryQuota = resp.indexMemoryQuota;
        vm.memoryQuotaConfig = resp;
      });

    if ($scope.rbac.cluster.settings.indexes.read) {
      mnPromiseHelper(vm, mnSettingsClusterService.getIndexSettings())
        .applyToScope(function (indexSettings) {
          vm.indexSettings = indexSettings;
          vm.initialIndexSettings = _.clone(indexSettings);
        });
    }
  }
}
