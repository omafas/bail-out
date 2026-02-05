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
import template from "./mn_settings_cluster_confirmation_dialog.html";

export default 'mnSettingsQuery';

angular.module('mnSettingsQuery', [
  mnHelper,
  mnSpinner,
  mnMainSpinner,
  mnField,
  mnPromiseHelper,
  mnMemoryQuota,
  mnStorageMode,
  mnPoolDefault,
  mnMemoryQuotaService,
  mnSettingsClusterService,
  mnClusterConfigurationService,
]).controller('mnSettingsQueryController', ["$scope", "$q", "$uibModal", "mnPoolDefault","mnSettingsClusterService", "mnHelper", "mnPromiseHelper", "mnClusterConfigurationService", mnSettingsQueryController]);

function mnSettingsQueryController($scope, $q, $uibModal, mnPoolDefault, mnSettingsClusterService, mnHelper, mnPromiseHelper, mnClusterConfigurationService) {
  var vm = this;
  vm.saveVisualInternalSettings = saveVisualInternalSettings;
  vm.reloadState = mnHelper.reloadState;
  vm.itemsSelect = [...Array(65).keys()].slice(1);

  activate();

  let submitted;

  function saveSettings() {
    if (!isFormInitialized() || submitted) {
      return;
    }
    submitted = true;
    var queries = [];
    var promise3;
    var promise5;

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
      if (mnPoolDefault.export.compat.atLeast75) {
        settings.push("queryUseReplica");
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
          .onSuccess(prepareQuerySettings)
          .getPromise();

      queries.push(promise3, promise5);
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

  function saveVisualInternalSettings() {
    if (vm.clusterSettingsLoading) {
      return;
    }
    saveSettings();
  }

  function maybeSetInititalValue(array, value) {
    if (array.length === 0) {
      array.push(value);
    }
  }

  function prepareQuerySettings(querySettings) {
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
    return ((compat.atLeast55 && cluster.settings.read) ? (vm.querySettings != void 0) : true) &&
        mnSettingsClusterService.getInitChecker().every(v => v());
  }

  function activate() {
    mnSettingsClusterService.clearSubmitCallbacks();
    mnSettingsClusterService.clearInitChecker();

    if (mnPoolDefault.export.compat.atLeast55 && $scope.rbac.cluster.settings.read) {
      mnPromiseHelper(vm, mnClusterConfigurationService.getQuerySettings())
          .onSuccess(prepareQuerySettings);
    }
  }
}
