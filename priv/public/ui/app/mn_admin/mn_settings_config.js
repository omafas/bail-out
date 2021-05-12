/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";

import mnPluggableUiRegistry from "/ui/app/components/mn_pluggable_ui_registry.js";
import mnElementCrane from "/ui/app/components/directives/mn_element_crane/mn_element_crane.js";

import mnSession from "./mn_session_controller.js";

import mnSettingsNotifications from "./mn_settings_notifications_controller.js";
import mnSettingsSampleBuckets from "./mn_settings_sample_buckets_controller.js";
import mnSettingsCluster from "./mn_settings_cluster_controller.js";
import mnSettingsAutoFailover from "./mn_settings_auto_failover_controller.js";
import mnSettingsAutoCompaction from "./mn_settings_auto_compaction_controller.js"
import mnSettingsAlerts from "./mn_settings_alerts_controller.js";
import mnSettingsNotificationsService from "./mn_settings_notifications_service.js";

export default "mnSettings";

angular
  .module('mnSettings', [
    uiRouter,
    mnPluggableUiRegistry,
    mnElementCrane,
    mnSession,
    mnSettingsNotifications,
    mnSettingsSampleBuckets,
    mnSettingsAutoFailover,
    mnSettingsAutoCompaction,
    mnSettingsCluster,
    mnSettingsAlerts,
    mnSettingsNotificationsService
  ])
  .config(mnSettingsConfig)
  .controller("mnSettingsController", mnSettingsController);

function mnSettingsController($scope) {
  var vm = this;
}

function mnSettingsConfig($stateProvider) {

  $stateProvider
    .state('app.admin.settings', {
      url: '/settings',
      abstract: true,
      views: {
        "main@app.admin": {
          templateUrl: 'app/mn_admin/mn_settings.html',
          controller: 'mnSettingsController as settingsCtl'
        }
      },
      data: {
        title: "Settings"
      }
    })
    .state('app.admin.settings.cluster', {
      url: '/cluster',
      views: {
        "": {
          controller: 'mnSettingsClusterController as settingsClusterCtl',
          templateUrl: 'app/mn_admin/mn_settings_cluster.html'
        },
        "autofailover@app.admin.settings.cluster": {
          controller: 'mnSettingsAutoFailoverController as settingsAutoFailoverCtl',
          templateUrl: 'app/mn_admin/mn_settings_auto_failover.html'
        },
        "notifications@app.admin.settings.cluster": {
          controller: 'mnSettingsNotificationsController as settingsNotificationsCtl',
          templateUrl: 'app/mn_admin/mn_settings_notifications.html'
        }
      }
    })
    .state('app.admin.settings.alerts', {
      url: '/alerts',
      controller: 'mnSettingsAlertsController as settingsAlertsCtl',
      templateUrl: 'app/mn_admin/mn_settings_alerts.html',
      data: {
        permissions: 'cluster.settings.read'
      }
    })
    .state('app.admin.settings.autoCompaction', {
      url: '/autoCompaction',
      controller: 'mnSettingsAutoCompactionController as settingsAutoCompactionCtl',
      templateUrl: 'app/mn_admin/mn_settings_auto_compaction.html',
      data: {
        permissions: 'cluster.settings.read'
      }
    })
    .state('app.admin.settings.sampleBuckets', {
      url: '/sampleBuckets',
      controller: 'mnSettingsSampleBucketsController as settingsSampleBucketsCtl',
      templateUrl: 'app/mn_admin/mn_settings_sample_buckets.html',
      data: {
        permissions: 'cluster.samples.read'
      }
    });
}
