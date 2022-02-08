/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import uiRouter from "@uirouter/angularjs";

import mnPluggableUiRegistry from "../components/mn_pluggable_ui_registry.js";
import mnElementCrane from "../components/directives/mn_element_crane/mn_element_crane.js";

import mnSettingsNotifications from "./mn_settings_notifications_controller.js";
import mnSettingsCluster from "./mn_settings_cluster_controller.js";
import mnSettingsAutoFailover from "./mn_settings_auto_failover_controller.js";
import mnSettingsNotificationsService from "./mn_settings_notifications_service.js";

import mnSettingsTemplate from "./mn_settings.html";
import mnSettingsClusterTemplate from "./mn_settings_cluster.html";
import mnSettingsAutoFailoverTemplate from "./mn_settings_auto_failover.html";
import mnSettingsNotificationsTemplate from "./mn_settings_notifications.html";

export default "mnSettings";

angular
  .module('mnSettings', [
    uiRouter,
    mnPluggableUiRegistry,
    mnElementCrane,
    mnSettingsNotifications,
    mnSettingsAutoFailover,
    mnSettingsCluster,
    mnSettingsNotificationsService
  ])
  .config(["$stateProvider", mnSettingsConfig])
  .controller("mnSettingsController", mnSettingsController);

function mnSettingsController() {
}

function mnSettingsConfig($stateProvider) {

  $stateProvider
    .state('app.admin.settings', {
      url: '/settings',
      abstract: true,
      views: {
        "main@app.admin": {
          template: mnSettingsTemplate,
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
          template: mnSettingsClusterTemplate
        },
        "autofailover@app.admin.settings.cluster": {
          controller: 'mnSettingsAutoFailoverController as settingsAutoFailoverCtl',
          template: mnSettingsAutoFailoverTemplate
        },
        "notifications@app.admin.settings.cluster": {
          controller: 'mnSettingsNotificationsController as settingsNotificationsCtl',
          template: mnSettingsNotificationsTemplate
        }
      }
    })
}
