/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {CommonModule} from '@angular/common';
import {BrowserModule} from '@angular/platform-browser';
import {HttpClientModule} from '@angular/common/http';
import {UIRouterModule, loadNgModule} from '@uirouter/angular';
import {Rejection} from '@uirouter/core';
import {UIRouterUpgradeModule} from '@uirouter/angular-hybrid';

import {MnPipesModule} from './mn.pipes.module.js';
import {MnSharedModule} from './mn.shared.module.js';
import {MnElementCraneModule} from './mn.element.crane.js';
import * as pluggableUIsModules from '../../pluggable-uis.js';
import {MnKeyspaceSelectorModule} from './mn.keyspace.selector.module.js';
import {MnHelper} from './ajs.upgraded.providers.js';

let wizardState = {
  name: 'app.wizard.**',
  lazyLoad: mnLoadNgModule(() => import('./mn.wizard.module.js'), 'MnWizardModule')
};

let collectionsState = {
  name: 'app.admin.collections.**',
  url: '/collections',
  lazyLoad: mnLoadNgModule(() => import('./mn.collections.module.js'), 'MnCollectionsModule')
};

let XDCRState = {
  name: 'app.admin.replications.**',
  url: '/replications',
  lazyLoad: mnLoadNgModule(() => import('./mn.xdcr.module.js'), "MnXDCRModule")
};

let otherSecuritySettingsState = {
  name: 'app.admin.security.other.**',
  url: '/other',
  lazyLoad: mnLoadNgModule(() => import('./mn.security.other.module.js'), 'MnSecurityOtherModule')
};

let auditState = {
  name: 'app.admin.security.audit.**',
  url: '/audit',
  lazyLoad: mnLoadNgModule(() => import('./mn.security.audit.module.js'), 'MnSecurityAuditModule')
};

let overviewState = {
  name: 'app.admin.overview.**',
  url: '/overview',
  lazyLoad: mnLazyload(() => import('./mn_admin/mn_overview_controller.js'), 'mnOverview')
};

let serversState = {
  name: 'app.admin.servers.**',
  url: '/servers',
  lazyLoad: mnLazyload(() => import('./mn_admin/mn_servers_controller.js'), 'mnServers')
};

let logsState = {
  name: 'app.admin.logs.**',
  url: '/logs',
  lazyLoad: mnLoadNgModule(() => import('./mn.logs.module.js'), 'MnLogsModule')
};

let logsListState = {
  name: "app.admin.logs.list.**",
  url: "",
  lazyLoad: mnLoadNgModule(() => import('./mn.logs.list.module.js'), "MnLogsListModule")
};

let logsCollectInfo = {
  name: "app.admin.logs.collectInfo.**",
  url: "/collectInfo",
  lazyLoad: mnLoadNgModule(() => import('./mn.logs.collectInfo.module.js'), "MnLogsCollectInfoModule")
}

let groupsState = {
  name: 'app.admin.groups.**',
  url: '/groups',
  lazyLoad: mnLazyload(() => import('./mn_admin/mn_groups_controller.js'), 'mnGroups')
};

let bucketsState = {
  name: 'app.admin.buckets.**',
  url: '/buckets',
  lazyLoad: mnLoadNgModule(() => import('./mn.buckets.module.js'), 'MnBucketsModule')
};

let authState = {
  name: "app.auth.**",
  lazyLoad: mnLoadNgModule(() => import('./mn.auth.module.js'), 'MnAuthModule')
};

let gsiState = {
  name: "app.admin.gsi.**",
  url: "/index",
  lazyLoad: mnLazyload(() => import('./mn_admin/mn_gsi_controller.js'), 'mnGsi')
};

let viewsState = {
  name: "app.admin.views.**",
  url: "/views",
  lazyLoad: mnLoadNgModule(() => import('./mn.views.module.js'), "MnViewsModule")
};

let settingsState = {
  name: "app.admin.settings.**",
  url: "/settings",
  lazyLoad: mnLazyload(() => import('./mn_admin/mn_settings_config.js'), "mnSettings")
};

let sampleBucketState = {
  name: 'app.admin.settings.sampleBuckets.**',
  url: '/sampleBuckets',
  lazyLoad: mnLoadNgModule(() => import('./mn.settings.sample.buckets.module.js'), 'MnSettingsSampleBucketsModule')
};

let alertsState = {
  name: "app.admin.settings.alerts.**",
  url: "/alerts",
  lazyLoad: mnLoadNgModule(() => import('./mn.settings.alerts.module.js'), "MnSettingsAlertsModule")
};

let autoCompactionState = {
  name: 'app.admin.settings.autoCompaction.**',
  url: '/autoCompaction',
  lazyLoad: mnLoadNgModule(() => import('./mn.settings.auto.compaction.module.js'), 'MnSettingsAutoCompactionModule')
}

let securityState = {
  name: "app.admin.security.**",
  url: "/security",
  lazyLoad: mnLazyload(() => import('./mn_admin/mn_security_config.js'), "mnSecurity")
};

function rejectTransition() {
  return Promise.reject(Rejection.superseded("Lazy loading has been suppressed by another transition"));
}

function ocLazyLoad($transition$, module, initialHref) {
  return $transition$
    .injector()
    .get('$ocLazyLoad')
    .load({name: module})
    .then(result => {
      let postLoadHref = window.location.href;
      return initialHref === postLoadHref ? result : rejectTransition();
    });
}

function mnLazyload(doImport, module) {
  return ($transition$) => {
    let initialHref = window.location.href;

    let mnHelper = $transition$.injector().get('mnHelper');
    mnHelper.mainSpinnerCounter.increase();

    return (typeof doImport == "function" ? doImport() : import(doImport)).then(() => {
      let postImportHref = window.location.href;

      return initialHref === postImportHref ?
        ocLazyLoad($transition$, module, initialHref) :
        rejectTransition();

    }).finally(() => mnHelper.mainSpinnerCounter.decrease());
  };
}

function mnLoadNgModule(doImport, module) {
  return (transition, stateObject) => {
    let initialHref = window.location.href;

    let mnHelper = transition.injector().get(MnHelper);
    mnHelper.mainSpinnerCounter.increase();

    let lazyLoadFn = loadNgModule(() =>
      (typeof doImport == "function" ? doImport() : import(doImport)).then(result => {
        let postImportHref = window.location.href;
        return initialHref === postImportHref ? result[module] : rejectTransition();
      }));

    return lazyLoadFn(transition, stateObject).then(result => {
      let postLoadHref = window.location.href;
      return initialHref === postLoadHref ? result : rejectTransition();

    }).finally(() => mnHelper.mainSpinnerCounter.decrease());
  };
}

let mnAppImports = [
  ...Object.values(pluggableUIsModules),
  UIRouterModule,
  MnPipesModule,
  BrowserModule,
  CommonModule,
  HttpClientModule,
  MnSharedModule,
  MnElementCraneModule,
  UIRouterUpgradeModule.forRoot({
    states: [
      authState,
      wizardState,
      overviewState,
      serversState,
      bucketsState,
      logsState,
      logsListState,
      logsCollectInfo,
      alertsState,
      groupsState,
      gsiState,
      viewsState,
      settingsState,
      sampleBucketState,
      autoCompactionState,
      securityState,
      collectionsState,
      XDCRState,
      otherSecuritySettingsState,
      auditState
    ]
  }),

  //downgradedModules
  MnKeyspaceSelectorModule
];

export {mnAppImports, mnLoadNgModule, mnLazyload};
