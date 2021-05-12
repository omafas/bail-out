/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {CommonModule} from '/ui/web_modules/@angular/common.js';
import {BrowserModule} from '/ui/web_modules/@angular/platform-browser.js';
import {HttpClientModule} from '../web_modules/@angular/common/http.js';
import {UIRouterModule, UIView, loadNgModule} from '../web_modules/@uirouter/angular.js';
import {Rejection} from '/ui/web_modules/@uirouter/core.js';
import {MnPipesModule} from './mn.pipes.module.js';
import {MnAppComponent} from './mn.app.component.js';
import {MnAuthComponent} from './mn.auth.component.js';
import {UpgradeModule} from '/ui/web_modules/@angular/upgrade/static.js';
import {MnSharedModule} from './mn.shared.module.js';
import {MnElementCraneModule} from './mn.element.crane.js';
import {UIRouterUpgradeModule} from '/ui/web_modules/@uirouter/angular-hybrid.js';
import * as pluggableUIsModules from '/ui/pluggable-uis.js';

import {MnKeyspaceSelectorModule} from './mn.keyspace.selector.module.js';

import {MnHelper} from '/ui/app/ajs.upgraded.providers.js';


let appState = {
  name: 'app',
  url: '/?{enableInternalSettings:bool}&{disablePoorMansAlerts:bool}',
  component: MnAppComponent,
  params: {
    enableInternalSettings: {
      value: null,
      squash: true,
      dynamic: true
    },
    disablePoorMansAlerts: {
      value: null,
      squash: true,
      dynamic: true
    }
  },
  abstract: true
};

let wizardState = {
  name: 'app.wizard.**',
  lazyLoad: mnLoadNgModule('./mn.wizard.module.js', 'MnWizardModule')
};

let collectionsState = {
  name: 'app.admin.collections.**',
  url: '/collections',
  lazyLoad: mnLoadNgModule('./mn.collections.module.js', 'MnCollectionsModule')
};

let XDCRState = {
  name: 'app.admin.replications.**',
  url: '/replications',
  lazyLoad: mnLoadNgModule('./mn.xdcr.module.js', "MnXDCRModule")
};

let sessionState = {
  name: 'app.admin.security.session.**',
  url: '/session',
  lazyLoad: mnLoadNgModule('./mn.session.module.js', 'MnSessionModule')
};

let logRedactionState = {
  name: 'app.admin.security.redaction.**',
  url: '/redaction',
  lazyLoad: mnLoadNgModule('./mn.security.log.redaction.module.js',
                           'MnSecurityLogRedactionModule')
};

let auditState = {
  name: 'app.admin.security.audit.**',
  url: '/audit',
  lazyLoad: mnLoadNgModule('./mn.security.audit.module.js', 'MnSecurityAuditModule')
};

let overviewState = {
  name: 'app.admin.overview.**',
  url: '/overview',
  lazyLoad: mnLazyload('./mn_admin/mn_overview_controller.js', 'mnOverview')
};

let serversState = {
  name: 'app.admin.servers.**',
  url: '/servers',
  lazyLoad: mnLazyload('./mn_admin/mn_servers_controller.js', 'mnServers')
};

let logsState = {
  name: 'app.admin.logs.**',
  url: '/logs',
  lazyLoad: mnLazyload('./mn_admin/mn_logs_controller.js', "mnLogs")
};

let groupsState = {
  name: 'app.admin.groups.**',
  url: '/groups',
  lazyLoad: mnLazyload('./mn_admin/mn_groups_controller.js', 'mnGroups')
};

let bucketsState = {
  name: 'app.admin.buckets.**',
  url: '/buckets',
  lazyLoad: mnLazyload('./mn_admin/mn_buckets_controller.js', 'mnBuckets')
};

let documentsState = {
  name: "app.admin.documents.**",
  url: "/documents",
  lazyLoad: mnLazyload('./mn_admin/mn_documents_controller.js', 'mnDocuments')
};

let authState = {
  name: "app.auth.**",
  lazyLoad: mnLoadNgModule('./mn.auth.module.js', 'MnAuthModule')
};

let gsiState = {
  name: "app.admin.gsi.**",
  url: "/index",
  lazyLoad: mnLazyload('./mn_admin/mn_gsi_controller.js', 'mnGsi')
};

let viewsState = {
  name: "app.admin.views.**",
  url: "/views",
  lazyLoad: mnLazyload('./mn_admin/mn_views_controller.js', "mnViews")
};

let settingsState = {
  name: "app.admin.settings.**",
  url: "/settings",
  lazyLoad: mnLazyload('./mn_admin/mn_settings_config.js', "mnSettings")
};

let securityState = {
  name: "app.admin.security.**",
  url: "/security",
  lazyLoad: mnLazyload('./mn_admin/mn_security_config.js', "mnSecurity")
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

function mnLazyload(url, module) {
  return ($transition$) => {
    let initialHref = window.location.href;

    let mnHelper = $transition$.injector().get('mnHelper');
    mnHelper.mainSpinnerCounter.increase();

    return import(url).then(m => {
      let postImportHref = window.location.href;

      return initialHref === postImportHref ?
        ocLazyLoad($transition$, module, initialHref) :
        rejectTransition();

    }).finally(() => mnHelper.mainSpinnerCounter.decrease());
  };
}

function mnLoadNgModule(url, module) {
  return (transition, stateObject) => {
    let initialHref = window.location.href;

    let mnHelper = transition.injector().get(MnHelper);
    mnHelper.mainSpinnerCounter.increase();

    let lazyLoadFn = loadNgModule(() => import(url).then(result => {
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
  UpgradeModule,
  UIRouterModule,
  MnPipesModule,
  BrowserModule,
  CommonModule,
  HttpClientModule,
  MnSharedModule,
  MnElementCraneModule.forRoot(),
  UIRouterUpgradeModule.forRoot({
    states: [authState, wizardState, overviewState, serversState, bucketsState, logsState, groupsState, documentsState, gsiState, viewsState, settingsState, securityState, collectionsState, XDCRState, sessionState, logRedactionState, auditState]
  }),

  //downgradedModules
  MnKeyspaceSelectorModule
];

export {mnAppImports, mnLoadNgModule, mnLazyload};
