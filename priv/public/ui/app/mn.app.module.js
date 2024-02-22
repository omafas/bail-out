/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// app import should go first in order to load AngularJS before
import app from './app.js';

import {NgModule, ErrorHandler} from '@angular/core';
import {HTTP_INTERCEPTORS} from '@angular/common/http';
import {UpgradeModule, setAngularJSGlobal} from '@angular/upgrade/static';
import angular from 'angular';
setAngularJSGlobal(angular);

import {NgbModalConfig} from '@ng-bootstrap/ng-bootstrap';
import {ClipboardService} from 'ngx-clipboard';

import {mnAppImports} from './mn.app.imports.js';
import {MnHttpInterceptor} from './mn.http.interceptor.js';

import {ajsUpgradedProviders} from './ajs.upgraded.providers.js';
import {MnAppService} from './mn.app.service.js';
import {MnTasksService} from './mn.tasks.service.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnServerGroupsService} from './mn.server.groups.service.js';
import {MnFormService} from './mn.form.service.js';
import {MnExceptionHandlerService} from './mn.exception.handler.service.js';
import {MnCollectionsService} from './mn.collections.service.js';
import {MnSettingsSampleBucketsService} from './mn.settings.sample.buckets.service.js';
import {MnKeyspaceSelectorService} from './mn.keyspace.selector.service.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnSettingsAutoCompactionService} from './mn.settings.auto.compaction.service.js';
import {MnAuthService} from './mn.auth.service.js';
import {MnElementCraneService} from './mn.element.crane.js';
import {MnBucketsService} from './mn.buckets.service.js';
import {MnWizardService} from './mn.wizard.service.js';
import {MnLogsCollectInfoService} from './mn.logs.collectInfo.service.js';
import {MnPermissionsService} from './mn.permissions.service.js';
import {MnUserRolesService} from './mn.user.roles.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnAlertsService} from './mn.alerts.service.js';
import {MnXDCRService} from "./mn.xdcr.service.js";
import {MnStatsService} from './mn.stats.service.js';
import {MnSettingsAlertsService} from './mn.settings.alerts.service.js';
import {MnLogsListService} from './mn.logs.list.service.js';
import {MnSessionService} from './mn.session.service.js';
import {MnViewsListService} from './mn.views.list.service.js';
import {MnViewsEditingService} from './mn.views.editing.service.js';
import {MnRouterService} from './mn.router.service.js';
import {MnDocumentsService} from './mn.documents.service.js';


export {MnAppModule};

class MnAppModule {
  static get annotations() { return [
    new NgModule({
      declarations: [

      ],
      entryComponents: [

      ],
      imports: mnAppImports,
      // bootstrap: [
      //   UIView
      // ],
      providers: [
        ...ajsUpgradedProviders,
        ClipboardService,
        MnAppService,
        MnTasksService,
        MnSecurityService,
        MnServerGroupsService,
        MnFormService,
        MnExceptionHandlerService,
        MnCollectionsService,
        MnSettingsSampleBucketsService,
        MnKeyspaceSelectorService,
        MnHelperService,
        MnSettingsAutoCompactionService,
        MnAuthService,
        MnElementCraneService,
        MnBucketsService,
        MnWizardService,
        MnLogsCollectInfoService,
        MnPermissionsService,
        MnUserRolesService,
        MnPoolsService,
        MnAdminService,
        MnAlertsService,
        MnXDCRService,
        MnStatsService,
        MnSettingsAlertsService,
        MnLogsListService,
        MnSessionService,
        MnViewsListService,
        MnViewsEditingService,
        MnRouterService,
        MnDocumentsService,
        {
          provide: HTTP_INTERCEPTORS,
          useClass: MnHttpInterceptor,
          multi: true
        }, {
          provide: ErrorHandler,
          useClass: MnExceptionHandlerService
        },
        NgbModalConfig
      ]
    })
  ]}

  static get parameters() {return  [
    UpgradeModule,
    NgbModalConfig
  ]}

  ngDoBootstrap() {
    this.upgrade.bootstrap(document, [app], { strictDi: false });
  }

  constructor(upgrade, ngbModalConfig) {
    this.upgrade = upgrade;
    ngbModalConfig.backdrop = "static";
  }

}
