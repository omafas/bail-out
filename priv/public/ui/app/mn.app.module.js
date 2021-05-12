/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import app from "./app.js";
import { ajsUpgradedProviders } from '/ui/app/ajs.upgraded.providers.js';
import { MnAppComponent } from './mn.app.component.js';
import { MnAppService } from './mn.app.service.js';
import { MnHelperService } from './mn.helper.service.js';
import { MnAdminService } from './mn.admin.service.js';
import { MnTasksService } from './mn.tasks.service.js';
import { MnPoolsService } from './mn.pools.service.js';
import { MnSecurityService } from './mn.security.service.js';
import { mnAppImports } from './mn.app.imports.js';
import { MnFormService } from './mn.form.service.js';
import { MnHttpInterceptor } from './mn.http.interceptor.js';
import { MnExceptionHandlerService } from './mn.exception.handler.service.js';
import { NgModule, ErrorHandler, APP_INITIALIZER} from '/ui/web_modules/@angular/core.js';
import { HTTP_INTERCEPTORS, HttpClient } from '/ui/web_modules/@angular/common/http.js';
import { UIView} from '/ui/web_modules/@uirouter/angular.js';
import { UpgradeModule } from '/ui/web_modules/@angular/upgrade/static.js';
import { NgbModalConfig } from '/ui/web_modules/@ng-bootstrap/ng-bootstrap.js';

export { MnAppModule };

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
        MnSecurityService,
        MnAppService,
        MnAdminService,
        MnTasksService,
        MnPoolsService,
        MnFormService,
        MnHelperService,
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
