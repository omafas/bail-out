/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { MnAppComponent } from './mn.app.component.js';
import { MnAppService } from './mn.app.service.js';
import { MnPoolsService } from './mn.pools.service.js';
import { mnAppImports } from './mn.app.imports.js';
import { MnFormService } from './mn.form.service.js';
import { MnAlertsService } from './mn.alerts.service.js';
import { MnHttpInterceptor } from './mn.http.interceptor.js';
import { MnExceptionHandlerService } from './mn.exception.handler.service.js';
import { NgModule, ErrorHandler} from '../web_modules/@angular/core.js';
import { HTTP_INTERCEPTORS, HttpClient } from '../web_modules/@angular/common/http.js';
import { UIView, UIRouter} from '../web_modules/@uirouter/angular.js';

export { MnAppModule };

class MnAppModule {
  static annotations = [
    new NgModule({
      declarations: [
        MnAppComponent
      ],
      imports: mnAppImports,
      bootstrap: [
        UIView
      ],
      providers: [
        MnAppService,
        MnPoolsService,
        MnFormService,
        MnAlertsService,
        {
          provide: HTTP_INTERCEPTORS,
          useClass: MnHttpInterceptor,
          multi: true
        }, {
          provide: ErrorHandler,
          useClass: MnExceptionHandlerService
        }
      ]
    })
  ]

  static parameters = [
    ErrorHandler,
    UIRouter,
    MnPoolsService
  ]

  constructor(mnExceptionHandlerService, uiRouter, mnPoolsService) {
    mnExceptionHandlerService.activate();
    // setTimeout(function () {a}, 1000)
    mnPoolsService.get().toPromise().then(function () {

    }, function () {
      uiRouter.stateService.go('app.auth', null, {location: false});
    });

    uiRouter.urlRouter.listen();
    uiRouter.urlRouter.sync();
  }
}
