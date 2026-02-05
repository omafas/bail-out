/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

(function (Rx) {

  MnHttpInterceptor.annotations = [
    new ng.core.Injectable()
  ];

  MnHttpInterceptor.parameters = [
    mn.services.MnApp
  ];

  MnHttpInterceptor.prototype.intercept = intercept;

  function MnHttpInterceptor(mnAppService) {
    this.httpClientResponse = mnAppService.stream.httpResponse;
  }

  function intercept(req, next) {
    var that = this;

    var mnReq = req.clone({
      setHeaders: {
        'invalid-auth-response': 'on',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'ns-server-ui': 'yes'
      }
    });

    var params;
    var headers;

    if ((req.method === 'POST' || req.method === 'PUT')) {
      if (!req.headers.get('isNotForm')) {
        if (R.is(Object, mnReq.body) && !Array.isArray(mnReq.body)) {
          params = new ng.common.http.HttpParams({
            fromString: new mn.core.jQueryLikeParamSerializer(mnReq.body).toString()
          });
        } else {
          params = mnReq.body;
        }
        mnReq = mnReq.clone({
          body: params,
          responseType: 'text',
          headers: mnReq.headers.set(
            'Content-Type', 'application/x-www-form-urlencoded; charset=UTF-8')});
      }
      if (req.headers.has('isNotForm')) {
        headers = mnReq.headers.delete('isNotForm');
        mnReq = mnReq.clone({headers: headers, responseType: 'text'});
      }
    }

    return next
      .handle(mnReq).pipe(
        Rx.operators.tap(function (event) {
          that.httpClientResponse.next(event);
        }),
        Rx.operators.catchError(function (event) {
          that.httpClientResponse.next(event);
          return Rx.throwError(event);
        })
      );
  }

  AppComponent.annotations = [
    new ng.core.Component({
      selector: "app-root",
      template: '<ui-view class="root-container"></ui-view>' +
      '<div class="global-spinner" [hidden]="!(loading | async)"></div>'
    })
  ];

  AppComponent.parameters = [
    mn.services.MnApp
  ];

  function AppComponent(mnAppService) {
    this.loading = mnAppService.stream.loading;
  }

  AppModule.annotations = [
    new ng.core.NgModule({
      declarations: [
        AppComponent,
        // ServersComponent,
        // OverviewComponent
      ],
      imports: [
        mn.modules.MnElementModule,
        mn.modules.MnPipesModule,
        mn.modules.MnAuth,
        mn.modules.MnAdmin,
        mn.modules.MnWizard,
        ng.platformBrowser.BrowserModule,
        ng.common.http.HttpClientModule,
        ngb.NgbModule.forRoot(),
        window['@uirouter/angular'].UIRouterModule.forRoot({
          states: [{
            name: 'app',
            url: '/?{enableInternalSettings:bool}&{disablePoorMansAlerts:bool}',
            component: AppComponent,
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
          }, {
            name: "app.auth",
            component: mn.components.MnAuth
          }, {
            name: "app.admin",
            abstract: true,
            component: mn.components.MnAdmin
          }],
          useHash: true,
          config: function uiRouterConfigFn(uiRouter, injector) {
            var mnAppService = injector.get(mn.services.MnApp);

            uiRouter.urlRouter.deferIntercept();

            uiRouter.stateService
              .defaultErrorHandler(function (error) {
                mnAppService.stream.appError.next(error);
              });

            uiRouter.urlRouter.otherwise(function () {
              mnAppService.stream.pageNotFound.next(true);
            });
          }
        }),
      ],
      bootstrap: [window["@uirouter/angular"].UIView],
      entryComponents: [
      ],
      providers: [
        mn.services.MnForm,
        mn.services.MnHelper,
        mn.services.MnTasks,
        mn.services.MnAlerts,
        mn.services.MnSession,
        mn.services.MnPools,
        mn.services.MnExceptionHandler,
        mn.services.MnPermissions,
        mn.services.MnBuckets,
        mn.services.MnApp, {
          provide: ng.core.ErrorHandler,
          useClass: mn.services.MnExceptionHandler
        }, {
          provide: ng.common.http.HTTP_INTERCEPTORS,
          useClass: MnHttpInterceptor,
          multi: true
        }
      ]
    })
  ];

  AppModule.parameters = [
    mn.services.MnExceptionHandler,
    ng.platformBrowser.Title,
    mn.services.MnApp,
    mn.services.MnAuth,
    window['@uirouter/angular'].UIRouter,
    mn.services.MnAdmin,
    ngb.NgbModal
  ];

  function AppModule(mnExceptionHandlerService, title, mnAppService, mnAuthService, uiRouter, mnAdminService, ngbModalService) {

    mnAppService
      .stream
      .appError
      .subscribe(function (error) {
        error && mnExceptionHandlerService.handleError(error);
      });

    Rx.merge(mnAppService.stream.http401,
             mnAuthService.stream.postUILogout.response)
      .subscribe(function () {
        uiRouter.stateService.go('app.auth', null, {location: false});
      });

    mnAuthService.stream.postUILogout.response.subscribe(function () {
      ngbModalService.dismissAll();
    });

    mnAppService
      .stream
      .pageNotFound
      .subscribe(function () {
        uiRouter.stateService.go('app.admin.overview');
      });

    mnAdminService
      .stream
      .prettyVersion
      .subscribe(function (version) {
        title.setTitle("Couchbase Console" + (version ? ' ' + version : ''));
      });

    uiRouter.urlRouter.listen();
    uiRouter.urlRouter.sync();
  }

  document.addEventListener('DOMContentLoaded', function () {
    ng.platformBrowserDynamic
      .platformBrowserDynamic()
      .bootstrapModule(AppModule);
  });

})(window.rxjs);
