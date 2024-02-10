/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnAdmin =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnAdminComponent, mn.core.MnEventableComponent);

    MnAdminComponent.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-admin.html",
        animations: [
          ng.animations.trigger(
            'mnMinimize', [
              ng.animations.state(
                ':enter',
                ng.animations.style({opacity: '0', height: '0'}),
                // ng.animations.animation('500ms',
                //                         ng.animations.style({opacity: '1', height: '2rem'}))
              ),
              ng.animations.state(
                ':leave',
                ng.animations.style({opacity: '1', height: '2rem'}),
                ng.animations.animation('500ms',
                                        ng.animations.style({opacity: '0', height: '0'}))
              )
            ]),
          ng.animations.trigger(
            'mnAnimateHeight', [

              ng.animations.transition(':enter', [
                ng.animations.style({opacity: 0, height: 0}),  // initial
                ng.animations.animate('0.4s ease',
                        ng.animations.style({opacity: 1, height: '*' }))  // final
              ]),

              ng.animations.transition(':leave', [
                ng.animations.style({opacity: 1, height: '*' }),
                ng.animations.animate('0.4s ease',
                        ng.animations.style({
                          opacity: 0,
                          height: '0px'
                        }))
              ])

            ]
          )
        ]
      })
    ];

    MnAdminComponent.parameters = [
      mn.services.MnAuth,
      mn.services.MnAdmin,
      mn.services.MnPools,
      mn.services.MnPermissions,
      mn.services.MnTasks,
      mn.services.MnAlerts,
      mn.services.MnSession,
      window['@uirouter/angular'].UIRouter
    ];

    MnAdminComponent.prototype.onLogout = onLogout;
    MnAdminComponent.prototype.runInternalSettingsDialog = runInternalSettingsDialog;
    MnAdminComponent.prototype.showResetPasswordDialog = showResetPasswordDialog;
    MnAdminComponent.prototype.toggleProgressBar = toggleProgressBar;
    MnAdminComponent.prototype.handleGetPoolsDefaultSuccess = handleGetPoolsDefaultSuccess;
    MnAdminComponent.prototype.handleGetPoolsDefaultError = handleGetPoolsDefaultError;

    return MnAdminComponent;

    function MnAdminComponent(mnAuthService, mnAdminService,
                              mnPoolsService,
                              mnPermissionsService,
                              mnTasksService,
                              mnAlertsService,
                              mnSessionService,
                              uiRouter
                             ) {
      mn.core.MnEventableComponent.call(this);
      mnSessionService.activate(this.mnOnDestroy);

      //MnExceptionHandlerService.stream.appException.subscribe(MnExceptionHandlerService.send);

      this.postUILogout = mnAuthService.stream.postUILogout;
      this.isProgressBarClosed = new Rx.BehaviorSubject(true);
      this.getPoolsDefaultEtag = mnAdminService.stream.etag;
      this.showRespMenu = false;
      this.closeAlert = mnAlertsService.removeItem.bind(mnAlertsService);

      this.majorMinorVersion = mnPoolsService.stream.majorMinorVersion;
      this.tasksToDisplay = mnTasksService.stream.tasksToDisplay;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.whomiId = mnAdminService.stream.whomi.pipe(Rx.operators.pluck("id"));
      this.mnAlerts = mnAlertsService.stream.alerts;
      this.stateService = uiRouter.stateService;

      this.tasksRead = mnPermissionsService.createPermissionStream("tasks!read");
      this.securityRead = mnPermissionsService.createPermissionStream("security!read");
      this.extractNextInterval = mnTasksService.stream.extractNextInterval;
      this.clusterName = mnAdminService.stream.clusterName;

      this.bucketSettingsAnyRead =
        mnPermissionsService.createPermissionStream("settings!read", ".");

      mnAdminService.stream.getPoolsDefault
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(R.pipe(R.path(['etag']),
                          this.handleGetPoolsDefaultSuccess.bind(this)),
                   this.handleGetPoolsDefaultError.bind(this));

      this.isAdminRootReady =
        mnAdminService.stream.getPoolsDefault.pipe(Rx.operators.map(Boolean));

      this.enableResetButton =
        Rx.combineLatest(mnPoolsService.stream.isEnterprise,
                         mnAdminService.stream.whomi
                         .pipe(Rx.operators.map(R.anyPass([R.propEq('domain', 'local'),
                                                           R.propEq('domain', 'admin')]))))
        .pipe(Rx.operators.map(R.all(R.equals(true))));

      this.enableInternalSettings =
        Rx.combineLatest(mnAdminService.stream.enableInternalSettings,
                         mnPermissionsService.createPermissionStream("admin.settings!write"))
        .pipe(Rx.operators.map(R.all(R.equals(true))));

      this.tasksRead
        .pipe(Rx.operators.switchMap(R.ifElse(R.equals(true),
                                              R.always(this.extractNextInterval),
                                              R.always(Rx.NEVER))),
              Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function (interval) {
          mnTasksService.stream.interval.next(interval);
        });

      this.mnOnDestroy.subscribe(this.handleGetPoolsDefaultSuccess.bind(this));
    }

    function handleGetPoolsDefaultSuccess(v) {
      this.getPoolsDefaultEtag.next(v);
    }

    function handleGetPoolsDefaultError(rv) {
      if ((rv instanceof ng.common.http.HttpErrorResponse) && (rv.status === 404)) {
        this.stateService.go('app.wizard.welcome', null, {location: false});
      }
    }

    function onLogout() {
      this.postUILogout.post(true);
    }

    function runInternalSettingsDialog() {

    }

    function showResetPasswordDialog() {

    }

    function toggleProgressBar() {
      this.isProgressBarClosed.next(!this.isProgressBarClosed.getValue());
    }
  })(window.rxjs);


var mn = mn || {};
mn.modules = mn.modules || {};
mn.modules.MnAdmin =
  (function () {
    "use strict";

    OverviewComponent.annotations = [
      new ng.core.Component({
        template: 'overview'
      })
    ];
    function OverviewComponent() {
    }

    ServersComponent.annotations = [
      new ng.core.Component({
        template: '<mn-element-cargo depot="alerts">asdasdasdas</mn-element-cargo>'
      })
    ];
    function ServersComponent() {
    }

    MnAdminModule.annotations = [
      new ng.core.NgModule({
        declarations: [
          mn.directives.MnDraggable,
          mn.components.MnAdmin,
          OverviewComponent,
          mn.components.MnServers,
          mn.components.MnServersItem,
          mn.components.MnServersItemDetails,
          mn.components.MnBuckets,
          mn.components.MnXDCR,
          mn.components.MnBucketsItem,
          mn.components.MnBucketsItemDetails,
          mn.components.MnBarUsage,
          mn.components.MnWarmupProgress,
          mn.components.MnBucketsDialog,
          mn.components.MnSessionTimeoutDialog,
          mn.components.MnServersEjectDialog,
          mn.components.MnServersStopRebalanceDialog,
          mn.components.MnServersAddDialog,
          mn.components.MnServersFailoverDialog,
          mn.components.MnServersFailoverConfirmationDialog,
          mn.components.MnXDCRAddReference,
          mn.components.MnXDCRDeleteReference,
          mn.components.MnXDCRItem,
          mn.components.MnXDCRAddReplication,
          mn.components.MnXDCRSettings,
          mn.components.MnXDCRDelete,
          mn.components.MnXDCREdit
        ],
        imports: [
          window['@uirouter/angular'].UIRouterModule.forChild({
            states: [{
              name: "app.admin.overview",
              url: "overview",
              views: {
                "main@app.admin": OverviewComponent
              },
              data: {
                title: "Dashboard"
              }
            }, {
              name: "app.admin.servers",
              url: "servers?openedServers",
              params: {
                openedServers: {
                  array: true,
                  dynamic: true
                }
              },
              views: {
                "main@app.admin": mn.components.MnServers
              },
              data: {
                title: "Servers"
              }
            }, {
              name: "app.admin.buckets",
              url: "buckets?openedBuckets",
              params: {
                openedBuckets: {
                  array: true,
                  dynamic: true
                }
              },
              views: {
                "main@app.admin": mn.components.MnBuckets
              },
              data: {
                title: "Buckets"
              }
            }, {
              name: 'app.admin.replications',
              url: 'replications',
              views: {
                "main@app.admin": mn.components.MnXDCR
              },
              data: {
                title: "XDCR"
              }
            }, {
              name: "app.admin.security",
              url: "security",
              views: {
                "main@app.admin": mn.components.MnSecurity
              },
              data: {
                title: "Security"
              }
            }, {
              name: "app.admin.settings",
              url: "settings",
              views: {
                "main@app.admin": mn.components.MnSettings
              },
              data: {
                title: "Settingsy"
              }
            }]
          }),
          ng.forms.ReactiveFormsModule,
          mn.modules.MnShared,
          mn.modules.MnSecurity,
          mn.modules.MnSettings,
          mn.modules.MnElementModule,
          mn.modules.MnPipesModule,
          ng.platformBrowser.BrowserModule,
          ngb.NgbModule,
          ng.platformBrowser.animations.BrowserAnimationsModule
        ],
        providers: [
          mn.services.MnAdmin,
          mn.services.MnServers,
          mn.services.MnGSI,
          mn.services.MnXDCR,
          mn.services.MnGroups
        ],
        entryComponents: [
          mn.components.MnSessionTimeoutDialog,
          mn.components.MnBucketsDialog,
          mn.components.MnServersEjectDialog,
          mn.components.MnServersStopRebalanceDialog,
          mn.components.MnServersAddDialog,
          mn.components.MnServersFailoverDialog,
          mn.components.MnServersFailoverConfirmationDialog,
          mn.components.MnXDCRAddReference,
          mn.components.MnXDCRDeleteReference,
          mn.components.MnXDCRAddReplication,
          mn.components.MnXDCRDelete,
          mn.components.MnXDCREdit
        ]
      })
    ];

    return MnAdminModule;

    function MnAdminModule() {
    }
  })();
