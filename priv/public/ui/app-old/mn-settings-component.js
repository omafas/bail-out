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
mn.components.MnSettings =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnSettings, mn.core.MnEventableComponent);

    MnSettings.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-settings.html"
      })
    ];

    MnSettings.parameters = [
      mn.services.MnPermissions,
      mn.services.MnPools,
      mn.services.MnAdmin
    ];

    return MnSettings;

    function MnSettings(mnPermissionsService, mnPoolsService, mnAdminService) {
      mn.core.MnEventableComponent.call(this);

      this.settingsRead = mnPermissionsService.createPermissionStream("settings!read");
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
    }

  })(window.rxjs);


var mn = mn || {};
mn.modules = mn.modules || {};
mn.modules.MnSettings =
  (function () {
    "use strict";

    MnSettingsModule.annotations = [
      new ng.core.NgModule({
        declarations: [
          mn.components.MnSettings,
          mn.components.MnEmailAlerts,
          mn.components.MnAutoCompaction,
          mn.components.MnGeneralSettings
        ],
        imports: [
          window['@uirouter/angular'].UIRouterModule.forChild({
            states: [
              {
                name: 'app.admin.settings.emailAlerts',
                url: '/alerts',
                component: mn.components.MnEmailAlerts
                // data: {
                //   permissions: 'cluster.settings.read'
                // }
              },

              {
                name: "app.admin.settings.autoCompaction",
                url: '/autoCompaction',
                component: mn.components.MnAutoCompaction,
                // data: {
                //   permissions: "cluster.admin.security.read"
                // }
              },

              {
                name: 'app.admin.settings.generalSettings',
                url: '/cluster',
                component: mn.components.MnGeneralSettings
              }
            ]
          }),
          ng.forms.ReactiveFormsModule,
          mn.modules.MnShared,
          mn.modules.MnElementModule,
          mn.modules.MnPipesModule,
          ng.platformBrowser.BrowserModule,
          ngb.NgbModule,
          // ng.platformBrowser.animations.BrowserAnimationsModule
        ],
        providers: [
          mn.services.MnSettings
        ],
        entryComponents: [
          // mn.components.MnBucketsDialog
        ]
      })
    ];

    return MnSettingsModule;

    function MnSettingsModule() {
    }
  })();
