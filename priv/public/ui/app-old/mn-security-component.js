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
mn.components.MnSecurity =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnSecurity, mn.core.MnEventableComponent);

    MnSecurity.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-security.html"
      })
    ];

    MnSecurity.parameters = [
      mn.services.MnPermissions,
      mn.services.MnPools,
      mn.services.MnAdmin
    ];

    return MnSecurity;

    function MnSecurity(mnPermissionsService, mnPoolsService, mnAdminService) {
      mn.core.MnEventableComponent.call(this);

      this.securityRead = mnPermissionsService.createPermissionStream("admin.security!read");
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.compatVersion55 = mnAdminService.stream.compatVersion55;
    }

  })(window.rxjs);


var mn = mn || {};
mn.modules = mn.modules || {};
mn.modules.MnSecurity =
  (function () {
    "use strict";

    MnSecurityModule.annotations = [
      new ng.core.NgModule({
        declarations: [
          mn.components.MnUserRoles,
          mn.components.MnUserRolesItem,
          mn.components.MnRootCertificate,
          mn.components.MnClientCertificate,
          mn.components.MnLogRedaction,
          mn.components.MnAudit,
          mn.components.MnAuditItem,
          mn.components.MnSession,
          mn.components.MnSecurity
        ],
        imports: [
          window['@uirouter/angular'].UIRouterModule.forChild({
            states: [
              {
                name: "app.admin.security.userRoles",
                url: "/userRoles?openedUsers&startFrom&startFromDomain&{pageSize:int}",
                component: mn.components.MnUserRoles,
                params: {
                  openedUsers: {
                    array: true,
                    dynamic: true
                  },
                  startFrom: {
                    value: null,
                    dynamic: true
                  },
                  startFromDomain: {
                    value: null,
                    dynamic: true
                  },
                  pageSize: {
                    value: 10,
                    dynamic: true
                  }
                },
                // data: {
                //   compat: "atLeast50"
                // }
              }, {
                name: "app.admin.security.rootCertificate",
                url: "/rootCertificate",
                component: mn.components.MnRootCertificate,
                // data: {
                //   enterprise: true
                // }
              }, {
                name: "app.admin.security.logRedaction",
                url: "/redaction",
                component: mn.components.MnLogRedaction,
                // data: {
                // compat: "atLeast55",
                // enterprise: true
                // }
              }, {
                name: 'app.admin.security.clientCert',
                url: '/clientCert',
                component: mn.components.MnClientCertificate,
                // data: {
                //   compat: "atLeast50",
                //   enterprise: true
                // }
              }, {
                name: 'app.admin.security.audit',
                url: '/audit',
                component: mn.components.MnAudit
                // data: {
                //   enterprise: true,
                //   compat: "atLeast40"
              },

              {
                name: "app.admin.security.session",
                url: '/session',
                component: mn.components.MnSession,
                // data: {
                //   permissions: "cluster.admin.security.read"
                // }
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
          mn.services.MnSecurity,
          mn.services.MnUserRoles
        ],
        entryComponents: [
          // mn.components.MnBucketsDialog
        ]
      })
    ];

    return MnSecurityModule;

    function MnSecurityModule() {
    }
  })();
