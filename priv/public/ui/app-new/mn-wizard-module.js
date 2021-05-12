/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.modules = mn.modules || {};
mn.modules.MnWizard =
  (function (Rx) {
    "use strict";

    MnWizardComponent.annotations = [
      new ng.core.Component({
        templateUrl: 'app-new/mn-wizard.html'
      })
    ];

    MnWizardComponent.parameters = [
      mn.services.MnWizard,
      mn.services.MnPools,
      mn.services.MnAdmin
    ];

    function MnWizardComponent(mnWizardService, mnPoolsService, mnAdminService) {
      var newClusterConfig = mnWizardService.wizardForm.newClusterConfig;
      var joinCluster = mnWizardService.wizardForm.joinCluster;

      //MnExceptionHandlerService.stream.appException.subscribe(MnExceptionHandlerService.send);

      mnAdminService.stream.implementationVersion
        .pipe(Rx.operators.first())
        .subscribe(function (implementationVersion) {
          mnWizardService.initialValues.implementationVersion = implementationVersion;
        });

      mnWizardService.stream.getSelfConfigFirst
        .subscribe(function (selfConfig) {
          var hostname = selfConfig['otpNode'].split('@')[1] || '127.0.0.1';
          if (hostname == "cb.local") {
              hostname = "127.0.0.1";
          }
          newClusterConfig.get("clusterStorage.hostname").setValue(hostname);
          joinCluster.get("clusterStorage.hostname").setValue(hostname);
          mnWizardService.initialValues.hostname = hostname;
        });

      function servicesToGroup(services, value) {
        return new ng.forms.FormGroup(services
                                      .reduce(function (acc, name) {
                                        acc[name] = new ng.forms.FormControl(value);
                                        return acc;
                                      }, {}));
      }

      mnPoolsService.stream.mnServices
        .pipe(Rx.operators.first())
        .subscribe(function (services) {
          newClusterConfig.get("services").addControl("flag", servicesToGroup(services, true));
          joinCluster.get("services").addControl("flag", servicesToGroup(services, true));
          newClusterConfig.get("services.flag.kv").disable({onlySelf: true});
        });

      mnPoolsService.stream.quotaServices
        .pipe(Rx.operators.first())
        .subscribe(function (services) {
          newClusterConfig.get("services").addControl("field", servicesToGroup(services, null));
        });

      mnPoolsService.stream.isEnterprise
        .pipe(Rx.operators.first())
        .subscribe(function (isEnterprise) {
          var storageMode = isEnterprise ? "plasma" : "forestdb";
          newClusterConfig.get("storageMode").setValue(storageMode);

          if (!isEnterprise) {
            joinCluster.get("clusterStorage.storage.cbas_path").disable({onlySelf: true});
            newClusterConfig.get("clusterStorage.storage.cbas_path").disable({onlySelf: true});
          }

          mnWizardService.initialValues.storageMode = storageMode;
        });

      mnWizardService.stream.initHddStorage
        .pipe(Rx.operators.first())
        .subscribe(function (initHdd) {
          newClusterConfig.get("clusterStorage.storage").patchValue(initHdd);
          joinCluster.get("clusterStorage.storage").patchValue(initHdd);

          mnWizardService.initialValues.clusterStorage = initHdd;
        });
    }

    MnWizardModule.annotations = [
      new ng.core.NgModule({
        declarations: [
          MnWizardComponent,
          mn.components.MnNewCluster,
          mn.components.MnNewClusterConfig,
          mn.components.MnTermsAndConditions,
          mn.components.MnWelcome,
          mn.components.MnNodeStorageConfig,
          mn.components.MnQuerySettingsConfig,
          mn.components.MnStorageMode,
          mn.components.MnJoinCluster,
          mn.components.MnPathField
        ],
        imports: [
          ng.platformBrowser.BrowserModule,
          ng.forms.ReactiveFormsModule,
          mn.modules.MnShared,
          mn.modules.MnPipesModule,
          ng.common.http.HttpClientJsonpModule,
          window['@uirouter/angular'].UIRouterModule.forChild({
            states: [{
              name: "app.wizard",
              component: MnWizardComponent,
              abstract: true
            }, {
              name: "app.wizard.welcome",
              component: mn.components.MnWelcome
            }, {
              name: "app.wizard.newCluster",
              component: mn.components.MnNewCluster
            }, {
              name: "app.wizard.joinCluster",
              component: mn.components.MnJoinCluster
            }, {
              name: "app.wizard.termsAndConditions",
              component: mn.components.MnTermsAndConditions
            }, {
              name: "app.wizard.newClusterConfig",
              component: mn.components.MnNewClusterConfig
            }]
          })
        ],
        providers: [
          mn.services.MnWizard
        ]
      })
    ];

    return MnWizardModule;

    function MnWizardModule() {
    }
  })(window.rxjs);
