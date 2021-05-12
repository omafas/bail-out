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
mn.components.MnNewClusterConfig =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnNewClusterConfig, mn.core.MnEventableComponent);

    MnNewClusterConfig.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-new-cluster-config.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnNewClusterConfig.parameters = [
      mn.services.MnWizard,
      mn.services.MnAdmin,
      mn.services.MnPools,
      mn.services.MnApp,
      mn.services.MnAuth,
      window['@uirouter/angular'].UIRouter
    ];

    return MnNewClusterConfig;

    function MnNewClusterConfig(mnWizardService, mnAdminService, mnPoolsService, mnAppService, mnAuthService, uiRouter) {
      mn.core.MnEventableComponent.call(this);

      this.onSubmit = new Rx.Subject();

      this.wizardForm = mnWizardService.wizardForm;

      this.mnAppLoding = mnAppService.stream.loading;

      this.newClusterConfigForm = mnWizardService.wizardForm.newClusterConfig;

      this.getServicesValues = mnWizardService.getServicesValues.bind(mnWizardService);

      this.totalRAMMegs = mnWizardService.stream.totalRAMMegs;
      this.maxRAMMegs = mnWizardService.stream.maxRAMMegs;
      this.memoryQuotasFirst = mnWizardService.stream.memoryQuotasFirst;

      this.servicesHttp = mnWizardService.stream.servicesHttp;
      this.groupHttp = mnWizardService.stream.groupHttp;
      this.initialValues = mnWizardService.initialValues;

      this.isButtonDisabled =
        mnAdminService.stream.postPoolsDefault.error.pipe(
          Rx.operators.map(function (error) {
            return error && !_.isEmpty(error.errors);
          })
        );

      Rx.merge(
        mnWizardService.stream.groupHttp.loading,
        mnWizardService.stream.secondGroupHttp.loading
      ).pipe(
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(this.mnAppLoding.next.bind(this.mnAppLoding));

      mnWizardService.stream.groupHttp.success.pipe(
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(function (result) {
        mnWizardService.stream.secondGroupHttp.post({
          indexesHttp: {
            storageMode: mnWizardService.wizardForm.newClusterConfig.get("storageMode").value
          },
          authHttp: [mnWizardService.wizardForm.newCluster.value.user, false]
        });
      });

      mnWizardService.stream.secondGroupHttp.success.pipe(
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(function () {
        mnAuthService.stream.postUILogin.post(mnWizardService.getUserCreds());
      });

      mnAuthService.stream.postUILogin.success.pipe(
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(function () {
        uiRouter.urlRouter.sync();
      });

      this.onSubmit.pipe(
        Rx.operators.tap(this.groupHttp.clearErrors.bind(this.groupHttp)),
        Rx.operators.filter(isNotLoading.bind(this)),
        Rx.operators.withLatestFrom(mnPoolsService.stream.isEnterprise),
        Rx.operators.map(getWizardValues.bind(this)),
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(this.groupHttp.post.bind(this.groupHttp));

      function isNotLoading() {
        return !this.mnAppLoding.getValue();
      }

      function getWizardValues(isEnterprise) {
        return {
          postPoolsDefault: [
            getPoolsDefaultValues.bind(this)(isEnterprise[1]),
            false
          ],
          servicesHttp: {
            services: this.getServicesValues(
              this.wizardForm.newClusterConfig.get("services.flag")
            ).join(","),
          },
          diskStorageHttp: this.wizardForm.newClusterConfig.get("clusterStorage.storage").value,
          hostnameHttp: this.wizardForm.newClusterConfig.get("clusterStorage.hostname").value,
          statsHttp: this.wizardForm.newClusterConfig.get("enableStats").value
        };
      }

      function getPoolsDefaultValues(isEnterprise) {
        var services = [
          ["memoryQuota", "kv"],
          ["indexMemoryQuota", "index"],
          ["ftsMemoryQuota", "fts"]
        ];
        if (isEnterprise) {
          services.push(["eventingMemoryQuota", "eventing"]);
          services.push(["cbasMemoryQuota", "cbas"]);
        }
        return _.reduce(services, getPoolsDefaultValue.bind(this), {
          clusterName: this.wizardForm.newCluster.get("clusterName").value
        });
      }

      function getPoolsDefaultValue(result, names) {
        var service = this.wizardForm.newClusterConfig.get("services.flag." + names[1]);
        if (service && service.value) {
          result[names[0]] =
            this.wizardForm.newClusterConfig.get("services.field." + names[1]).value;
        }
        return result;
      }
    }
  })(window.rxjs);
