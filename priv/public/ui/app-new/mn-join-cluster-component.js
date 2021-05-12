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
mn.components.MnJoinCluster =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnJoinCluster, mn.core.MnEventableComponent);

    MnJoinCluster.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-join-cluster.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnJoinCluster.parameters = [
      mn.services.MnWizard,
      mn.services.MnApp,
      mn.services.MnAuth,
      window['@uirouter/angular'].UIRouter
    ];

    return MnJoinCluster;

    function MnJoinCluster(mnWizardService, mnAppService, mnAuthService, uiRouter) {
      mn.core.MnEventableComponent.call(this);

      this.focusFieldSubject = new Rx.BehaviorSubject("hostname");
      this.onSubmit = new Rx.Subject();
      this.submitted = this.onSubmit.pipe(Rx.operators.mapTo(true));

      this.wizardForm = mnWizardService.wizardForm;
      this.mnAppLoding = mnAppService.stream.loading;

      this.joinClusterForm = mnWizardService.wizardForm.joinCluster;

      this.hostnameHttp = mnWizardService.stream.hostnameHttp;
      this.diskStorageHttp = mnWizardService.stream.diskStorageHttp;
      this.joinClusterHttp = mnWizardService.stream.joinClusterHttp;

      this.groupHttp =
        new mn.core.MnPostGroupHttp({
          hostnameHttp: this.hostnameHttp,
          diskStorageHttp: this.diskStorageHttp
        })
        .addLoading()
        .addSuccess();

      Rx
        .merge(this.groupHttp.loading, this.joinClusterHttp.loading)
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.mnAppLoding.next.bind(this.mnAppLoding));

      this.groupHttp.success
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe((function () {
          var data = mnWizardService.wizardForm.joinCluster.get("clusterAdmin").value;
          data.services =
            mnWizardService.getServicesValues(
              mnWizardService.wizardForm.joinCluster.get("services.flag")).join(",");

          mnWizardService.stream.joinClusterHttp.post(data);
        }).bind(this));

      this.joinClusterHttp.success
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function () {
          var data = mnWizardService.wizardForm.joinCluster.get("clusterAdmin").value;
          mnAuthService.stream.postUILogin.post(data);
        });

      mnAuthService.stream.postUILogin.success
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function () {
          uiRouter.urlRouter.sync();
        });

      this.onSubmit
        .pipe(Rx.operators.tap(this.groupHttp.clearErrors.bind(this.groupHttp)),
              Rx.operators.filter(isValid.bind(this)),
              Rx.operators.filter(isNotLoading.bind(this)),
              Rx.operators.map(getValues.bind(this)),
              Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.groupHttp.post.bind(this.groupHttp));

      function isNotLoading() {
        return !this.mnAppLoding.getValue();
      }

      function isValid() {
        return !this.joinClusterForm.invalid;
      }

      function getValues() {
        return {
          hostnameHttp: this.wizardForm.joinCluster.get("clusterStorage.hostname").value,
          diskStorageHttp: this.wizardForm.joinCluster.get("clusterStorage.storage").value
        };
      }
    }
  })(window.rxjs);
