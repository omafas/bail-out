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
mn.components.MnTermsAndConditions =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnTermsAndConditions, mn.core.MnEventableComponent);

    MnTermsAndConditions.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-terms-and-conditions.html"
      })
    ];

    MnTermsAndConditions.parameters = [
      mn.services.MnWizard,
      mn.services.MnPools,
      mn.services.MnApp,
      mn.services.MnAuth,
      window['@uirouter/angular'].UIRouter
    ];

    return MnTermsAndConditions;

    function MnTermsAndConditions(mnWizardService, mnPoolsService, mnAppService, mnAuthService, uiRouter) {
      mn.core.MnEventableComponent.call(this);

      this.focusFieldSubject = new Rx.BehaviorSubject(true);
      this.onSubmit = new Rx.Subject();
      this.onFinishWithDefaut = new Rx.Subject();

      this.submitted =
        Rx.merge(
          this.onSubmit,
          this.onFinishWithDefaut
        )
        .pipe(Rx.operators.mapTo(true));

      this.uiRouter = uiRouter;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.wizardForm = mnWizardService.wizardForm;
      this.initialValues = mnWizardService.initialValues;

      this.termsForm = mnWizardService.wizardForm.termsAndConditions;
      this.termsForm.get("agree").setValue(false);

      this.groupHttp = mnWizardService.stream.groupHttp;
      this.secondGroupHttp = mnWizardService.stream.secondGroupHttp;
      this.servicesHttp = mnWizardService.stream.servicesHttp;

      this.mnAppLoding = mnAppService.stream.loading;

      this.license = mnPoolsService.stream.isEnterprise.pipe(Rx.operators.switchMap(getLicense));
      this.termsHref = mnPoolsService.stream.isEnterprise.pipe(Rx.operators.map(getTermsAndCond));


      Rx
        .merge(
          this.groupHttp.loading,
          this.secondGroupHttp.loading
        )
        .pipe(
          Rx.operators.takeUntil(this.mnOnDestroy)
        )
        .subscribe(this.mnAppLoding.next.bind(this.mnAppLoding));

      this.groupHttp.success
        .pipe(
          Rx.operators.map(getSecondValues.bind(this)),
          Rx.operators.takeUntil(this.mnOnDestroy)
        )
        .subscribe(this.secondGroupHttp.post.bind(this.secondGroupHttp));

      this.secondGroupHttp.success
        .pipe(
          Rx.operators.takeUntil(this.mnOnDestroy)
        )
        .subscribe(function () {
          mnAuthService.stream.postUILogin.post(mnWizardService.getUserCreds());
        });

      mnAuthService.stream.postUILogin.success
        .pipe(
          Rx.operators.takeUntil(this.mnOnDestroy)
        )
        .subscribe(function () {
          uiRouter.urlRouter.sync();
        });

      this.onSubmit
        .pipe(
          Rx.operators.filter(isValid.bind(this)),
          Rx.operators.takeUntil(this.mnOnDestroy)
        )
        .subscribe(onSuccess);

      this.onFinishWithDefaut
        .pipe(
          Rx.operators.tap(this.groupHttp.clearErrors.bind(this.groupHttp)),
          Rx.operators.filter(isValid.bind(this)),
          Rx.operators.filter(isNotLoading.bind(this)),
          Rx.operators.withLatestFrom(mnPoolsService.stream.isEnterprise),
          Rx.operators.map(getValues.bind(this)),
          Rx.operators.takeUntil(this.mnOnDestroy)
        )
        .subscribe(this.groupHttp.post.bind(this.groupHttp));

      function onSuccess() {
        uiRouter.stateService.go('app.wizard.newClusterConfig', null, {location: false});
      }

      function isValid() {
        return !this.termsForm.invalid;
      }

      function isNotLoading() {
        return !this.mnAppLoding.getValue();
      }

      function getLicense(isEnterprise) {
        return isEnterprise ? mnWizardService.getEELicense() : mnWizardService.getCELicense();
      }

      function getTermsAndCond(isEnterprise) {
        return isEnterprise ?
          'https://www.couchbase.com/ESLA05242016' :
          'https://www.couchbase.com/community';
      }

      function getSecondValues() {
        return {
          indexesHttp: {
            storageMode: mnWizardService.initialValues.storageMode
          },
          authHttp: [mnWizardService.wizardForm.newCluster.value.user, false]
        };
      }

      function getValues(isEnterprise) {
        return {
          postPoolsDefault: [{
            clusterName: this.wizardForm.newCluster.get("clusterName").value
          }, false],
          servicesHttp: {
            services: 'kv,index,fts,n1ql' + (isEnterprise[1] ? ',eventing' : ''),
            setDefaultMemQuotas : true
          },
          diskStorageHttp: this.initialValues.clusterStorage,
          hostnameHttp: this.initialValues.hostname,
          statsHttp: true
        };
      }

    }
  })(window.rxjs);
