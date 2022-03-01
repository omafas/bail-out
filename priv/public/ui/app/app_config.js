/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

export default appConfig;

appConfig.$inject = ["$httpProvider", "$stateProvider", "$urlRouterProvider", "$transitionsProvider", "$uibTooltipProvider", "$animateProvider", "$qProvider", "$sceDelegateProvider", "$locationProvider", "$uibModalProvider"];
function appConfig($httpProvider, $stateProvider, $urlRouterProvider, $transitionsProvider, $uibTooltipProvider, $animateProvider, $qProvider, $sceDelegateProvider, $locationProvider, $uibModalProvider) {
  $httpProvider.defaults.headers.common['invalid-auth-response'] = 'on';
  $httpProvider.defaults.headers.common['Cache-Control'] = 'no-cache';
  $httpProvider.defaults.headers.common['Pragma'] = 'no-cache';
  $httpProvider.defaults.headers.common['ns-server-ui'] = 'yes';

  $animateProvider.classNameFilter(/enable-ng-animation/);

  $urlRouterProvider.deferIntercept();
  $locationProvider.hashPrefix('');
  $urlRouterProvider.otherwise(function ($injector) {
    $injector.invoke(['$state', function($state) {
      $state.go('app.admin.overview.statistics');
    }]);
  });

  $uibModalProvider.options.backdrop = "static";

  $sceDelegateProvider.resourceUrlWhitelist([
    'self', // Allow same origin resource loads
    'https://ph.couchbase.net/**' // Allow JSONP calls that match this pattern
  ]);

  $qProvider.errorOnUnhandledRejections(false);
  // When using a tooltip in an absolute positioned element,
  // you need tooltip-append-to-body="true" https://github.com/angular-ui/bootstrap/issues/4195
  $uibTooltipProvider.options({
    placement: "auto right",
    trigger: "outsideClick"
  });

  $stateProvider.state('app', {
    url: '?{enableInternalSettings:bool}&{disablePoorMansAlerts:bool}&{enableDeveloperSettings:bool}',
    params: {
      enableDeveloperSettings: {
        value: null,
        squash: true
      },
      enableInternalSettings: {
        value: null,
        squash: true
      },
      disablePoorMansAlerts: {
        value: null,
        squash: true
      }
    },
    abstract: true,
    resolve: {
      env: ['mnEnv', '$rootScope', function (mnEnv, $rootScope) {
        return mnEnv.loadEnv().then(function(env) {
          $rootScope.ENV = env;
        });
      }]
    },
    template: '<div ui-view="" class="root-container"></div>' +
      '<div ng-show="mnGlobalSpinnerFlag" class="global-spinner"></div>'
  });

  $transitionsProvider.onBefore({
    to: "app.admin.**"
  }, (trans) => {
    //convert pre 7.0 bucket params to 7.0 commonBucket
    let original = Object.assign({}, trans.params('to'));

    if (!original.commonBucket) {
      let params = Object.assign({}, original);
      (["bucket", "scenarioBucket", "collectionsBucket", "indexesBucket"])
        .forEach(bucket => {
          if (params[bucket]) {
            params.commonBucket = params[bucket];
            if (bucket != "bucket") {
              //do not remove 'bucket' parameter since this is
              //popular pluggable UI param, so we just pass it
              //along with commonBucket parameter
              delete params[bucket];
            }
          }
        });

      if (params.commonBucket) {
        return trans.router.stateService.target(trans.to().name, params);
      }
    }
  });

  $transitionsProvider.onBefore({
    to: (state) => state.data && state.data.requiresAuth
  }, (transition) => {
    let mnPools = transition.injector().get('mnPools');
    let $state = transition.router.stateService;
    return mnPools.get().then(pools => {
      if (!pools.isInitialized) {
        $state.go('app.wizard.welcome', null, {location: false});
        return false;
      } else {
        return true;
      }
    }, function (resp) {
      switch (resp.status) {
      case 401:
        $state.go('app.auth', null, {location: false});
        return false;
      }
    });
  });

  function isThisTransitionBetweenTabs(trans) {
    let toName = trans.to().name;
    let fromName = trans.from().name;
    return toName.indexOf(fromName) === -1 && fromName.indexOf(toName) === -1;
  }

  $transitionsProvider.onFinish({
    from: "app.admin.**",
    to: "app.admin.**"
  }, function (trans) {
    if (isThisTransitionBetweenTabs(trans)) {
      let mnHelper = trans.injector().get('mnHelper');
      mnHelper.mainSpinnerCounter.decrease();
    }
  });

  $transitionsProvider.onError({
    from: "app.admin.**",
    to: "app.admin.**"
  }, function (trans) {
    if (isThisTransitionBetweenTabs(trans)) {
      let mnHelper = trans.injector().get('mnHelper');
      mnHelper.mainSpinnerCounter.decrease();
    }
  });

  $transitionsProvider.onBefore({
    from: "app.admin.**",
    to: "app.admin.**"
  }, function (trans) {
    var $rootScope = trans.injector().get('$rootScope');
    var mnPendingQueryKeeper = trans.injector().get('mnPendingQueryKeeper');
    var $uibModalStack = trans.injector().get('$uibModalStack');
    var isModalOpen = !!$uibModalStack.getTop();

    if ($rootScope.mnGlobalSpinnerFlag) {
      return false;
    }
    if (!isModalOpen && isThisTransitionBetweenTabs(trans)) {
      //cancel tabs specific queries in case toName is not child of fromName and vise versa
      mnPendingQueryKeeper.cancelTabsSpecificQueries();
      var mnHelper = trans.injector().get('mnHelper');
      mnHelper.mainSpinnerCounter.increase();
    }
    return !isModalOpen;
  });
  $transitionsProvider.onBefore({
    from: "app.auth",
    to: "app.admin.**"
  }, function (trans, $state) {
    var mnPools = trans.injector().get('mnPools');
    return mnPools.get().then(function (pools) {
      return pools.isInitialized ? true : $state.target("app.wizard.welcome");
    }, function (resp) {
      switch (resp.status) {
      case 401: return false;
      }
    });
  });
  $transitionsProvider.onBefore({
    from: "app.wizard.**",
    to: "app.admin.**"
  }, function (trans) {
    var mnPools = trans.injector().get('mnPools');
    return mnPools.getFresh().then(function (pools) {
      return pools.isInitialized;
    });
  });

  $transitionsProvider.onStart({
    to: function (state) {
      return state.data && state.data.permissions;
    }
  }, function (trans) {
    var mnPermissions = trans.injector().get('mnPermissions');
    var $parse = trans.injector().get('$parse');
    return mnPermissions.check().then(function() {
      if ($parse(trans.to().data.permissions)(mnPermissions.export)) {
        return true;
      } else {
        return trans.router.stateService.target('app.admin.overview.statistics');
      }
    });
  });
  $transitionsProvider.onStart({
    to: function (state) {
      return state.data && state.data.compat;
    }
  }, function (trans) {
    var mnPoolDefault = trans.injector().get('mnPoolDefault');
    var $parse = trans.injector().get('$parse');
    return mnPoolDefault.get().then(function() {
      if ($parse(trans.to().data.compat)(mnPoolDefault.export.compat)) {
        return true;
      } else {
        return trans.router.stateService.target('app.admin.overview.statistics');
      }
    });
  });
  $transitionsProvider.onStart({
    to: function (state) {
      return state.data && state.data.enterprise;
    }
  }, function (trans) {
    var mnPools = trans.injector().get('mnPools');
    return mnPools.get().then(function (pools) {
      if (pools.isEnterprise) {
        return true;
      } else {
        return trans.router.stateService.target('app.admin.overview.statistics');
      }
    });
  });
}
