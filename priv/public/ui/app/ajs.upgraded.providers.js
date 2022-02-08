/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

class $rootScope {}
const $rootScopeProvider = {
  provide: $rootScope,
  useFactory: function $rootScopeFactory(i) {
    return i.get('$rootScope');
  },
  deps: ['$injector']
};

class $state {}
const $stateProvider = {
  provide: $state,
  useFactory: function $stateFactory(i) {
    return i.get('$state');
  },
  deps: ['$injector']
};

class MnPendingQueryKeeper {}
const MnPendingQueryKeeperProvider = {
  provide: MnPendingQueryKeeper,
  useFactory: function MnPendingQueryKeeperFactory(i) {
    return i.get('mnPendingQueryKeeper');
  },
  deps: ['$injector']
};

class MnPools {}
const MnPoolsProvider = {
  provide: MnPools,
  useFactory: function MnPoolsFactory(i) {
    return i.get('mnPools');
  },
  deps: ['$injector']
};

class MnHelper {}
const MnHelperProvider = {
  provide: MnHelper,
  useFactory: function MnHelperFactory(i) {
    return i.get('mnHelper');
  },
  deps: ['$injector']
};

class MnPoolDefault {}
const MnPoolDefaultProvider = {
  provide: MnPoolDefault,
  useFactory: function MnPoolDefaultFactory(i) {
    return i.get('mnPoolDefault');
  },
  deps: ['$injector']
};

class MnPermissions {}
const MnPermissionsProvider = {
  provide: MnPermissions,
  useFactory: function MnPermissionsFactory(i) {
    return i.get('mnPermissions');
  },
  deps: ['$injector']
};

class MnAuth {}
const MnAuthProvider = {
  provide: MnAuth,
  useFactory: function MnAuthFactory(i) {
    return i.get('mnAuthService');
  },
  deps: ['$injector']
};

class MnAlerts {}
const MnAlertsProvider = {
  provide: MnAlerts,
  useFactory: function MnAlertsFactory(i) {
    return i.get('mnAlertsService');
  },
  deps: ['$injector']
};

class MnServers {}
const MnServersProvider = {
  provide: MnServers,
  useFactory: function MnServersFactory(i) {
    return i.get('mnServersService');
  },
  deps: ['$injector']
};

class MnStatisticsNew {}
const MnStatisticsNewProvider = {
  provide: MnStatisticsNew,
  useFactory: function MnStatisticsNewFactory(i) {
    return i.get('mnStatisticsNewService');
  },
  deps: ['$injector']
};

let ajsUpgradedProviders = [
  $rootScopeProvider,
  $stateProvider,
  MnPoolsProvider,
  MnHelperProvider,
  MnPendingQueryKeeperProvider,
  MnPermissionsProvider,
  MnAuthProvider,
  MnAlertsProvider,
  MnServersProvider,
  MnStatisticsNewProvider,
  MnPoolDefaultProvider
];

export {
  ajsUpgradedProviders,
  $state,
  $rootScope,
  MnPools,
  MnHelper,
  MnPendingQueryKeeper,
  MnPermissions,
  MnAuth,
  MnAlerts,
  MnServers,
  MnStatisticsNew,
  MnPoolDefault
};
