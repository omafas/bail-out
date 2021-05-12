/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";
import { upgradeModule } from '/ui/web_modules/@uirouter/angular-hybrid.js';
import oclazyLoad from "/ui/web_modules/oclazyload.js";
import ngSanitize from "/ui/web_modules/angular-sanitize.js";
import ngAnimate from "/ui/web_modules/angular-animate.js";
import uiBootstrap from "/ui/web_modules/angular-ui-bootstrap.js";
import mnAdmin from "/ui/app/mn_admin/mn_admin_config.js";
import mnAppConfig from "/ui/app/app_config.js";
import mnPools from "/ui/app/components/mn_pools.js";
import mnEnv from "/ui/app/components/mn_env.js";
import mnFilters from "/ui/app/components/mn_filters.js";
import mnHttp from "/ui/app/components/mn_http.js";
import mnExceptionReporter from "/ui/app/components/mn_exception_reporter.js";
import {
  bucketsFormConfiguration,
  daysOfWeek,
  knownAlerts,
  timeUnitToSeconds,
  docsLimit,
  docBytesLimit,
  viewsPerPageLimit,
  IEC
} from "/ui/app/constants/constants.js";

export default 'app';

angular.module('app', [
  upgradeModule.name,
  oclazyLoad,
  mnPools,
  mnEnv,
  mnHttp,
  mnFilters,
  mnExceptionReporter,
  ngAnimate,
  ngSanitize,
  uiRouter,
  uiBootstrap,
  mnAdmin
]).config(mnAppConfig)
  .constant("bucketsFormConfiguration", bucketsFormConfiguration)
  .constant("daysOfWeek", daysOfWeek)
  .constant("knownAlerts", knownAlerts)
  .constant("timeUnitToSeconds", timeUnitToSeconds)
  .constant("docsLimit", docsLimit)
  .constant("docBytesLimit", docBytesLimit)
  .constant("viewsPerPageLimit", viewsPerPageLimit)
  .constant("IEC", IEC)
  .run(appRun);

function appRun($state, $urlRouter, $exceptionHandler, mnPools, $window, $rootScope, $location, $http, mnPrettyVersionFilter) {

  angular.element($window).on("storage", function (storage) {
    if (storage.key === "mnLogIn") {
      mnPools.clearCache();
      $urlRouter.sync();
    }
  });

  var originalOnerror = $window.onerror;
  $window.onerror = onError;
  function onError(message, url, lineNumber, columnNumber, exception) {
    $exceptionHandler({
      message: message,
      fileName: url,
      lineNumber: lineNumber,
      columnNumber: columnNumber,
      stack: exception && exception.stack
    });
    originalOnerror && originalOnerror.apply($window, Array.prototype.slice.call(arguments));
  }

  $http({method: "GET", url: "/versions"}).then(function (resp) {
    var pools = resp.data;
    var version = mnPrettyVersionFilter(pools.implementationVersion);
    $rootScope.mnTitle = "Couchbase Server";
  });

  $state.defaultErrorHandler(function (error) {
    error && $exceptionHandler(error);
  });
}
