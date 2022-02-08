/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import uiRouter from '@uirouter/angularjs';
import {upgradeModule} from '@uirouter/angular-hybrid';
import oclazyLoad from 'oclazyload';
import ngSanitize from 'angular-sanitize';
import ngAnimate from 'angular-animate';
import uiBootstrap from 'angular-ui-bootstrap';

import mnAdmin from './mn_admin/mn_admin_config.js';
import mnAppConfig from './app_config.js';
import mnPools from './components/mn_pools.js';
import mnEnv from './components/mn_env.js';
import mnFilters from './components/mn_filters.js';
import mnHttp from './components/mn_http.js';
import mnExceptionReporter from './components/mn_exception_reporter.js';
import {
  daysOfWeek,
  knownAlerts,
  timeUnitToSeconds,
  docsLimit,
  docBytesLimit,
  viewsPerPageLimit,
  IEC
} from './constants/constants.js';

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
  .constant("daysOfWeek", daysOfWeek)
  .constant("knownAlerts", knownAlerts)
  .constant("timeUnitToSeconds", timeUnitToSeconds)
  .constant("docsLimit", docsLimit)
  .constant("docBytesLimit", docBytesLimit)
  .constant("viewsPerPageLimit", viewsPerPageLimit)
  .constant("IEC", IEC)
  .run(["$state", "$urlRouter", "$exceptionHandler", "mnPools", "$window", "$rootScope", appRun]);

function appRun($state, $urlRouter, $exceptionHandler, mnPools, $window, $rootScope) {

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


  $rootScope.mnTitle = "Couchbase Server";

  $state.defaultErrorHandler(function (error) {
    error && $exceptionHandler(error);
  });
}
