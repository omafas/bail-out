/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default 'mnEnv';

/**
 * Service supporting access to UI applicable environment variables.
 */
angular
  .module('mnEnv', [])
  .factory('mnEnv', mnEnvFactory);

function mnEnvFactory($http) {

  var envUrl = '/_uiEnv';
  var envDefaults = {
    disable_autocomplete: true
  };
  return {
    loadEnv: loadEnv
  };

  /**
   * Invokes the server side REST API and returns a promise that fulfills
   * with a JSON object that fulfills with the complete set of environment
   * variables.
   * @returns Promise
   */
  function loadEnv() {
    return $http({method: 'GET', url: envUrl, cache: true}).then(
      function (resp) {
        return angular.extend({}, envDefaults, resp.data);
      });
  }
}
