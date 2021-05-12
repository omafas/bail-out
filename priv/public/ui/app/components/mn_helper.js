/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import mnPendingQueryKeeper from "/ui/app/components/mn_pending_query_keeper.js";
import _ from "/ui/web_modules/lodash.js";

export default 'mnHelper';

angular
  .module('mnHelper', [mnPendingQueryKeeper])
  .factory('mnHelper', mnHelperFactory);

function mnHelperFactory($window, $state, $location, $timeout, $q, $http, mnPendingQueryKeeper) {
  var mnHelper = {
    wrapInFunction: wrapInFunction,
    calculateMaxMemorySize: calculateMaxMemorySize,
    initializeDetailsHashObserver: initializeDetailsHashObserver,
    checkboxesToList: checkboxesToList,
    reloadApp: reloadApp,
    reloadState: reloadState,
    listToCheckboxes: listToCheckboxes,
    getEndings: getEndings,
    generateID: generateID,
    counterSpinner: counterSpinner,
    mainSpinnerCounter: counterSpinner()
  };

  return mnHelper;

  function getEndings(length) {
    return length !== 1 ? "s" : "";
  }

  function counterSpinner() {
    let counter = 0;
    return {
      increase: () => (counter++),
      decrease: () => (counter > 0 && counter--),
      value: () => !!counter
    }
  }

  function wrapInFunction(value) {
    return function () {
      return value;
    };
  }
  function calculateMaxMemorySize(totalRAMMegs) {
    return Math.floor(Math.max(totalRAMMegs * 0.8, totalRAMMegs - 1024));
  }
  function initializeDetailsHashObserver($scope, hashKey, stateName) {
    function getHashValue() {
      return _.clone($state.params[hashKey]) || [];
    }
    $scope.isDetailsOpened = function (hashValue) {
      return _.contains(getHashValue(), String(hashValue));
    };
    $scope.toggleDetails = function (hashValue) {
      var currentlyOpened = getHashValue();
      var stateParams = {};
      if ($scope.isDetailsOpened(hashValue)) {
        stateParams[hashKey] = _.difference(currentlyOpened, [String(hashValue)]);
        $state.go(stateName, stateParams);
      } else {
        currentlyOpened.push(String(hashValue));
        stateParams[hashKey] = currentlyOpened;
        $state.go(stateName, stateParams);
      }
    };
  }
  function checkboxesToList(object) {
    return _.chain(object).pick(angular.identity).keys().value();
  }
  function listToCheckboxes(list) {
    return _.zipObject(list, _.fill(new Array(list.length), true, 0, list.length));
  }
  function reloadApp() {
    $window.location.reload();
  }
  function generateID() {
    return Math.random().toString(36).substr(2, 9);
  }
  function reloadState(state) {
    if (!state) {
      mnPendingQueryKeeper.cancelAllQueries();
    }
    return $state.reload(state);
  }
}
