/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default "mnLaunchpad";

angular
  .module('mnLaunchpad', [])
  .directive('mnLaunchpad', mnLaunchpadDirective);

function mnLaunchpadDirective($timeout) {
  var mnLaunchpad = {
    scope: {
      launchpadSource: "=",
      launchpadId: "="
    },
    link: link
  }

  return mnLaunchpad;

  function link($scope, $element, $attrs) {
    $scope.$watch('launchpadSource', function (launchpadSource) {
      if (!launchpadSource) {
        return;
      }
      var iframe = document.createElement("iframe");
      iframe.style.display = 'none'
      $element.append(iframe);
      var idoc = iframe.contentWindow.document;
      idoc.body.innerHTML = "<form id=\"launchpad\" method=\"POST\"><textarea id=\"sputnik\" name=\"stats\"></textarea></form>";
      var form = idoc.getElementById("launchpad");
      var textarea = idoc.getElementById("sputnik");
      form['action'] = "https://ph.couchbase.net/v2?launchID=" + $scope.launchpadId;
      textarea.innerText = JSON.stringify(launchpadSource);
      form.submit();
      $scope.launchpadSource = undefined;

      $timeout(function () {
        $element.empty();
      }, 30000);
    });
  }
}
