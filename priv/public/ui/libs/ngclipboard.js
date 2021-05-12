/*! ngclipboard - v1.1.1 - 2016-02-26
 * https://github.com/sachinchoolur/ngclipboard
 * Copyright (c) 2016 Sachin; Licensed MIT */
import angular from "/ui/web_modules/angular.js";
import Clipboard from "/ui/web_modules/clipboard.js";
var MODULE_NAME = 'ngclipboard';
export default MODULE_NAME;

angular.module(MODULE_NAME, []).directive('ngclipboard', function() {
  return {
    restrict: 'A',
    scope: {
      ngclipboardSuccess: '&',
      ngclipboardError: '&'
    },
    link: function(scope, element) {
      var clipboard = new Clipboard(element[0]);

      clipboard.on('success', function(e) {
        scope.$apply(function () {
          scope.ngclipboardSuccess({
            e: e
          });
        });
      });

      clipboard.on('error', function(e) {
        scope.$apply(function () {
          scope.ngclipboardError({
            e: e
          });
        });
      });

    }
  };
});
