/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default "mnFileReader";

angular
  .module('mnFileReader', [])
  .directive("mnFileReader", function () {
    var index = 0;
    return {
      template: `
        <textarea
          rows="4"
          autocorrect="off"
          autocompleterg="off"
          spellcheck="false"
          ng-change="onTextareaChange()"
          ng-disabled="disable"
          ng-class="classContainer"
          ng-model="result"></textarea>
        <label
          class="btn ellipsis outline left-ellipsis margin-top-half"
          ng-class="classContainer"
          ng-attr-for="select-file-{{index}}"
          ng-disabled="disable">{{name}}</label>
        <input
          ng-attr-id="select-file-{{index}}"
          ng-show="false"
          ng-disabled="disable"
          type="file">`,
      scope: {
        classes: "=",
        result: "=",
        disable: "="
      },
      link: function (scope, element, attributes) {
        var defaultName = "Select File";
        var inputFile = element.find("input");

        scope.classContainer = [...(scope.classes || [])];
        scope.index = index++;
        scope.name = defaultName;
        scope.onTextareaChange = onTextareaChange;

        function onTextareaChange(a) {
          scope.name = defaultName;
          inputFile[0].value = "";
        }

        function setNameAndRead(changeEvent, reader) {
          return function () {
            var file = changeEvent.target.files[0];
            if (file) {
              if (file.size > (1024 * 1024)) {
                return;
              }
              scope.name = file.name;
              reader.readAsText(file);
            } else {
              scope.name = defaultName;
              scope.result = "";
            }
          }
        }

        function setResult(loadEvent) {
          return function () {
            scope.result = loadEvent.target.result.toString().slice();
          }
        }

        function loadFile(changeEvent) {
          var reader = new FileReader();
          reader.onload = function (loadEvent) {
            scope.$apply(setResult(loadEvent));
          };
          scope.$apply(setNameAndRead(changeEvent, reader));
        }

        inputFile.bind("change", loadFile);

        scope.$on('$destory', function () {
          inputFile.unbind("change", loadFile);
        });
      }
    };
  });
