/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default "mnDragAndDrop";

angular
  .module('mnDragAndDrop', [])
  .directive('mnDragAndDrop', mnDragAndDropDirective);

function mnDragAndDropDirective($window, $document) {
  var mnDragAndDrop = {
    scope: {
      onItemTaken: '&',
      onItemDropped: '&',
      onItemMoved: '&'
    },
    link: link
  };

  return mnDragAndDrop;

  function link($scope, $element, $attrs) {
    var draggedObject;
    var startX;
    var startY;
    var initialMouseX;
    var initialMouseY;
    var baseCornerRight = $attrs.baseCornerRight;

    $element.on('mousedown touchstart', onMouseDown);
    $scope.$on("$destroy", function () {
      $element.off('mousedown touchstart', onMouseDown);
    });

    function onMouseDown(e) {
      e = e || $window.event;

      if (draggedObject) {
        onMouseUp();
        return;
      }
      var target = e.currentTarget;
      draggedObject = $element;

      if ($scope.onItemTaken) {
        $scope.onItemTaken({$event: e});
      }
      startX = target.offsetLeft;
      if (baseCornerRight) {
        startX += target.clientWidth;
      }
      startY = target.offsetTop;
      initialMouseX = e.clientX;
      initialMouseY = e.clientY;

      $element.addClass("dragged");
      $document.on('mousemove touchmove', onMouseMove);
      $document.on('mouseup touchend', onMouseUp);
      $document.find('body').addClass('disable-text-selection');
      return false;
    }

    function onMouseMove(e) {
      e = e || $window.event;
      if ($scope.onItemMoved) {
        $scope.onItemMoved(this);
      }
      var dx = e.clientX - initialMouseX;
      var dy = e.clientY - initialMouseY;
      var move = {
        top: startY + dy + 'px',
        bottom: 'auto'
      };
      if (baseCornerRight) {
        move.right = -(startX + dx) + 'px';
        move.left = "auto";
      } else {
        move.right = "auto";
        move.left = startX + dx + 'px';
      }
      $element.css(move);
      return false;
    }
    function onMouseUp() {
      if ($scope.onItemDropped) {
        $scope.onItemDropped(this);
      }
      draggedObject.removeClass("dragged");
      $document.off('mousemove touchmove', onMouseMove);
      $document.off('mouseup touchend', onMouseUp);
      $document.find('body').removeClass('disable-text-selection');
      draggedObject = null;
    }

  }
}
