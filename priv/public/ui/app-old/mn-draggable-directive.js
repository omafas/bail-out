/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.directives = mn.directives || {};
mn.directives.MnDraggable =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnFocusDirective, mn.core.MnEventableComponent);

    MnFocusDirective.annotations = [
      new ng.core.Directive({
        selector: "[mnDraggable]",
        inputs: [
          "baseCornerRight"
        ],
        host: {
          '[style.top]': 'top',
          '[style.right]': 'right',
          '[style.left]': 'left',
          '[style.bottom]': 'bottom',
          '(mousedown)': 'mousedown($event)',
          '(document:mousemove)': 'mousemove($event)',
          '(document:mouseup)': 'mouseup($event)'
        },
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnFocusDirective.prototype.ngOnInit = ngOnInit;
    MnFocusDirective.prototype.mousedown = mousedown;
    MnFocusDirective.prototype.mouseup = mouseup;
    MnFocusDirective.prototype.mousemove = mousemove;
    MnFocusDirective.prototype.getNewCoordinates = getNewCoordinates;
    MnFocusDirective.prototype.setNewCoordinates = setNewCoordinates;
    MnFocusDirective.prototype.getInitCoordinates = getInitCoordinates;

    return MnFocusDirective;

    function ngOnInit() {
      this.stream.mousedown.pipe(
        Rx.operators.map(this.getInitCoordinates.bind(this)),
        Rx.operators.switchMap((function (init) {
          return this.stream.mousemove.pipe(
            Rx.operators.takeUntil(this.stream.mouseup),
            Rx.operators.map(this.getNewCoordinates(init).bind(this))
          );
        }).bind(this)),
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(this.setNewCoordinates.bind(this));
    }

    function MnFocusDirective() {
      mn.core.MnEventableComponent.call(this);
      this.stream = {};
      this.stream.mouseup = new Rx.Subject();
      this.stream.mousemove = new Rx.Subject();
      this.stream.mousedown = new Rx.Subject();
    }

    function setNewCoordinates(css) {
      this.top = css.top;
      this.bottom = css.bottom;
      this.left = css.left;
      this.right = css.right;
    }

    function getNewCoordinates(init) {
      return function (e) {
        var dx = e.clientX - init.mouseX;
        var dy = e.clientY - init.mouseY;
        var rv = {
          top: init.startY + dy + 'px',
          bottom: 'auto'
        };
        if (this.baseCornerRight) {
          rv.right = -(init.startX + dx) + 'px';
          rv.left = "auto";
        } else {
          rv.right = "auto";
          rv.left = init.startX + dx + 'px';
        }
        return rv;
      }
    }

    function getInitCoordinates(e) {
      var target = e.currentTarget;
      var startX = target.offsetLeft;

      if (this.baseCornerRight) {
        startX += target.clientWidth;
      }
      return {
        startX: startX,
        startY: target.offsetTop,
        mouseX: e.clientX,
        mouseY: e.clientY
      };
    }

    function mousedown(e) {
      this.stream.mousedown.next(e);
      return false;
    }

    function mouseup(e) {
      this.stream.mouseup.next(e);
    }

    function mousemove(e) {
      this.stream.mousemove.next(e);
      return false;
    }
  })(window.rxjs);
