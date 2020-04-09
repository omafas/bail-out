var mn = mn || {};
mn.directives = mn.directives || {};
mn.directives.MnFocus =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnFocusDirective, mn.core.MnEventableComponent);

    MnFocusDirective.annotations = [
      new ng.core.Directive({
        selector: "[mnFocus]",
        inputs: [
          "mnFocus"
        ],
        host: {
          '(blur)': 'blur()'
        },
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnFocusDirective.parameters = [
      ng.core.ElementRef
    ];

    MnFocusDirective.prototype.blur = blur;
    MnFocusDirective.prototype.ngOnInit = ngOnInit;
    MnFocusDirective.prototype.maybePrevent = maybePrevent;
    MnFocusDirective.prototype.doFocus = doFocus;

    return MnFocusDirective;

    function MnFocusDirective(el) {
      mn.core.MnEventableComponent.call(this);
      this.el = el.nativeElement;
      this.formControlName = this.el.getAttribute("formControlName");
    }

    function ngOnInit() {
      this.mnFocus = this.mnFocus || new Rx.BehaviorSubject(true);
      this.mnFocus.pipe(
        Rx.operators.filter(this.maybePrevent.bind(this)),
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(this.doFocus.bind(this));
    }

    function doFocus(value) {
      this.el.focus();
    }

    function maybePrevent(value) {
      if (typeof value === "string") {
        return value === this.formControlName;
      } else {
        return value; //Boolean
      }
    }

    function blur() {
      // this.mnFocus.next(false);
    }
  })(window.rxjs);
