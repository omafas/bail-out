var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnSearch =
  (function (Rx) {
    "use strict";

    MnSearchComponent.annotations = [
      new ng.core.Component({
        selector: "mn-search",
        templateUrl: 'app-new/mn-search.html',
        inputs: [
          "mnFormGroup",
          "mnPlaceholder"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    return MnSearchComponent;

    function MnSearchComponent() {
      this.onShowClick = new Rx.Subject();
      this.onHideClick = new Rx.Subject();

      var showToTrue = this.onShowClick.pipe(Rx.operators.mapTo(true));
      var hideToFalse = this.onHideClick.pipe(Rx.operators.mapTo(false));

      this.toggleFilter =
        Rx.merge(
          showToTrue,
          hideToFalse
        ).pipe(
          mn.core.rxOperatorsShareReplay(1)//do not calculate toggleFilter on each subscription
        );

      this.mnFocusStream =
        showToTrue.pipe(
          Rx.operators.debounceTime(0)//wait until field will be shown in order to do focus
        );
    }
  })(window.rxjs);
