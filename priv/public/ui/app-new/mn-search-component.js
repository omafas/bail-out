/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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
