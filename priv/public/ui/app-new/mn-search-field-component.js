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
mn.components.MnSearchField =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnSearchFieldComponent, mn.core.MnEventableComponent);

    MnSearchFieldComponent.annotations = [
      new ng.core.Component({
        selector: "mn-search-field",
        templateUrl: 'app-new/mn-search-field.html',
        inputs: [
          "mnFormGroup",
          "mnFocusStream",
          "mnClearStream",
          "mnPlaceholder"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnSearchFieldComponent.prototype.clearSearchTerm = clearSearchTerm;
    MnSearchFieldComponent.prototype.ngOnInit = ngOnInit;

    return MnSearchFieldComponent;

    function MnSearchFieldComponent() {
      mn.core.MnEventableComponent.call(this);
    }

    function ngOnInit() {
      this.onClearClick = new Rx.Subject();
      this.mnFocusStream = this.mnFocusStream || Rx.NEVER;
      this.mnClearStream = this.mnClearStream || Rx.NEVER;

      Rx.merge(this.onClearClick, this.mnClearStream)
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.clearSearchTerm.bind(this));

      this.isSearchPresent =
        this.mnFormGroup.valueChanges.pipe(
          Rx.operators.pluck("searchTerm"),
          Rx.operators.map(Boolean),
          mn.core.rxOperatorsShareReplay(1)
        );

    }

    function clearSearchTerm() {
      this.mnFormGroup.patchValue({searchTerm: ""});
    }
  })(window.rxjs);
