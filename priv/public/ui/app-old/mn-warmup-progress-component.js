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
mn.components.MnWarmupProgress =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnWarmupProgressComponent, mn.core.MnEventableComponent);

    MnWarmupProgressComponent.annotations = [
      new ng.core.Component({
        selector: "mn-warmup-progress",
        templateUrl: "app-new/mn-warmup-progress.html",
        inputs: [
          "mnTasks", //observable
          "mnSortBy" //string
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnWarmupProgressComponent.prototype.stopPropagation = stopPropagation;
    MnWarmupProgressComponent.prototype.sortBy = sortBy;
    MnWarmupProgressComponent.prototype.slice = slice;
    MnWarmupProgressComponent.prototype.toggle = toggle;
    MnWarmupProgressComponent.prototype.doToggleText = doToggleText;
    MnWarmupProgressComponent.prototype.isLessThanLimit = isLessThanLimit;
    MnWarmupProgressComponent.prototype.ngOnInit = ngOnInit;

    return MnWarmupProgressComponent;

    function MnWarmupProgressComponent() {
      mn.core.MnEventableComponent.call(this);

      this.limit = 3;
      this.onToggle = new Rx.Subject();
      this.limitTo = new Rx.BehaviorSubject(this.limit);
    }

    function ngOnInit() {
      this.tasks =
        Rx.combineLatest(
          this.mnTasks,
          this.limitTo
        ).pipe(
          Rx.operators.map(this.slice),
          Rx.operators.map(this.sortBy.bind(this))
        );

      this.isTasksLessThanLimit =
        this.mnTasks.pipe(Rx.operators.map(this.isLessThanLimit.bind(this)));

      this.toggleText =
        this.limitTo.pipe(Rx.operators.map(this.doToggleText));

      this.onToggle
        .pipe(
          Rx.operators.tap(this.stopPropagation),
          Rx.operators.withLatestFrom(this.limitTo),
          Rx.operators.map(this.toggle.bind(this)),
          Rx.operators.takeUntil(this.mnOnDestroy)
        )
        .subscribe(this.limitTo);
    }

    function stopPropagation($event) {
      $event.stopPropagation();
    }

    function sortBy(tasks) {
      return _.sortBy(tasks, this.mnSortBy);
    }

    function slice(values) {
      return values[0].slice(0, values[1]);
    }

    function toggle(limit) {
      return !!limit[1] ? undefined : this.limit;
    }

    function doToggleText(limit) {
      return !!limit ? '... more' : 'less';
    }

    function isLessThanLimit(tasks) {
      return tasks.length <= this.limit;
    }

  })(window.rxjs);
