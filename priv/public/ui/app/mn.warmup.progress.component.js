/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {Subject, BehaviorSubject, combineLatest} from 'rxjs';
import {map, tap, withLatestFrom, takeUntil} from 'rxjs/operators';
import {sortBy, prop} from 'ramda';

import {MnLifeCycleHooksToStream} from './mn.core.js';

export {MnWarmupProgressComponent};

class MnWarmupProgressComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-warmup-progress",
      templateUrl: 'app/mn.warmup.progress.html',
      inputs: [
        "mnTasks",
        "mnSortBy"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() {
    return [];
  }

  constructor() {
    super();

    this.limit = 3;
    this.limitTo = new BehaviorSubject(this.limit);
    this.onToggle = new Subject();
  }

  ngOnInit() {
    this.tasks =
      combineLatest(this.mnTasks,
                    this.limitTo)
      .pipe(map(this.slice.bind(this)),
            map(this.sortBy.bind(this)));

    this.isTasksLessThanLimit = this.mnTasks
      .pipe(map(this.isLessThanLimit.bind(this)));

    this.toggleText = this.limitTo
      .pipe(map(this.doToggleText.bind(this)));

    this.onToggle
      .pipe(tap(this.stopPropagation.bind(this)),
            withLatestFrom(this.limitTo),
            map(this.toggle.bind(this)),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.limitTo);
  }

  slice([tasks, limit]) {
    return tasks.slice(0, limit);
  }

  sortBy(tasks) {
    return sortBy(prop(this.mnSortBy))(tasks);
  }

  isLessThanLimit(tasks) {
    return tasks.length <= this.limit;
  }

  stopPropagation($event) {
    $event.stopPropagation();
  }

  toggle([, limit]) {
    return limit ? undefined : this.limit;
  }

  doToggleText(limit) {
    return limit ? '... more' : 'less';
  }
}
