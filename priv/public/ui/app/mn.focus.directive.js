/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Directive, ElementRef} from '@angular/core';
import {BehaviorSubject} from 'rxjs';
import {filter, takeUntil} from 'rxjs/operators';
import {MnLifeCycleHooksToStream} from './mn.core.js';

export { MnFocusDirective };

class MnFocusDirective extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Directive({
      selector: "[mnFocus]",
      inputs: [
        "mnFocus",
        "mnName"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}


  static get parameters() { return [
    ElementRef
  ]}

  constructor(el) {
    super();
    this.el = el.nativeElement;
  }

  ngOnInit() {
    this.mnFocus = this.mnFocus || new BehaviorSubject(true);
    this.mnFocus.pipe(
      filter(this.maybePrevent.bind(this)),
      takeUntil(this.mnOnDestroy)
    ).subscribe(this.doFocus.bind(this));
  }

  doFocus() {
    this.el.focus();
  }

  maybePrevent(value) {
    return (typeof value === "string") ? value === this.mnName : value;
  }

}
