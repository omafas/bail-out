/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { ChangeDetectionStrategy,
         Directive,
         ElementRef } from '../web_modules/@angular/core.js';
import { BehaviorSubject } from '../web_modules/rxjs.js';
import { filter, takeUntil } from '../web_modules/rxjs/operators.js';
import { MnLifeCycleHooksToStream } from './mn.core.js';

export { MnFocusDirective };

class MnFocusDirective extends MnLifeCycleHooksToStream {
  static annotations = [
    new Directive({
      selector: "[mnFocus]",
      inputs: [
        "mnFocus"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ];

  static parameters = [ElementRef]

  constructor(el) {
    super();
    this.el = el.nativeElement;
    this.formControlName = this.el.getAttribute("formControlName");
  }

  ngOnInit() {
    this.mnFocus = this.mnFocus || new BehaviorSubject(true);
    this.mnFocus.pipe(
      filter(this.maybePrevent.bind(this)),
      takeUntil(this.mnOnDestroy)
    ).subscribe(this.doFocus.bind(this));
  }

  doFocus(value) {
    this.el.focus();
  }

  maybePrevent(value) {
    return (typeof value === "string") ? value === this.formControlName : value
  }

}
