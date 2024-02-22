/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { ChangeDetectionStrategy, Directive, ElementRef, Renderer2 } from '@angular/core';
import { MnLifeCycleHooksToStream } from './mn.core.js';

export { MnSpinnerDirective };

class MnSpinnerDirective extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Directive({
      selector: "[mnSpinner]",
      inputs: ["mnSpinner"],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}


  static get parameters() { return [
    ElementRef,
    Renderer2
  ]}

  constructor(el, mnRenderer) {
    super();

    this.renderer = mnRenderer;
    this.el = el;

    this.loadingElement = this.renderer.createElement('div');

    this.renderer.addClass(this.loadingElement, 'spinner');
    this.renderer.addClass(this.el.nativeElement, 'relative')
    this.renderer.appendChild(this.el.nativeElement, this.loadingElement);
  }

  ngOnChanges() {
    if (this.mnSpinner) {
      this.renderer.addClass(this.loadingElement, 'hidden');
    } else {
      this.renderer.removeClass(this.loadingElement, 'hidden');
    }
  }
}
