/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/
import {startWith} from 'rxjs/operators';
import {ChangeDetectionStrategy, Component} from '@angular/core';
import template from "./mn.input.filter.html";

export {MnInputFilterComponent};

class MnInputFilterComponent {
  static get annotations() { return [
    new Component({
      selector: "mn-input-filter",
      template,
      inputs: [
        "group",
        "mnFocusStatus",
        "mnFocus",
        "mnClearDisabled",
        "mnPlaceholder",
        "mnName"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
  ]}

  ngOnInit() {
    let value = this.group.get('value');
    this.currentValue = value.valueChanges.pipe(startWith(value.value));
  }

  constructor() {
  }

  onBlur() {
    this.mnFocusStatus && this.mnFocusStatus.next(false);
  }

  onFocus() {
    this.mnFocusStatus && this.mnFocusStatus.next(true);
  }
}
