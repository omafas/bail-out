/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {FormGroup} from '/ui/web_modules/@angular/forms.js';
import {interval} from '/ui/web_modules/rxjs.js';
import {NgbActiveModal} from '/ui/web_modules/@ng-bootstrap/ng-bootstrap.js';
import {scan, startWith} from '/ui/web_modules/rxjs/operators.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';

export {MnSessionTimeoutDialogComponent};

class MnSessionTimeoutDialogComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "/ui/app/mn.session.timeout.dialog.html",
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    NgbActiveModal
  ]}

  constructor(activeModal) {
    super();
    this.activeModal = activeModal;
    this.formGroup = new FormGroup({});
    var time = 29;
    this.time = interval(1000).pipe(scan(acc => acc ? (--acc) : 0, time), startWith(time));
  }
}
