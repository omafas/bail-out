/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {FormGroup} from '@angular/forms';
import {interval} from 'rxjs';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';
import {scan, startWith} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import template from "./mn.session.timeout.dialog.html";

export {MnSessionTimeoutDialogComponent};

class MnSessionTimeoutDialogComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
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
