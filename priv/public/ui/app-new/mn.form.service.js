/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Injectable } from '../web_modules/@angular/core.js';
import { FormBuilder, FormGroup } from '../web_modules/@angular/forms.js';
import { MnAlertsService } from './mn.alerts.service.js';
import { BehaviorSubject, Subject } from "../web_modules/rxjs.js";
import { map, tap, first, merge, takeUntil, switchMap, throttleTime } from "../web_modules/rxjs/operators.js";

export { MnFormService };

class MnFormService {
  static annotations = [
    new Injectable()
  ]

  static parameters = [
    FormBuilder,
    MnAlertsService
    // ngb.NgbModal
  ]

  constructor(formBuilder, mnAlertsService, modalService) {
    this.formBuilder = formBuilder;
    this.mnAlertsService = mnAlertsService;
    this.modalService = modalService;
  }

  create(component) {
    return new MnForm(this.formBuilder, component, this.mnAlertsService);
  }
}

class MnForm {
  constructor(builder, component, mnAlertsService) {
    this.builder = builder;
    this.component = component;
    this.mnAlertsService = mnAlertsService;
    this.defaultPackPipe = map(this.getFormValue.bind(this));
  }

  getFormValue() {
    return this.group.value;
  }

  setFormGroup(formDescription) {
    this.group = (formDescription instanceof FormGroup) ?
      formDescription : this.builder.group(formDescription);
    return this;
  }

  setSource(source) {
    var sourcePipe = source.pipe(this.unpackPipe || tap(), first());

    this.changes = merge(this.group.valueChanges, sourcePipe);

    sourcePipe
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe((v) => {
        this.group.patchValue(v, {emitEvent: false});
      });
    return this;
  }

  success(fn) {
    this.postRequest.success
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe(fn);
    return this;
  }

  error(fn) {
    this.postRequest.error
      .pipe(this.unpackErrorPipe || tap(),
            takeUntil(this.component.mnOnDestroy))
      .subscribe(fn);
    return this;
  }

  successMessage(message) {
    this.success(this.mnAlertsService.success(message));
    return this;
  }

  errorMessage(message) {
    this.error(this.mnAlertsService.error(message));
    return this;
  }

  disableFields(path) {
    return (value) => {
      this.group.get(path)[value ? "disable" : "enable"]({emitEvent: false});
    }
  }

  setPostRequest(postRequest) {
    this.postRequest = postRequest;
    this.submit = new Subject();

    this.submit
      .pipe(this.packPipe || (this.group ? this.defaultPackPipe : tap()),
            takeUntil(this.component.mnOnDestroy))
      .subscribe(function (v) {
        this.postRequest.post(v);
      }.bind(this));
    return this;
  }

  hasNoHandler() {
    this.postRequest.success
      .pipe(Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe();
    return this;
  }

  clearErrors() {
    this.submit
      .pipe(Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(function () {
        this.postRequest.clearError();
      }.bind(this));
    return this;
  }

  setUnpackErrorPipe(unpackErrorPipe) {
    this.unpackErrorPipe = unpackErrorPipe;
    return this
  }

  setUnpackPipe(unpackPipe) {
    this.unpackPipe = unpackPipe;
    return this;
  }

  setPackPipe(packPipe) {
    this.packPipe = packPipe;
    return this;
  }

  confirmation504(dialog) {
    this.postRequest.error
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe((resp) => {
        if (resp && resp.status === 504) {
          this.modalService
            .open(dialog)
            .result
            .then((a) => {
              this.submit.next(true);
            }, function () {});
        }
      });
    return this;
  }

  setValidation(validationPostRequest, permissionStream) {
    validationPostRequest.response
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe(function () {
        validationPostRequest.clearError();
      });

    (permissionStream || new BehaviorSubject(true)).pipe(
      switchMap((v) => {
        return v ? this.group.valueChanges : Rx.NEVER;
      }),
      throttleTime(500, undefined, {leading: true, trailing: true}),
      // Rx.operators.debounceTime(0),
      this.packPipe || this.defaultPackPipe,
      takeUntil(this.component.mnOnDestroy))
      .subscribe((v) => {
        validationPostRequest.post(v);
      });
    return this;
  }
}
