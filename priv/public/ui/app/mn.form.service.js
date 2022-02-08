/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {FormBuilder, FormGroup} from '@angular/forms';
import {BehaviorSubject, Subject, NEVER, merge} from 'rxjs';
import {map, tap, first, takeUntil, switchMap, mapTo,
        shareReplay, filter, debounceTime, startWith} from 'rxjs/operators';
import {singletonGuard} from './mn.core.js';

import {MnAlerts, $rootScope} from './ajs.upgraded.providers.js';

export {MnFormService};

class MnFormService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    FormBuilder,
    MnAlerts,
    $rootScope
    // ngb.NgbModal
  ]}

  constructor(formBuilder, mnAlerts, $rootScope, modalService) {
    singletonGuard(MnFormService);

    this.formBuilder = formBuilder;
    this.mnAlerts = mnAlerts;
    this.modalService = modalService;
    this.$rootScope = $rootScope;
  }

  create(component) {
    return new MnForm(this.formBuilder, component, this.mnAlerts, this.$rootScope);
  }
}

class MnForm {
  constructor(builder, component, mnAlerts, $rootScope) {
    this.builder = builder;
    this.component = component;
    this.mnAlerts = mnAlerts;
    this.defaultPackPipe = map(this.getFormValue.bind(this));
    this.requestsChain = [];
    this.$rootScope = $rootScope;

  }

  getFormValue() {
    return this.group.value;
  }

  setFormGroup(formDescription, options) {
    this.group = (formDescription instanceof FormGroup) ?
      formDescription : this.builder.group(formDescription, options);
    return this;
  }

  setSource(source) {
    var sourcePipe = source.pipe(this.unpackPipe || tap(), first());

    this.loadingPipe = sourcePipe
      .pipe(mapTo(false),
            startWith(true));

    sourcePipe.subscribe(v => this.group.patchValue(v));

    return this;
  }

  setSourceShared(source) {
    var sourcePipe = source.pipe(this.unpackPipe || tap(),
                                 takeUntil(this.component.mnOnDestroy));

    sourcePipe.subscribe(v => this.group.patchValue(v));

    return this;
  }

  getLastRequest() {
    return this.requestsChain[this.requestsChain.length - 1];
  }

  getFirstRequest() {
    return this.requestsChain[0];
  }

  success(fn) {
    (this.requestsChain.length ? this.getLastRequest().success : this.submitPipe)
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe(fn);
    return this;
  }

  error(fn) {
    this.getLastRequest().error
      .pipe(this.unpackErrorPipe || tap(),
            takeUntil(this.component.mnOnDestroy))
      .subscribe(fn);
    return this;
  }

  successMessage(message) {
    this.success(() => this.mnAlerts.formatAndSetAlerts(message, "success", 2500));
    return this;
  }

  errorMessage(message) {
    this.error(() => this.mnAlerts.formatAndSetAlerts(message, "error"));
    return this;
  }

  disableFields(path) {
    return (value) => {
      this.group.get(path)[value ? "disable" : "enable"]({emitEvent: false});
    }
  }

  showGlobalSpinner() {
    this.trackSubmit();
    this.processing
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe(v => {
        this.$rootScope.mnGlobalSpinnerFlag = v;
      });
    return this;
  }

  trackSubmit() {
    var success = this.getLastRequest().success.pipe(mapTo(false));
    var request = this.getFirstRequest().request.pipe(mapTo(true));
    var errors = merge.apply(merge,
                             this.requestsChain.map(req =>
                                                    req.error
                                                    .pipe(filter(v => !!v)))).pipe(mapTo(false));
    this.processing =
      merge(request, success, errors)
      .pipe(shareReplay({refCount: true, bufferSize: 1}));

    return this;
  }

  setPostRequest(postRequest) {
    let lastRequest = this.getLastRequest();
    this.requestsChain.push(postRequest);

    if (!lastRequest) {
      this.createSubmitPipe();
      this.submitPipe.subscribe((v) => this.getFirstRequest().post(v));
    } else {
      lastRequest.success
        .pipe(this.getPackPipe(),
              takeUntil(this.component.mnOnDestroy))
        .subscribe((function (postRequestIndex) {
          return (v) => this.requestsChain[postRequestIndex - 1].post(v);
        }.bind(this))(this.requestsChain.length));
    }
    return this;
  }

  hasNoPostRequest() {
    this.createSubmitPipe();
    return this;
  }

  getPackPipe() {
    return this.packPipe || (this.group ? this.defaultPackPipe : tap())
  }

  createSubmitPipe() {
    this.submit = new Subject();
    this.submitPipe =
      this.submit.pipe(this.getPackPipe(),
                       takeUntil(this.component.mnOnDestroy))
  }

  setReset(reset) {
    this.reset = new Subject();
    this.reset
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe(reset);

    return this;
  }

  hasNoHandler() {
    this.getLastRequest().success
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe();
    return this;
  }

  clearErrors() {
    this.submit
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe(() => this.requestsChain.forEach((req) => req.clearError()));
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
    this.getLastRequest().error
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe((resp) => {
        if (resp && resp.status === 504) {
          this.modalService
            .open(dialog)
            .result
            .then(() => {
              this.submit.next(true);
            }, function () {});
        }
      });
    return this;
  }

  setValidation(validationPostRequest, permissionStream, validateOnStream, keepErrors) {
    validationPostRequest.response
      .pipe(takeUntil(this.component.mnOnDestroy))
      .subscribe(function () {
        if (!keepErrors) {
          validationPostRequest.clearError();
        }
      });
    //skip initialization of the form
    (permissionStream || new BehaviorSubject(true)).pipe(
      switchMap((v) => v ? (validateOnStream || this.group.valueChanges) : NEVER),
      debounceTime(500),
      this.getPackPipe(),
      takeUntil(this.component.mnOnDestroy))
      .subscribe((v) => {
        validationPostRequest.post(v);
      });
    return this;
  }
}
