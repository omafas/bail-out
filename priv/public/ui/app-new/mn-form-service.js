/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnForm = (function (Rx) {

  MnForm.annotations = [
    new ng.core.Injectable()
  ];

  MnForm.parameters = [
    ng.forms.FormBuilder,
    mn.services.MnAlerts,
    ngb.NgbModal
  ];

  MnForm.prototype.create = create;

  Form.prototype.setFormGroup = setFormGroup;
  Form.prototype.setSource = setSource;
  Form.prototype.setPostRequest = setPostRequest;
  Form.prototype.clearErrors = clearErrors;
  Form.prototype.successMessage = successMessage;
  Form.prototype.errorMessage = errorMessage;
  Form.prototype.disableFields = disableFields;
  Form.prototype.setValidation = setValidation;
  Form.prototype.setUnpackPipe = setUnpackPipe;
  Form.prototype.setUnpackErrorPipe = setUnpackErrorPipe;
  Form.prototype.setPackPipe = setPackPipe;
  Form.prototype.getFormValue = getFormValue;
  Form.prototype.success = success;
  Form.prototype.error = error;
  Form.prototype.hasNoHandler = hasNoHandler;
  Form.prototype.confirmation504 = confirmation504;

  return MnForm;

  function MnForm(formBuilder, mnAlertsService, modalService) {
    this.formBuilder = formBuilder;
    this.mnAlertsService = mnAlertsService;
    this.modalService = modalService;
  }

  function create(component) {
    return new Form(this.formBuilder, component, this.mnAlertsService);
  }

  function Form(builder, component, mnAlertsService) {
    this.builder = builder;
    this.component = component;
    this.mnAlertsService = mnAlertsService;
    this.defaultPackPipe = Rx.operators.map(this.getFormValue.bind(this));
  }

  function getFormValue() {
    return this.group.value;
  }

  function setFormGroup(formDescription) {
    this.group = (formDescription instanceof ng.forms.FormGroup) ?
      formDescription : this.builder.group(formDescription);
    return this;
  }

  function setSource(source) {
    this.sourcePipe = source.pipe(this.unpackPipe || Rx.operators.tap(),
                                  Rx.operators.first());

    this.changes = Rx.merge(this.group.valueChanges, this.sourcePipe);

    this.sourcePipe
      .pipe(Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(function (v) {
        this.group.patchValue(v, {emitEvent: false});
      }.bind(this));
    return this;
  }

  function success(fn) {
    this.postRequest.success
      .pipe(Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(fn);
    return this;
  }

  function error(fn) {
    this.postRequest.error
      .pipe(this.unpackErrorPipe || Rx.operators.tap(),
            Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(fn);
    return this;
  }

  function successMessage(message) {
    this.success(this.mnAlertsService.success(message));
    return this;
  }

  function errorMessage(message) {
    this.error(this.mnAlertsService.error(message));
    return this;
  }

  function disableFields(path) {
    return function (value) {
      this.group.get(path)[value ? "disable" : "enable"]({emitEvent: false});
    }.bind(this);
  }

  function setPostRequest(postRequest) {
    this.postRequest = postRequest;
    this.submit = new Rx.Subject();

    this.submit
      .pipe(this.packPipe || (this.group ? this.defaultPackPipe : Rx.operators.tap()),
            Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(function (v) {
        this.postRequest.post(v);
      }.bind(this));
    return this;
  }

  function hasNoHandler() {
    this.postRequest.success
      .pipe(Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe();
    return this;
  }

  function clearErrors() {
    this.submit
      .pipe(Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(function () {
        this.postRequest.clearError();
      }.bind(this));
    return this;
  }

  function setUnpackErrorPipe(unpackErrorPipe) {
    this.unpackErrorPipe = unpackErrorPipe;
    return this
  }

  function setUnpackPipe(unpackPipe) {
    this.unpackPipe = unpackPipe;
    return this;
  }

  function setPackPipe(packPipe) {
    this.packPipe = packPipe;
    return this;
  }

  function confirmation504(dialog) {
    this.postRequest.error
      .pipe(Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(function (resp) {
        if (resp && resp.status === 504) {
          this.modalService
            .open(dialog)
            .result
            .then(function (a) {
              this.submit.next(true);
            }.bind(this), function () {});
        }
      }.bind(this));
    return this;
  }

  function setValidation(validationPostRequest, permissionStream) {
    validationPostRequest.response
      .pipe(Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(function () {
        validationPostRequest.clearError();
      });
    (permissionStream || new Rx.BehaviorSubject(true))
      .pipe(Rx.operators.switchMap(function (v) {
        return v ? this.group.valueChanges : Rx.NEVER;
      }.bind(this)),
            Rx.operators.throttleTime(500, undefined, {leading: true, trailing: true}),
            // Rx.operators.debounceTime(0),
            this.packPipe || this.defaultPackPipe,
            Rx.operators.takeUntil(this.component.mnOnDestroy))
      .subscribe(function (v) {
        validationPostRequest.post(v);
      }.bind(this));
    return this;
  }

})(window.rxjs);
