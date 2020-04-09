var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnXDCRAddReference =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnXDCRAddReference, mn.core.MnEventableComponent);

    MnXDCRAddReference.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-xdcr-add-reference.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush,
        inputs: [
          "reference"
        ],
      })
    ];

    MnXDCRAddReference.parameters = [
      mn.services.MnForm,
      mn.services.MnPools,
      mn.services.MnXDCR,
      ngb.NgbActiveModal
    ];

    MnXDCRAddReference.prototype.ngOnInit = ngOnInit;
    MnXDCRAddReference.prototype.setInitialValues = setInitialValues;
    MnXDCRAddReference.prototype.pack = pack;

    return MnXDCRAddReference;

    function ngOnInit() {
      this.isNew = !this.reference;

      this.isEnterprise
        .pipe(Rx.operators.first())
        .subscribe(this.setInitialValues.bind(this));
    }

    function setInitialValues(isEnterprise) {
      var value;
      if (this.reference) {
        value = Object.assign({}, this.reference);
      } else {
        value = {username: 'Administrator'};
      }
      if (!value.encryptionType && isEnterprise) {
        value.encryptionType = "half";
      }
      this.form.group.patchValue(value, {emitEvent: false});
    }

    function pack() {
      return [this.form.group.value, this.reference && this.reference.name];
    }

    function MnXDCRAddReference(mnFormService, mnPoolsService, mnXDCRService, activeModal) {
      mn.core.MnEventableComponent.call(this);

      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.postRemoteClusters = mnXDCRService.stream.postRemoteClusters;
      this.activeModal = activeModal;

      this.form = mnFormService.create(this)
        .setFormGroup({name: null,
                       hostname: null,
                       username: null,
                       password: null,
                       demandEncryption: null,
                       encryptionType: null,
                       certificate: null,
                       clientCertificate: null,
                       clientKey: null})
        .setPackPipe(Rx.operators.map(this.pack.bind(this)))
        .setPostRequest(this.postRemoteClusters)
        .clearErrors()
        .successMessage("Cluster reference saved successfully!")
        .success(function () {
          activeModal.close();
          mnXDCRService.stream.updateRemoteClusters.next();
        });

    }

  })(window.rxjs);
