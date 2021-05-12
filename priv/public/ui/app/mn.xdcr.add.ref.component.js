/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {NgbActiveModal} from '/ui/web_modules/@ng-bootstrap/ng-bootstrap.js';
import {first, map} from '/ui/web_modules/rxjs/operators.js';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from "./mn.form.service.js";
import {MnPoolsService} from './mn.pools.service.js';
import {MnXDCRService} from './mn.xdcr.service.js';

export {MnXDCRAddRefComponent};

class MnXDCRAddRefComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "/ui/app/mn.xdcr.add.ref.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item"
      ],
    })
  ]}

  static get parameters() { return [
    MnFormService,
    MnPoolsService,
    MnXDCRService,
    NgbActiveModal
  ]}

  constructor(mnFormService, mnPoolsService, mnXDCRService, activeModal) {
    super();

    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.postRemoteClusters = mnXDCRService.stream.postRemoteClusters;
    this.activeModal = activeModal;

    this.formHelper =
      mnFormService.create(this)
      .setFormGroup({useClientCertificate: false});

    this.form = mnFormService.create(this);

    this.form
      .setFormGroup({name: "",
                     hostname: "",
                     username: "",
                     password: "",
                     demandEncryption: false,
                     encryptionType: null,
                     certificate: "",
                     clientCertificate: "",
                     clientKey: ""})
      .setPackPipe(map(this.pack.bind(this)))
      .setPostRequest(this.postRemoteClusters)
      .clearErrors()
      .successMessage("Cluster reference saved successfully!")
      .showGlobalSpinner()
      .success(function () {
        activeModal.close();
        mnXDCRService.stream.updateRemoteClusters.next();
      });

  }

  ngOnInit() {
    this.isNew = !this.item;

    this.isEnterprise
      .pipe(first())
      .subscribe(this.setInitialValues.bind(this));
  }

  setInitialValues(isEnterprise) {
    var value;
    if (this.item) {
      value = Object.assign({}, this.item);
    } else {
      value = {username: 'Administrator'};
    }
    if (!value.encryptionType && isEnterprise) {
      value.encryptionType = "half";
    }
    this.form.group.patchValue(value, {emitEvent: false});
  }

  pack() {
    return [this.form.group.value, this.item && this.item.name];
  }
}
