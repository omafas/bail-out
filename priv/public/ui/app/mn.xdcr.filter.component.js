/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {merge, pipe} from 'rxjs';
import {map, filter, startWith,
        withLatestFrom, takeUntil} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnXDCRService} from "./mn.xdcr.service.js";
import {MnFormService} from "./mn.form.service.js";
import {MnKeyspaceSelectorService} from "./mn.keyspace.selector.service.js";
import {MnAdminService} from "./mn.admin.service.js";

export {MnXDCRFilterComponent};

class MnXDCRFilterComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-filter",
      templateUrl: "app/mn.xdcr.filter.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "bucket",
        "group",
        "xdcrGroup",
        "isEditMode",
        "settingsPipe"
      ]
    })
  ]}

  static get parameters() { return [
    MnXDCRService,
    MnFormService,
    MnKeyspaceSelectorService,
    MnAdminService
  ]}

  constructor(mnXDCRService, mnFormService, mnKeyspaceSelectorService, mnAdminService) {
    super();

    this.form = mnFormService.create(this);

    this.formHelper =
      mnFormService.create(this)
      .setFormGroup({enableFilters: false});

    this.postRegexpValidation =
      mnXDCRService.stream.postRegexpValidation;
    this.postSettingsReplicationsValidation =
      mnXDCRService.stream.postSettingsReplicationsValidation;
    this.postCreateReplication =
      mnXDCRService.stream.postCreateReplication;
    this.postSettingsReplications =
      mnXDCRService.stream.postSettingsReplications;
    this.compatVersion70 =
      mnAdminService.stream.compatVersion70;

    this.mnKeyspaceSelectorService = mnKeyspaceSelectorService;

    this.errors = merge(
      this.postRegexpValidation.success,
      this.postRegexpValidation.error,
      this.postCreateReplication.error,
      this.postSettingsReplications.error,
      this.postSettingsReplicationsValidation.error
    ).pipe(map(errors => {
      return errors &&
        (errors.error ? errors.error._ ? errors.error._ :
         errors.error : errors.filterExpression);
    }));

  }

  ngOnInit() {
    this.mnKeyspaceSelector =
      this.mnKeyspaceSelectorService.createCollectionSelector({
        component: this,
        steps: ["bucket", "scope", "collection"]
      });

    let validateOnStream =
        merge(this.group.valueChanges,
              this.mnKeyspaceSelector.stream.step.pipe(filter(v => v == "ok")));

    this.form
      .setFormGroup(this.group)
      .setPackPipe(pipe(withLatestFrom(this.mnKeyspaceSelector.stream.result,
                                       this.compatVersion70),
                        filter(([, r, is70]) => is70 ?
                               r.bucket && r.scope && r.collection :
                               !!this.bucket),
                        map(this.pack.bind(this))))
      .setSourceShared(this.settingsPipe)
      .setPostRequest(this.postRegexpValidation)
      .setValidation(this.postRegexpValidation, null, validateOnStream)
      .trackSubmit()
      .clearErrors();

    this.formHelper.group.patchValue({
      enableFilters: !!this.group.get("filterExpression").value
    });

    let hasSourceBucketField = this.xdcrGroup.get("fromBucket");
    if (hasSourceBucketField) {
      hasSourceBucketField.valueChanges
        .pipe(startWith(hasSourceBucketField.value),
              takeUntil(this.mnOnDestroy))
        .subscribe(v => {
          let action = v ? "enable" : "disable";
          this.formHelper.group.get("enableFilters")[action]({onlySelf: true});
        });
    }
  }

  pack([, result, is70]) {
    let rv = {
      expression: this.group.get("filterExpression").value,
      docId: this.group.get("docId").value,
    };
    if (is70) {
      rv.bucket = result.bucket.name;
      rv.scope = result.scope.name;
      rv.collection = result.collection.name;
    } else {
      rv.bucket = this.bucket;
    }
    return rv;
  }
}
