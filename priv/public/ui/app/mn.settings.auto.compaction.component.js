/*
  Copyright 2021-Present Couchbase, Inc.

  Use of this software is governed by the Business Source License included in
  the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
  file, in accordance with the Business Source License, use of this software will
  be governed by the Apache License, Version 2.0, included in the file
  licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {UIRouter} from '@uirouter/angular';
import {FormBuilder} from '@angular/forms';
import {pipe, merge} from 'rxjs';
import {map} from 'rxjs/operators';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnFormService} from './mn.form.service.js';
import {MnSettingsAutoCompactionService} from './mn.settings.auto.compaction.service.js';
import {MnHelperService} from './mn.helper.service.js';
import template from "./mn.settings.auto.compaction.html";

export {MnSettingsAutoCompactionComponent};

class MnSettingsAutoCompactionComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    MnPermissions,
    UIRouter,
    MnSettingsAutoCompactionService,
    FormBuilder,
    MnHelperService
  ]}

  constructor(mnFormService, mnPermissions, uiRouter, mnSettingsAutoCompactionService, formBuilder, mnHelperService) {
    super();

    this.postAutoCompaction = mnSettingsAutoCompactionService.stream.postAutoCompaction;
    this.postAutoCompactionValidation = mnSettingsAutoCompactionService.stream.postAutoCompactionValidation;
    this.getAutoCompaction = mnSettingsAutoCompactionService.stream.getAutoCompaction;
    this.permissions = mnPermissions.stream;
    this.uiRouter = uiRouter;
    this.formBuilder = formBuilder;
    this.cancel = this.cancel.bind(this);

    this.httpError = merge(this.postAutoCompaction.error,
                           this.postAutoCompactionValidation.error);

    this.hasErrors = this.httpError
      .pipe(map(val => val && Object.keys(val.errors).length));

    this.transformMBToBytes = mnHelperService.transformMBToBytes;
    this.stringifyValues = mnHelperService.stringifyValues;
    this.flattenData = mnSettingsAutoCompactionService.flattenData;
    this.hasWritePermissions = this.permissions
      .pipe(map(permissions => permissions.cluster.settings.autocompaction.write));

    let settingsSource = mnSettingsAutoCompactionService.stream.settingsSource;

    this.form = mnFormService.create(this)
      .setFormGroup({
        indexCompactionMode: null,
        allowedTimePeriod: this.formBuilder.group({
          fromHour: null,
          toHour: null,
          fromMinute: null,
          toMinute: null,
          abortOutside: false
        }),
        databaseFragmentationThreshold: this.formBuilder.group({
          percentageFlag: null,
          sizeFlag: null,
          percentage: null,
          size: null
        }),
        magmaFragmentationPercentage: null,
        viewFragmentationThreshold: this.formBuilder.group({
          percentageFlag: null,
          sizeFlag: null,
          percentage: null,
          size: null
        }),
        indexFragmentationThreshold: this.formBuilder.group({
          percentage: null
        }),
        indexCircularCompaction: this.formBuilder.group({
          daysOfWeek: this.formBuilder.group({
            Monday: null,
            Tuesday: null,
            Wednesday: null,
            Thursday: null,
            Friday: null,
            Saturday: null,
            Sunday: null
          }),
          interval: this.formBuilder.group({
            fromHour: null,
            toHour: null,
            fromMinute: null,
            toMinute: null,
            abortOutside: false
          }),
        }),
        parallelDBAndViewCompaction: null,
        purgeInterval: null,
        timePeriodFlag: null
      });

    this.form
      .setPackPipe(pipe(map(mnSettingsAutoCompactionService.getAutoCompactionData.bind(mnSettingsAutoCompactionService, this.form.group))))
      .setSource(settingsSource)
      .setPostRequest(this.postAutoCompaction)
      .setValidation(this.postAutoCompactionValidation, this.hasWritePermissions)
      .clearErrors()
      .showGlobalSpinner()
      .successMessage("Settings saved successfully!");

    this.form.group.disable();
  }

  cancel() {
    return this.uiRouter.stateService.reload();
  }
}
