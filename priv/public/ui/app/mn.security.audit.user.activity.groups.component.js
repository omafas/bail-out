/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {pluck, scan, distinctUntilChanged, shareReplay,
        takeUntil, startWith, map} from 'rxjs/operators';
import {Subject, combineLatest} from 'rxjs';
import {not, pipe, includes, all, equals} from 'ramda';
import {FormControl, FormGroup} from '@angular/forms';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import template from "./mn.security.audit.user.activity.groups.html";

export {MnSecurityAuditUserActivityGroupsComponent};

class MnSecurityAuditUserActivityGroupsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-security-audit-user-activity-groups",
      template,
      inputs: [
        "group",
        "groupDescriptors"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  constructor() {
    super();
  }

  ngOnInit() {
    this.formHelper = new FormGroup({
      toggleAll: new FormControl()
    });

    this.groupDescriptors
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.generateForm.bind(this));

    this.groupChanges =
      this.group.get("groupDescriptors").valueChanges.pipe(
        startWith(this.group.get("groupDescriptors").getRawValue()),
        map(() => this.group.get("groupDescriptors").getRawValue()));

    this.isUserActivityEnabled =
      this.group.valueChanges.pipe(
        startWith(this.group.value),
        pluck("enabled"),
        distinctUntilChanged());

    this.isUserActivityEnabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableToggleAll.bind(this));

    combineLatest(this.isUserActivityEnabled, this.groupDescriptors)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableFields.bind(this));

    this.groupChanges
      .pipe(map(pipe(Object.values, all(equals(true)))),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.setToggleAllValue.bind(this));

    this.formHelper.get("toggleAll").valueChanges
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.doToggleAll.bind(this));

  }

  maybeDisableToggleAll(value) {
    let method = value ? "enable" : "disable";
    this.formHelper.get("toggleAll")[method]({emitEvent: false});
  }

  maybeDisableFields(value) {
    let controls = this.group.get("groupDescriptors").controls;
    Object.keys(controls).forEach(controlID => {
      let method = value[0]  ? "enable" : "disable";
      controls[controlID][method]({emitEvent: false});
    });
  }

  setToggleAllValue(value) {
    this.formHelper.get("toggleAll").setValue(value, {emitEvent: false});
  }

  doToggleAll(value) {
    let groups = this.group.get("groupDescriptors");
    let ids = Object.keys(groups.value);
    groups.patchValue(ids.reduce((acc, key) => {
      acc[key] = value;
      return acc;
    }, {}));
  }

  generateForm(descriptors) {
    Object.keys(descriptors).forEach(group => {
      this.group.get("groupDescriptors").addControl(group, new FormControl(descriptors[group].value))
    });
  }
}
