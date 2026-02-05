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
import template from "./mn.security.audit.user.activity.role.html";

export {MnSecurityAuditUserActivityRoleComponent};

class MnSecurityAuditUserActivityRoleComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-security-audit-user-activity-role",
      template,
      inputs: [
        "group",
        "roleDescriptors",
        "moduleName"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  constructor() {
    super();
    this.onToggleClick = new Subject();
    this.toggleSection = this.onToggleClick.pipe(scan(not, false),
                                                 shareReplay({refCount: true, bufferSize: 1}));
  }

  ngOnInit() {
    this.formHelper = new FormGroup({
      toggleAll: new FormControl()
    });
    this.thisDescriptors = this.roleDescriptors.pipe(pluck(this.moduleName));
    var thisDescriptorsByID = this.thisDescriptors
        .pipe(map(desc => desc.reduce((acc, item) => {
          acc[item.role] = item;
          return acc;
        }, {})));

    this.thisDescriptors
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.generateForm.bind(this));

    var thisModuleGroup = this.group.get("roleDescriptors").get(this.moduleName);

    this.thisModuleChanges =
      thisModuleGroup.valueChanges.pipe(startWith(thisModuleGroup.getRawValue()),
                                        map(() => thisModuleGroup.getRawValue()));

    this.isUserActivityEnabled =
      this.group.valueChanges.pipe(startWith(this.group.value),
                                        pluck("enabled"),
                                        distinctUntilChanged());

    this.isUserActivityEnabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableToggleAll.bind(this));

    combineLatest(this.isUserActivityEnabled, thisDescriptorsByID)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableFields.bind(this));

    this.isFieldEnabled =
      this.thisModuleChanges.pipe(map(pipe(Object.values, includes(true))),
                                  shareReplay({refCount: true, bufferSize: 1}));

    this.thisModuleChanges
      .pipe(map(pipe(Object.values, all(equals(true)))),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.setToggleAllValue.bind(this));

    combineLatest(this.formHelper.get("toggleAll").valueChanges, thisDescriptorsByID)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.doToggleAll.bind(this));

  }

  maybeDisableToggleAll(value) {
    var method = value ? "enable" : "disable";
    this.formHelper.get("toggleAll")[method]({emitEvent: false});
  }

  maybeDisableFields(value) {
    var controls = this.group.get("roleDescriptors").get(this.moduleName).controls;
    Object.keys(controls).forEach(controlID => {
      var method = !value[1][controlID].nonFilterable && value[0]  ? "enable" : "disable";
      controls[controlID][method]({emitEvent: false});
    });
  }

  setToggleAllValue(value) {
    this.formHelper.get("toggleAll").setValue(value, {emitEvent: false});
  }

  doToggleAll(value) {
    var thisModule = this.group.get("roleDescriptors").get(this.moduleName);
    var ids = Object.keys(thisModule.value);
    thisModule.patchValue(ids.reduce((acc, key) => {
      acc[key] = value[1][key].nonFilterable || value[0];
      return acc;
    }, {}));
  }

  generateForm(descriptors) {
    this.group.get("roleDescriptors")
      .addControl(this.moduleName, new FormGroup(
        descriptors.reduce(function (acc, item) {
          acc[item.role] = new FormControl(item.value);
          return acc;
        }.bind(this), {})
      ));
  }
}
