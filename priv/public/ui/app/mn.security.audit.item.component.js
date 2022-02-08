/*
Copyright 2020-Present Couchbase, Inc.

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
import {not, pipe, contains, all, equals} from 'ramda';
import {FormControl, FormGroup} from '@angular/forms';

import {MnLifeCycleHooksToStream} from './mn.core.js';

export {MnSecurityAuditItemComponent};

class MnSecurityAuditItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-security-audit-item",
      templateUrl: "app/mn.security.audit.item.html",
      inputs: [
        "form",
        "descriptors",
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
    this.thisDescriptors = this.descriptors.pipe(pluck(this.moduleName));
    var thisDescriptorsByID = this.thisDescriptors
        .pipe(map(desc => desc.reduce((acc, item) => {
          acc[item.id] = item;
          return acc;
        }, {})));

    combineLatest(this.thisDescriptors, thisDescriptorsByID)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.generateForm.bind(this));

    var thisModuleGroup = this.form.group.get("descriptors").get(this.moduleName);


    this.thisModuleChanges =
      thisModuleGroup.valueChanges.pipe(startWith(thisModuleGroup.getRawValue()),
                                        map(() => thisModuleGroup.getRawValue()));

    this.isAuditEnabled =
      this.form.group.valueChanges.pipe(startWith(this.form.group.value),
                                        pluck("auditdEnabled"),
                                        distinctUntilChanged());

    this.isAuditEnabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableToggleAll.bind(this));

    combineLatest(this.isAuditEnabled, thisDescriptorsByID)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableFields.bind(this));

    this.isThereEnabledField =
      this.thisModuleChanges.pipe(map(pipe(Object.values, contains(true))),
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
    var controls = this.form.group.get("descriptors").get(this.moduleName).controls;
    Object.keys(controls).forEach(controlID => {
      var method = !value[1][controlID].nonFilterable && value[0]  ? "enable" : "disable";
      controls[controlID][method]({emitEvent: false});
    });
  }

  setToggleAllValue(value) {
    this.formHelper.get("toggleAll").setValue(value, {emitEvent: false});
  }

  doToggleAll(value) {
    var thisModule = this.form.group.get("descriptors").get(this.moduleName);
    var ids = Object.keys(thisModule.value);
    thisModule.patchValue(ids.reduce((acc, key) => {
      acc[key] = value[1][key].nonFilterable || value[0];
      return acc;
    }, {}));
  }

  generateForm(descriptors) {
    this.form.group.get("descriptors")
      .addControl(this.moduleName, new FormGroup(
        descriptors[0].reduce(function (acc, item) {
          acc[item.id] = new FormControl({
            value: item.value,
            disabled: descriptors[1][item.id].nonFilterable
          });
          return acc;
        }.bind(this), {})
      ));
  }

  mapNames(name) {
    switch (name) {
    case "auditd":
      return "Audit";
    case "ns_server":
      return "REST API";
    case "n1ql":
      return "Query and Index Service";
    case "eventing":
      return "Eventing Service";
    case "memcached":
      return "Data Service";
    case "xdcr":
      return name.toUpperCase();
    case "fts":
      return "Search Service";
    case "view_engine":
      return "Views";
    default:
      return name.charAt(0).toUpperCase() + name.substr(1).toLowerCase();
    }
  }
}
