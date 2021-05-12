/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js'
import {takeUntil, withLatestFrom, merge, startWith} from '/ui/web_modules/rxjs/operators.js';
import {FormBuilder} from '/ui/web_modules/@angular/forms.js';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnXDCRService, collectionDelimiter} from "./mn.xdcr.service.js";

export {MnXDCRAddRepMappingItemComponent};

class MnXDCRAddRepMappingItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-add-rep-mapping-item",
      templateUrl: "/ui/app/mn.xdcr.add.rep.mapping.item.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item",
        "explicitMappingGroup",
        "parent",
        "explicitMappingRules",
        "keyspace"
      ]
    })
  ]}

  static get parameters() { return [
    FormBuilder,
    MnXDCRService
  ]}

  constructor(formBuilder, mnXDCRService) {
    super();
    this.formBuilder = formBuilder;
    this.setMappingRule = mnXDCRService.setMappingRule;
  }

  ngOnInit() {
    let isCollection = this.keyspace == "collections";

    this.group = this.explicitMappingGroup[this.keyspace][this.parent];
    this.controls = this.explicitMappingGroup[this.keyspace + "Controls"][this.parent];
    this.scopeGroup = isCollection ? this.explicitMappingGroup.scopes.root : this.group;

    if (!this.group.flags.get(this.item.name)) {
      let rules =
          this.explicitMappingRules.getValue();
      let name =
          isCollection ? this.parent + collectionDelimiter + this.item.name : this.item.name;

      this.group.flags.addControl(
        this.item.name,
        this.formBuilder.control({
          value: !!rules[name],
          disabled: false
        })
      );
    }
    if (!this.group.fields.get(this.item.name)) {
      this.group.fields.addControl(
        this.item.name,
        this.formBuilder.control({
          value: this.item.name,
          disabled: isCollection ? this.group.flags.get(this.item.name).value : false
        })
      );
    }

    this.flag = this.group.flags.get(this.item.name);
    this.field = this.group.fields.get(this.item.name);

    let doSet = isCollection ? this.setCollectionsRule : this.setScopesRule;


    this.flag.valueChanges
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(doSet.bind(this));

    this.field.valueChanges
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(doSet.bind(this));

    if (!isCollection) {
      this.field.valueChanges
        .pipe(takeUntil(this.mnOnDestroy))
        .subscribe(this.updateScopeName.bind(this));
    } else {
      this.flag.valueChanges.pipe(startWith(this.flag.value))
        .pipe(takeUntil(this.mnOnDestroy))
        .subscribe(this.toggleFiled.bind(this));
    }
  }

  updateScopeName() {
    let rules = this.explicitMappingRules.getValue();
    let collectionGroup = this.explicitMappingGroup.collections[this.item.name];
    let collectionControls = this.explicitMappingGroup.collectionsControls[this.item.name];

    this.item.collections.forEach(item => {
      let sourceScope = this.item.name;
      let sourceCollection = item.name;
      let source = sourceScope + collectionDelimiter + sourceCollection;

      let targetScope = this.field.value;
      let targetCollection = collectionGroup.fields.get(item.name).value;
      let target = targetScope + collectionDelimiter + targetCollection;

      let collectionFlag = collectionGroup.flags.get(item.name).value;
      this.setMappingRule(collectionFlag, source, target, rules);
    });

    this.explicitMappingRules.next(rules);
  }

  setScopesRule() {
    let rules = this.explicitMappingRules.getValue();

    let sourceScope = this.item.name;
    let targetScope = this.field.value;

    let scopeFlag = this.flag.value

    this.setMappingRule(scopeFlag, sourceScope, targetScope, rules);

    this.explicitMappingRules.next(rules);
  }

  setCollectionsRule() {
    let rules = this.explicitMappingRules.getValue();

    let sourceScope = this.parent;
    let sourceCollection = this.item.name;
    let source = sourceScope + collectionDelimiter + sourceCollection;

    let targetScope = this.scopeGroup.fields.get(this.parent).value;
    let targetCollection = this.field.value;
    let target = targetScope + collectionDelimiter + targetCollection;

    let collectionFlag = this.flag.value;

    this.setMappingRule(collectionFlag, source, target, rules);

    this.explicitMappingRules.next(rules);
  }

  toggleFiled(v) {
    this.field[v ? "enable" : "disable"]({emitEvent: false});
  }
}
