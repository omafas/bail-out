/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core'
import {takeUntil, withLatestFrom, shareReplay} from 'rxjs/operators';
import {of, combineLatest} from 'rxjs';
import {FormBuilder} from '@angular/forms';

import {MnHelperService} from './mn.helper.service.js';
import {MnXDCRService, collectionDelimiter} from './mn.xdcr.service.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';

export {MnXDCRAddRepMappingControlsComponent};

class MnXDCRAddRepMappingControlsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-add-rep-mapping-controls",
      templateUrl: "app/mn.xdcr.add.rep.mapping.controls.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item",
        "keyspace",
        "parent",
        "explicitMappingGroup",
        "explicitMappingRules"
      ]
    })
  ]}


  static get parameters() { return [
    MnHelperService,
    FormBuilder,
    MnXDCRService
  ]}

  constructor(mnHelperService, formBuilder, mnXDCRService) {
    super();
    this.toggler = mnHelperService.createToggle();
    this.filter = mnHelperService.createFilter(this);
    this.mnHelperService = mnHelperService;
    this.formBuilder = formBuilder;
    this.setMappingRule = mnXDCRService.setMappingRule;
  }

  ngOnInit() {
    this.isCollection = this.keyspace == "collections";

    if (!this.explicitMappingGroup[this.keyspace][this.parent]) {
      this.explicitMappingGroup[this.keyspace][this.parent] = {
        flags: this.formBuilder.group({}),
        fields: this.formBuilder.group({})
      };
    }

    if (!this.explicitMappingGroup[this.keyspace + "Controls"][this.parent]) {
      this.explicitMappingGroup[this.keyspace + "Controls"][this.parent] =
        this.formBuilder.group({
          checkAll: this.formBuilder.control({
            value: false,
            disabled: false
          })
        });
    }

    this.group = this.explicitMappingGroup[this.keyspace][this.parent];
    this.controls = this.explicitMappingGroup[this.keyspace + "Controls"][this.parent];

    this.scopeGroup = this.isCollection ?
      this.explicitMappingGroup.scopes.root : this.group;

    this.scopeControlsGroup = this.isCollection ?
      this.explicitMappingGroup.scopesControls.root : this.controls;


    let rawItems = of(this.isCollection ? this.item.collections : this.item);
    let doToggle = this.isCollection ? this.toggleCheckAllCollections:this.toggleCheckAllScopes;

    this.filteredItems = rawItems.pipe(this.filter.pipe,
                                       shareReplay({refCount: true, bufferSize: 1}));

    this.controls.get("checkAll").valueChanges
      .pipe(withLatestFrom(this.filteredItems),
            takeUntil(this.mnOnDestroy))
      .subscribe(doToggle.bind(this));

    if (!this.isCollection) {
      this.scopesPaginator =
        this.mnHelperService.createPagenator(this, this.filteredItems, "scopesPage");
    }

    combineLatest(this.explicitMappingRules,
                  this.filteredItems)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(([rules, items]) => {
        let hasUncheckedItem =
            items.find(v => !rules[this.isCollection ? (this.parent +  "." + v.name) : v.name]);
        this.controls.get("checkAll").setValue(!hasUncheckedItem, {
          emitEvent: false
        });
      });
  }

  toggleCheckAllScopes([checkAll, items]) {
    let rules = this.explicitMappingRules.getValue();

    items.forEach(item => {
      let targetScopeField = this.scopeGroup.fields.get(item.name);
      let sourceScope = item.name;
      let targetScope =  targetScopeField ? targetScopeField.value : sourceScope;

      this.setMappingRule(checkAll, sourceScope, targetScope, rules);
    });

    this.toggleFlags([checkAll, items]);

    this.explicitMappingRules.next(rules);
  }

  toggleCheckAllCollections([checkAll, items], sourceScope, rules) {
    sourceScope = sourceScope || this.parent;
    rules = rules || this.explicitMappingRules.getValue();

    let collectionGroup = this.explicitMappingGroup.collections[sourceScope];
    let targetScopeField = this.scopeGroup.fields.get(sourceScope);
    let targetScope =  targetScopeField ? targetScopeField.value : sourceScope;

    items.forEach(item => {
      let targetCollField = collectionGroup && collectionGroup.fields.get(item.name);
      let sourceCollection = item.name;
      let source = sourceScope + collectionDelimiter + sourceCollection;
      let targetCollection = targetCollField ? targetCollField.value : sourceCollection;
      let target = targetScope + collectionDelimiter + targetCollection;

      this.setMappingRule(checkAll, source, target, rules);
    });

    this.toggleFlags([checkAll, items]);
  }

  toggleFlags([value, items]) {
    this.group.flags.patchValue(items.reduce((acc, item) => {
      acc[item.name] = value;
      return acc;
    }, {}));
  }

  trackCollectionsBy(_, coll) {
    return coll.uid;
  }
}
