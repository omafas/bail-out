/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core'
import {takeUntil, shareReplay, startWith} from 'rxjs/operators';
import {of} from 'rxjs';
import {FormBuilder} from '@angular/forms';

import {MnHelperService} from './mn.helper.service.js';
import {MnXDCRService, collectionDelimiter} from './mn.xdcr.service.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnKeyspaceSelectorService} from "./mn.keyspace.selector.service.js";
import template from "./mn.xdcr.conflict.log.mapping.controls.html";

export {MnXDCRConflictLogMappingControlsComponent};

class MnXDCRConflictLogMappingControlsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-conflict-log-mapping-controls",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item",
        "keyspace",
        "parent",
        "mappingGroup",
        "mappingRules"
      ]
    })
  ]}

  static get parameters() { return [
    MnHelperService,
    FormBuilder,
    MnXDCRService,
    MnKeyspaceSelectorService
  ]}

  constructor(mnHelperService, formBuilder, mnXDCRService, mnKeyspaceSelectorService) {
    super();
    this.toggler = mnHelperService.createToggle();
    this.filter = mnHelperService.createFilter(this);
    this.mnHelperService = mnHelperService;
    this.formBuilder = formBuilder;
    this.setMappingRule = mnXDCRService.setMappingRule;
    this.mnKeyspaceSelectorService = mnKeyspaceSelectorService;
  }

  ngOnInit() {
    this.isCollection = this.keyspace == "collections";

    if (this.parent === 'root') {
      this.group = this.mappingGroup.rootControls;
      this.controls = this.mappingGroup.rootControls.controls;
    } else {
      this.group = this.mappingGroup.ruleControls.scopes[this.item.name];
      this.controls = this.mappingGroup.ruleControls.scopes[this.item.name].collections;
    }
    this.scopeGroup = this.isCollection ?
      this.mappingGroup.ruleControls.scopes[this.item.name] : this.group;

    let rawItems = of(this.isCollection ? this.item.collections : this.item);

    this.filteredItems = rawItems.pipe(this.filter.pipe,
                                       shareReplay({refCount: true, bufferSize: 1}));

    if (this.parent === 'root') {
      this.group.get("root_bucket").valueChanges
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.setRootBucket.bind(this));

      this.group.get("root_collection").valueChanges
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.setRootCollection.bind(this));

      this.mnKeyspaceSelector =
        this.mnKeyspaceSelectorService.createCollectionSelector({
          component: this,
          steps: ["bucket", "scope", "collection"],
        });
      this.setDefaultRootValues();
      this.mnKeyspaceSelector.stream.result
        .pipe(takeUntil(this.mnOnDestroy))
        .subscribe(this.setDefaultValuesToGroup.bind(this));
    }

    if (!this.isCollection) {
      this.scopesPaginator =
        this.mnHelperService.createPagenator(this, this.filteredItems, "scopesPage");
    }


    this.customiseChildrenFieldName = this.parent === 'root' ? 'conflict_log_custom_scopes' : `conflict_log_custom_collections_${this.item.name}`;
    this.customiseChildren = this.group.get(this.customiseChildrenFieldName).valueChanges
      .pipe(startWith(this.group.get(this.customiseChildrenFieldName).value));
  }

  setRootBucket(value) {
    let rules = this.mappingRules.getValue();
    rules.bucket = value;
    this.mappingRules.next(rules);
  }

  setRootCollection(value) {
    let rules = this.mappingRules.getValue();
    rules.collection = value;
    this.mappingRules.next(rules);
    if (value === '') {
      // when the default conflict log collection is removed
      this.setDefaultRootValues();
    }
  }

  trackCollectionsBy(_, coll) {
    return coll.uid;
  }

  setDefaultRootValues() {
    const defaultTarget = this.group.get("root_collection").value;
    if (defaultTarget) {
      const [defaultScope, defaultCollection] = defaultTarget.split(collectionDelimiter);
      this.mnKeyspaceSelector.setKeyspace({bucket: this.group.get("root_bucket").value, scope: defaultScope, collection: defaultCollection});
    } else {
      this.mnKeyspaceSelector.reset();
    }
  }

  setDefaultValuesToGroup(result) {
    if (result.bucket) {
      if (this.parent === 'root') {
        this.group.get('root_bucket').patchValue(result.bucket.name);
      }
    }
    if (this.parent === 'root' && result.scope && result.collection) {
      this.group.get('root_collection').patchValue(`${result.scope.name}.${result.collection.name}`);
    }
  }
}
