/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core'
import {takeUntil, startWith} from 'rxjs/operators';
import {FormBuilder} from '@angular/forms';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnXDCRService, collectionDelimiter} from "./mn.xdcr.service.js";
import {MnKeyspaceSelectorService} from "./mn.keyspace.selector.service.js";
import template from "./mn.xdcr.conflict.log.mapping.item.html";

export {MnXDCRConflictLogMappingItemComponent};

class MnXDCRConflictLogMappingItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-conflict-log-mapping-item",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "item",
        "mappingGroup",
        "parent",
        "mappingRules",
        "keyspace",
      ]
    })
  ]}

  static get parameters() { return [
    FormBuilder,
    MnXDCRService,
    MnKeyspaceSelectorService
  ]}

  constructor(formBuilder, mnXDCRService, mnKeyspaceSelectorService) {
    super();
    this.formBuilder = formBuilder;
    this.setMappingRule = mnXDCRService.setMappingRule;
    this.mnKeyspaceSelectorService = mnKeyspaceSelectorService;
    this.collectionDelimiter = collectionDelimiter;
  }

  ngOnInit() {
    this.isCollection = this.keyspace == "collections";

    if (this.parent === 'root') {
      this.group = this.mappingGroup.rootControls;
    } else {
      if (this.isCollection) {
        this.group = this.mappingGroup.ruleControls.scopes[this.parent].collections[this.item.name];
      } else {
        this.group = this.mappingGroup.ruleControls.scopes[this.parent];
      }
    }
    this.controls = this.group.controls;
    this.targetFieldName = `${this.keyspace}_${this.item.name}_target`;
    this.targetFieldValue = this.group.get(this.targetFieldName).valueChanges
      .pipe(startWith(this.group.get(this.targetFieldName).value));

    this.targetFieldValue
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.changeTarget.bind(this));

    this.mnKeyspaceSelector =
      this.mnKeyspaceSelectorService.createCollectionSelector({
        component: this,
        steps: ["bucket", "scope", "collection"],
      });
    this.setKeyspaceSelectorValue();

    this.mnKeyspaceSelector.stream.result
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe((result) => {
        this.setCustomRule(result);
        this.setFormFields(result);
    });

    this.group.get('bucket').valueChanges.pipe(takeUntil(this.mnOnDestroy)).subscribe((value) => this.setRule(value, 'bucket'));
    this.group.get('collection').valueChanges.pipe(takeUntil(this.mnOnDestroy)).subscribe((value) => this.setRule(value, 'collection'));

    this.targetValues = this.keyspace === 'scopes' ? ['default', 'custom', 'null'] : ['default', 'parent', 'custom', 'null'];
    this.targetLabels = this.keyspace === 'scopes' ? ['Default collection', 'Custom collection', 'Do not log'] : ['Default collection', 'Parent collection', 'Custom collection', 'Do not log'];

    this.rootBucket = this.mappingGroup.rootControls.get('root_bucket').valueChanges
      .pipe(startWith(this.mappingGroup.rootControls.get('root_bucket').value));
    this.rootCollection = this.mappingGroup.rootControls.get('root_collection').valueChanges
      .pipe(startWith(this.mappingGroup.rootControls.get('root_collection').value));

    if (this.keyspace === 'collections') {
      this.parentBucket = this.mappingGroup.ruleControls.scopes[this.parent].get('bucket').valueChanges
        .pipe(startWith(this.mappingGroup.ruleControls.scopes[this.parent].get('bucket').value));

      this.parentBucket.subscribe(() => {});
      this.parentCollection = this.mappingGroup.ruleControls.scopes[this.parent].get('collection').valueChanges
        .pipe(startWith(this.mappingGroup.ruleControls.scopes[this.parent].get('collection').value));
    }
  }

  setRule(value, property) {
    let rules = this.mappingRules.getValue();
    rules.loggingRules = rules.loggingRules || {};
    const ruleName = this.getRuleName();
    rules.loggingRules[ruleName] = rules.loggingRules[ruleName] || {};
    if (property) {
      rules.loggingRules[ruleName][property] = value;
    } else {
      rules.loggingRules[ruleName] = value;
    }

    this.mappingRules.next(rules);
  }

  deleteRule() {
    let rules = this.mappingRules.getValue();
    rules.loggingRules = rules.loggingRules || {};
    delete rules.loggingRules[this.getRuleName()];
    this.mappingRules.next(rules);
  }

  getRuleName() {
    if (this.isCollection) {
      return `${this.parent}${collectionDelimiter}${this.item.name}`;
    }
    return this.item.name;
  }

  changeTarget(value) {
    this.setKeyspaceSelectorValue(value);
    if (value === 'null') {
      this.setRule(null);
    }

    if (value === 'default' && this.keyspace === 'collections') {
      this.setRule({});
    }

    if (value === 'parent') {
      // remove the rule for this collection
      this.deleteRule();

    }
  }

  setKeyspaceSelectorValue(value) {
    if (!value) {
      value = this.group.get(this.targetFieldName).value;
    }

    switch(value) {
      case 'custom':
        let customBucket = this.controls?.bucket?.value;
        let [customScope, customCollection] = this.controls?.collection?.value?.split(collectionDelimiter);
        if (customBucket && customCollection && customScope) {
          this.mnKeyspaceSelector?.setKeyspace({bucket: customBucket, scope: customScope, collection: customCollection});
        } else {
          this.mnKeyspaceSelector?.reset();
        }
        break;
      default:
        this.mnKeyspaceSelector?.reset();
        break;
    }
  }

  setCustomRule(result) {
    if (result.bucket) {
      this.setRule(result.bucket.name, 'bucket');
    }
    if ( result.scope && result.collection) {
      this.setRule(`${result.scope.name}${collectionDelimiter}${result.collection.name}`, 'collection');
    }
  }

  setFormFields(result) {
    if (result.bucket) {
      this.group.get('bucket').patchValue(result.bucket.name);
    }
    if (result.scope && result.collection) {
      this.group.get('collection').patchValue(`${result.scope.name}${collectionDelimiter}${result.collection.name}`);
    }
  }
}
