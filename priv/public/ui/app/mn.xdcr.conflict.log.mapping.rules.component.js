/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {map} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {collectionDelimiter} from './mn.xdcr.service.js';
import template from "./mn.xdcr.conflict.log.mapping.rules.html";

export {MnXDCRConflictLogMappingRulesComponent};

class MnXDCRConflictLogMappingRulesComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-conflict-log-mapping-rules",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "isEditMode",
        "mappingRules",
        "mappingGroup"
      ]
    })
  ]}

  constructor() {
    super();
  }

  ngOnInit() {
    let kvToArray = (rules) => {
      const rulesToDisplay = [];
      if (rules.bucket || rules.collection) {
        rulesToDisplay.push(['All Conflicts', `${rules['bucket'] || ''}${collectionDelimiter}${rules['collection'] || ''}`]);
      }
      Object.keys(rules?.loggingRules || {}).forEach(rule => {
        const target = rules.loggingRules[rule];
        if (target) {
          if (target.bucket || target.collection) {
            rulesToDisplay.push([rule, `${target.bucket || ''}${collectionDelimiter}${target.collection || ''}`]);
          } else {
            if (typeof target === 'object' && Object.keys(target).length === 0) {
              rulesToDisplay.push([rule, '{}']);
            }
          }
        } else {
          // target is null
          rulesToDisplay.push([rule, 'null']);
        }
      });
      return rulesToDisplay;
    };

    this.mappingRulesKeys = this.mappingRules.pipe(map(kvToArray));
  }

  deleteRule(key) {
    let rules = this.mappingRules.getValue();
    if (key === 'All Conflicts') {
      rules.bucket = "";
      rules.collection = "";
      this.mappingGroup.rootControls.get('root_bucket').patchValue('');
      this.mappingGroup.rootControls.get('root_collection').patchValue('');
    } else {
      if (key.includes(collectionDelimiter)) {
        const [scopeName, collectionName] = key.split(collectionDelimiter);
        // set collection target as parent collection
        this.mappingGroup.ruleControls.scopes[scopeName].collections[collectionName].get(`collections_${collectionName}_target`).patchValue('parent');
      } else {
        // set scope target as default collection
        this.mappingGroup.ruleControls.scopes[key].get(`scopes_${key}_target`).patchValue('default');
      }
      delete rules.loggingRules[key];
    }
    this.mappingRules.next(rules);
  }
}
