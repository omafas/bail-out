/*
Copyright 2020-Present Couchbase, Inc.

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

export {MnXDCRAddRepMappingRulesComponent};

class MnXDCRAddRepMappingRulesComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-xdcr-mapping-rules",
      templateUrl: "app/mn.xdcr.add.rep.mapping.rules.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "isEditMode",
        "isMigrationMode",
        "isExplicitMappingMode",
        "explicitMappingRules",
        "explicitMappingMigrationRules",
        "explicitMappingGroup"
      ]
    })
  ]}

  constructor() {
    super();
  }

  ngOnInit() {
    let kvToArray = (rules) => Object.keys(rules).sort().map(from => [from, rules[from]]);

    this.explicitMappingRulesKeys = this.explicitMappingRules.pipe(map(kvToArray));
    this.explicitMappingMigrationRulesKeys = this.explicitMappingMigrationRules.pipe(map(kvToArray));
  }

  delExplicitMappingRules(key) {
    let scopeCollection = key.split(collectionDelimiter);
    let rules = this.explicitMappingRules.getValue();
    if (scopeCollection.length == 2) {
      this.explicitMappingGroup.collections[scopeCollection[0]]
        .flags.get(scopeCollection[1]).setValue(rules[key] == null);
    } else {
      if (rules[key]) {
        this.explicitMappingGroup.scopes.root.flags.get(scopeCollection[0]).setValue(false);
        Object.keys(rules).forEach(mapKey => {
          if (mapKey.startsWith(scopeCollection[0])) {
            delete rules[mapKey];
          }
        });
      } else {
        this.explicitMappingGroup.scopes.root.flags.get(scopeCollection[0]).setValue(true);
      }
    }
    delete rules[key];
    this.explicitMappingRules.next(rules);
  }

  delExplicitMappingMigrationRules(key) {
    let rules = this.explicitMappingMigrationRules.getValue();
    delete rules[key];
    this.explicitMappingMigrationRules.next(rules);
  }
}
