/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '@angular/core';
import {UIRouterModule} from '@uirouter/angular';
import {ReactiveFormsModule} from '@angular/forms';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';

import {MnPipesModule} from './mn.pipes.module.js';
import {MnElementCraneModule} from './mn.element.crane.js';
import {MnCollectionsComponent} from './mn.collections.component.js';
import {MnCollectionsItemComponent} from './mn.collections.item.component.js';
import {MnCollectionsScopeComponent} from './mn.collections.scope.component.js';
import {MnCollectionsScopeDetailsComponent} from './mn.collections.scope.details.component.js';

import {MnSharedModule} from './mn.shared.module.js';
import {MnInputFilterModule} from './mn.input.filter.module.js';
import {MnSelectModule} from './mn.select.module.js';
import {MnCollectionsAddScopeComponent} from './mn.collections.add.scope.component.js';
import {MnCollectionsDeleteScopeComponent} from './mn.collections.delete.scope.component.js';
import {MnCollectionsAddItemComponent} from './mn.collections.add.item.component.js';
import {MnCollectionsDeleteItemComponent} from './mn.collections.delete.item.component.js';

let collectionsState = {
  url: '/collections?scopeDetails&scopesPage&collsPage',
  name: "app.admin.collections",
  data: {
    permissions: "cluster.bucket['.'].settings.read && cluster.collection['.:.:.'].collections.read",
    title: "Scopes & Collections",
    parent: {name: 'Buckets', link: 'app.admin.buckets'},
    compat: "atLeast70"
  },
  params: {
    scopeDetails: {
      array: true,
      dynamic: true
    },
    scopesPage: {
      value: {page:1, size:10},
      type: 'json',
      dynamic: true
    },
    collsPage: {
      value: {},
      type: 'json',
      dynamic: true
    }
  },
  views: {
    "main@app.admin": {
      component: MnCollectionsComponent
    }
  }
};

export { MnCollectionsModule };

class MnCollectionsModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
        MnCollectionsAddScopeComponent,
        MnCollectionsDeleteScopeComponent,
        MnCollectionsAddItemComponent,
        MnCollectionsDeleteItemComponent
      ],
      declarations: [
        MnCollectionsComponent,
        MnCollectionsItemComponent,
        MnCollectionsScopeComponent,
        MnCollectionsScopeDetailsComponent,
        MnCollectionsAddScopeComponent,
        MnCollectionsDeleteScopeComponent,
        MnCollectionsAddItemComponent,
        MnCollectionsDeleteItemComponent
      ],
      imports: [
        MnPipesModule,
        NgbModule,
        ReactiveFormsModule,
        MnSharedModule,
        MnInputFilterModule,
        MnSelectModule,
        MnElementCraneModule,
        UIRouterModule.forChild({ states: [collectionsState] })
      ]
    })
  ]}
}
