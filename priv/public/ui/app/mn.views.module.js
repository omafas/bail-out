/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { NgModule } from '@angular/core';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { UIRouterModule } from "@uirouter/angular";
import { ReactiveFormsModule } from '@angular/forms';

import { MnCodeMirrorEditorModule } from './mn.codemirror.editor.module.js';
import { MnElementCraneModule } from "./mn.element.crane.js";
import { MnPipesModule } from "./mn.pipes.module.js";
import { MnSelectModule } from './mn.select.module.js';
import { MnSharedModule } from './mn.shared.module.js';

import { MnViewsComponent } from './mn.views.component.js';
import { MnViewsConfirmOverrideDialogComponent } from './mn.views.confirm.override.dialog.component.js';
import { MnViewsCopyDialogComponent } from './mn.views.copy.dialog.component.js';
import { MnViewsCreateDialogComponent } from './mn.views.create.dialog.component.js';
import { MnViewsDeleteDialogDdocComponent } from './mn.views.delete.dialog.ddoc.component.js';
import { MnViewsDeleteDialogViewComponent } from './mn.views.delete.dialog.view.component.js';
import { MnViewsEditingComponent } from './mn.views.editing.component.js';
import { MnViewsEditingResultComponent } from './mn.views.editing.result.component.js';
import { MnViewsFilterComponent } from './mn.views.filter.component.js';
import { MnViewsListComponent } from './mn.views.list.component.js';
import { MnViewsListItemComponent } from './mn.views.list.item.component.js';

let viewsState = {
  url: "/views",
  name: "app.admin.views",
  abstract: true,
  data: {
    title: "Views",
    permissions: "cluster.bucket['.'].settings.read && cluster.bucket['.'].views.read"
  },
  views: {
    "main@app.admin": {
      component: MnViewsComponent
    }
  }
};

let viewsListState = {
  url: "?type",
  name: "app.admin.views.list",
  params: {
    type:  {
      value: 'development'
    }
  },
  views: {
    "main@app.admin": {
      component: MnViewsListComponent
    }
  }
};

let viewsEditingState = {
  url: '/:ddocumentId?viewId&type',
  name: 'app.admin.views.editing',
  abstract: true,
  views: {
    "main@app.admin": {
      component: MnViewsEditingComponent
    }
  },
  data: {
    parent: {
      name: 'Views',
      link: 'app.admin.views.list'
    },
    title: "Views Editing"
  }
};

let viewsEditingResultState = {
  url: '?{full_set:bool}&{pageNumber:int}',
  name: 'app.admin.views.editing.result',
  params: {
    full_set: {
      value: false,
      dynamic: true
    },
    pageNumber: {
      value: null,
      dynamic: true
    }
  },
};

export {MnViewsModule};

class MnViewsModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
        MnViewsConfirmOverrideDialogComponent,
        MnViewsCopyDialogComponent,
        MnViewsCreateDialogComponent,
        MnViewsDeleteDialogDdocComponent,
        MnViewsDeleteDialogViewComponent
      ],
      declarations: [
        MnViewsComponent,
        MnViewsConfirmOverrideDialogComponent,
        MnViewsCopyDialogComponent,
        MnViewsCreateDialogComponent,
        MnViewsDeleteDialogDdocComponent,
        MnViewsDeleteDialogViewComponent,
        MnViewsEditingComponent,
        MnViewsEditingResultComponent,
        MnViewsFilterComponent,
        MnViewsListComponent,
        MnViewsListItemComponent
      ],
      imports: [
        MnSharedModule,
        NgbModule,
        MnElementCraneModule,
        UIRouterModule.forChild({ states: [
          viewsState,
          viewsListState,
          viewsEditingState,
          viewsEditingResultState
        ] }),
        ReactiveFormsModule,
        MnSelectModule,
        MnPipesModule,
        MnCodeMirrorEditorModule
      ]
    })
  ]}
}
