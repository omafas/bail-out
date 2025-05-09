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

import { MnElementCraneModule } from "./mn.element.crane.js";
import { MnSharedModule } from './mn.shared.module.js';
import { MnInputFilterModule } from './mn.input.filter.module.js';
import { MnPipesModule } from './mn.pipes.module.js';
import { MnKeyspaceSelectorModule } from "./mn.keyspace.selector.module.js";
import { MnSelectModule } from './mn.select.module.js';

import { MnXDCRComponent } from "./mn.xdcr.component.js";
import { MnXDCRItemComponent } from "./mn.xdcr.item.component.js";
import { MnXDCRItemDetailsComponent,
         MnReplicationStatus } from "./mn.xdcr.item.details.component.js";
import { MnXDCRRefItemComponent } from "./mn.xdcr.ref.item.component.js";
import { MnXDCRIncomingComponent } from "./mn.xdcr.incoming.component.js";
import { MnXDCRIncomingDetailsComponent } from "./mn.xdcr.incoming.details.component.js";
import { MnXDCRIncomingReplicationDetailsComponent } from "./mn.xdcr.incoming.replication.details.component.js";
import { MnXDCRIncomingReplicationSettingsComponent } from "./mn.xdcr.incoming.replication.settings.component.js";
import { MnXDCRMobileSettingComponent } from "./mn.xdcr.mobile.setting.component.js";

import { MnXDCRAddRefComponent } from "./mn.xdcr.add.ref.component.js";
import { MnXDCRAddRepComponent } from "./mn.xdcr.add.rep.component.js";
import { MnXDCRAddRepScopeComponent } from "./mn.xdcr.add.rep.scope.component.js";
import { MnXDCRAddRepMappingControlsComponent } from "./mn.xdcr.add.rep.mapping.controls.component.js";
import { MnXDCRAddRepMappingItemComponent } from "./mn.xdcr.add.rep.mapping.item.component.js";
import { MnXDCRAddRepMappingRulesComponent } from "./mn.xdcr.add.rep.mapping.rules.component.js";
import { MnXDCRAddRepMappingComponent } from "./mn.xdcr.add.rep.mapping.component.js";
import { MnXDCRDeleteRefComponent } from "./mn.xdcr.delete.ref.component.js";
import { MnXDCRDeleteRepComponent } from "./mn.xdcr.delete.rep.component.js";
import { MnXDCRFilterComponent } from "./mn.xdcr.filter.component.js";
import { MnXDCRSettingsComponent } from "./mn.xdcr.settings.component.js";
import { MnXDCREditRepComponent } from "./mn.xdcr.edit.rep.component.js";
import { MnXDCRErrorsComponent } from "./mn.xdcr.errors.component.js";
import { MnXDCRRepMessageComponent } from "./mn.xdcr.rep.message.component.js";
import { MnXDCRConflictLogComponent } from "./mn.xdcr.conflict.log.component.js";
import { MnXDCRConflictLogMappingControlsComponent } from "./mn.xdcr.conflict.log.mapping.controls.component.js"
import { MnXDCRConflictLogScopeComponent } from "./mn.xdcr.conflict.log.scope.component.js"
import { MnXDCRConflictLogMappingItemComponent } from "./mn.xdcr.conflict.log.mapping.item.component.js";
import { MnXDCRConflictLogMappingRulesComponent } from "./mn.xdcr.conflict.log.mapping.rules.component.js";

import { MnDetailStatsDirective } from "./ajs.upgraded.components.js";

let XDCRState = {
  url: '/replications',
  name: "app.admin.replications",
  data: {
    permissions: "cluster.tasks.read || cluster.xdcr.remote_clusters.read",
    title: "XDCR Replications"
  },
  params: {
    xdcrDetails: {
      array: true,
      dynamic: true
    },
    xdcrIncomingDetails: {
      array: true,
      dynamic: true
    },
    xdcrIncomingReplicationDetails: {
      array: true,
      dynamic: true
    }
  },
  views: {
    "main@app.admin": {
      component: MnXDCRComponent
    }
  }
};

let AddXDCRState = {
  name: "app.admin.replications.add",
  data: {
    title: "XDCR Add Replication"
  },
  params: {
    scopesPage: {
      value: {page:1, size:15},
      type: 'json',
      dynamic: true
    },
  },
  views: {
    "main@app.admin": {
      component: MnXDCRAddRepComponent
    }
  }
};

let EditXDCRState = {
  name: "app.admin.replications.edit",
  data: {
    title: "XDCR Edit Replication"
  },
  params: {
    scopesPage: {
      value: {page:1, size:15},
      type: 'json',
      dynamic: true
    },
    item: null
  },
  views: {
    "main@app.admin": {
      component: MnXDCREditRepComponent
    }
  }
};

export { MnXDCRModule };

class MnXDCRModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [
        MnXDCRAddRefComponent,
        MnXDCRDeleteRefComponent,
        MnXDCRDeleteRepComponent,
        MnXDCREditRepComponent,
        MnXDCRErrorsComponent,
        MnXDCRIncomingReplicationSettingsComponent
      ],
      declarations: [
        MnDetailStatsDirective,

        MnXDCRComponent,
        MnXDCRItemComponent,
        MnXDCRItemDetailsComponent,
        MnXDCRRefItemComponent,
        MnXDCRAddRefComponent,
        MnXDCRAddRepComponent,
        MnXDCRAddRepMappingItemComponent,
        MnXDCRAddRepMappingRulesComponent,
        MnXDCRAddRepMappingComponent,
        MnXDCRAddRepScopeComponent,
        MnXDCRAddRepMappingControlsComponent,
        MnXDCRDeleteRefComponent,
        MnXDCRDeleteRepComponent,
        MnXDCREditRepComponent,
        MnXDCRSettingsComponent,
        MnXDCRFilterComponent,
        MnXDCRErrorsComponent,
        MnReplicationStatus,
        MnXDCRRepMessageComponent,
        MnXDCRIncomingComponent,
        MnXDCRIncomingDetailsComponent,
        MnXDCRIncomingReplicationDetailsComponent,
        MnXDCRIncomingReplicationSettingsComponent,
        MnXDCRConflictLogComponent,
        MnXDCRConflictLogMappingControlsComponent,
        MnXDCRConflictLogScopeComponent,
        MnXDCRConflictLogMappingItemComponent,
        MnXDCRConflictLogMappingRulesComponent,
        MnXDCRMobileSettingComponent
      ],
      imports: [
        NgbModule,
        ReactiveFormsModule,

        MnInputFilterModule,
        MnElementCraneModule,
        MnKeyspaceSelectorModule,

        MnSharedModule,
        MnPipesModule,
        MnSelectModule,
        UIRouterModule.forChild({ states: [XDCRState, AddXDCRState, EditXDCRState] })
      ]
    })
  ]}
}
