/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { NgModule } from '@angular/core';
import { UIRouterModule } from '@uirouter/angular';
import { ReactiveFormsModule } from '@angular/forms';

import { MnSharedModule } from './mn.shared.module.js';
import { MnSettingsAlertsComponent } from './mn.settings.alerts.component.js';


let alertsState = {
  url: "/alerts",
  name: "app.admin.settings.alerts",
  component: MnSettingsAlertsComponent,
  data: {
    permissions: "cluster.settings.read"
  }
};

export { MnSettingsAlertsModule };

class MnSettingsAlertsModule {
  static get annotations() { return [
    new NgModule({
      entryComponents: [],
      declarations: [
        MnSettingsAlertsComponent
      ],
      imports: [
        MnSharedModule,
        ReactiveFormsModule,
        UIRouterModule.forChild({ states: [alertsState] }),
      ]
    })
  ]}
}
