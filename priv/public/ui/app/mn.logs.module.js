/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {UIRouterModule} from '@uirouter/angular';

import {MnElementCraneModule} from './mn.element.crane.js';
import {MnLogsComponent} from "./mn.logs.component.js";

let logsStates = [
  {
    url: "/logs",
    abstract: true,
    name: "app.admin.logs",
    data: {
      permissions: "cluster.logs.read",
      title: "Logs"
    },
    views: {
      "main@app.admin": {
        component: MnLogsComponent
      }
    }
  }
]

export {MnLogsModule};

class MnLogsModule {
  static get annotations() {
    return [
      new NgModule({
        imports: [
          CommonModule,
          MnElementCraneModule,
          UIRouterModule.forChild({states: logsStates})
        ],
        declarations: [
          MnLogsComponent
        ]
      })
    ]
  }
}
