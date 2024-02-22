/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgModule} from '@angular/core';
import {CommonModule, DatePipe} from '@angular/common';
import {UIRouterModule} from '@uirouter/angular';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';

import {MnSharedModule} from './mn.shared.module.js';
import {MnInputFilterModule} from './mn.input.filter.module.js';
import {MnTextExpanderModule} from './mn.text.expander.module.js';
import {MnLogsListComponent} from './mn.logs.list.component.js';

let states = [{
  url: '',
  name: 'app.admin.logs.list',
  component: MnLogsListComponent
}];

export {MnLogsListModule};

class MnLogsListModule {
  static get annotations() { return [
    new NgModule({
      declarations: [
        MnLogsListComponent
      ],
      imports: [
        UIRouterModule.forChild({
          states: states
        }),
        CommonModule,
        MnInputFilterModule,
        MnTextExpanderModule,
        MnSharedModule,
        NgbModule
      ],
      providers: [
        DatePipe
      ]
    })
  ]}
}
