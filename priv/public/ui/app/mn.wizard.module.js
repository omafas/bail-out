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
import {HttpClientJsonpModule, HttpClientModule, HTTP_INTERCEPTORS} from '@angular/common/http';
import {CommonModule} from '@angular/common';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';

import {MnWizardComponent} from './mn.wizard.component.js';
import {MnWizardWelcomeComponent} from './mn.wizard.welcome.component.js';
import {MnWizardSetupNewClusterComponent} from './mn.wizard.setup.new.cluster.component.js';
import {MnWizardNewClusterConfigComponent} from './mn.wizard.new.cluster.config.component.js';
import {MnWizardTermsAndConditionsComponent} from './mn.wizard.terms.and.conditions.component.js';
import {MnWizardJoinClusterComponent} from './mn.wizard.join.cluster.component.js';
import {MnSharedModule} from './mn.shared.module.js';
import {MnPipesModule} from './mn.pipes.module.js';

import {MnNodeStorageConfigComponent} from './mn.node.storage.config.component.js';
import {MnHostnameConfigComponent} from './mn.hostname.config.component.js';
import {MnServicesConfigComponent} from './mn.services.config.component.js';
import {MnStorageModeComponent} from './mn.storage.mode.component.js';
import {MnPathFieldComponent} from './mn.path.field.component.js';

import {MnHttpInterceptor} from './mn.http.interceptor.js';

let states = [{
  name: 'app.wizard',
  abstract: true,
  component: MnWizardComponent
}, {
  name: 'app.wizard.welcome',
  component: MnWizardWelcomeComponent
}, {
  name: "app.wizard.setupNewCluster",
  component: MnWizardSetupNewClusterComponent
}, {
  name: 'app.wizard.joinCluster',
  component: MnWizardJoinClusterComponent
}, {
  name:'app.wizard.termsAndConditions',
  component: MnWizardTermsAndConditionsComponent
}, {
  name: 'app.wizard.clusterConfiguration',
  component: MnWizardNewClusterConfigComponent
}];



export {MnWizardModule};

class MnWizardModule {
  static get annotations() { return [
    new NgModule({
      declarations: [
        MnNodeStorageConfigComponent,
        MnHostnameConfigComponent,
        MnServicesConfigComponent,
        MnStorageModeComponent,
        MnPathFieldComponent,

        MnWizardComponent,
        MnWizardWelcomeComponent,
        MnWizardNewClusterConfigComponent,
        MnWizardSetupNewClusterComponent,
        MnWizardTermsAndConditionsComponent,
        MnWizardJoinClusterComponent
      ],
      providers: [
        {
          provide: HTTP_INTERCEPTORS,
          useClass: MnHttpInterceptor,
          multi: true
        }
      ],
      imports: [
        NgbModule,
        UIRouterModule.forChild({
          states: states
        }),
        CommonModule,
        ReactiveFormsModule,
        MnSharedModule,
        MnPipesModule,
        HttpClientModule,
        HttpClientJsonpModule
      ]
    })
  ]}
}
