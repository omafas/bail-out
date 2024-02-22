/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';
import {downgradeComponent,
        downgradeInjectable,
        setAngularJSGlobal} from '@angular/upgrade/static';
setAngularJSGlobal(angular);

import {MnKeyspaceSelectorComponent} from "./mn.keyspace.selector.component.js";
import {MnKeyspaceSelectorService} from './mn.keyspace.selector.service.js';

export default "mnKeyspaceSelectorDowngradeModule";

angular
  .module('mnKeyspaceSelectorDowngradeModule', [])
  .factory('mnKeyspaceSelectorServiceDowngrade', downgradeInjectable(MnKeyspaceSelectorService))
  .directive('mnKeyspaceSelectorDowngrade', downgradeComponent({
    component: MnKeyspaceSelectorComponent
  }));
