/*
Copyright 2025-Present Couchbase, Inc.

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

import {MnTimezoneDetailsComponent} from './mn.timezone.details.component.js';
import {MnTimezoneDetailsService} from './mn.timezone.details.service.js';

export default "mnTimezoneDetailsDowngradeModule";

angular
  .module('mnTimezoneDetailsDowngradeModule', [])
  .factory('mnTimezoneDetailsServiceDowngrade', downgradeInjectable(MnTimezoneDetailsService))
  .directive('mnTimezoneDetailsDowngrade', downgradeComponent({
    component: MnTimezoneDetailsComponent
  }));
