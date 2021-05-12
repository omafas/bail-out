/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import {downgradeComponent,
        downgradeInjectable} from"/ui/web_modules/@angular/upgrade/static.js";
import {MnKeyspaceSelectorComponent} from "/ui/app/mn.keyspace.selector.component.js";
import {MnCollectionsService} from '/ui/app/mn.collections.service.js';
import {Subject, of} from "/ui/web_modules/rxjs.js";

export default "mnKeyspaceSelectorDowngradeModule";

angular
  .module('mnKeyspaceSelectorDowngradeModule', [])
  .factory('mnCollectionsServiceDowngrade', downgradeInjectable(MnCollectionsService))
  .directive('mnKeyspaceSelectorDowngrade', downgradeComponent({
    component: MnKeyspaceSelectorComponent
  }));
