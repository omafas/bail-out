/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

import {MnElementCargoComponent,
        MnElementDepotComponent} from "/ui/app/mn.element.crane.js";

import {downgradeComponent} from "/ui/web_modules/@angular/upgrade/static.js";

export default "mnElementCrane";

angular
  .module('mnElementCrane', [])
  .directive('mnElementDepot', downgradeComponent({component: MnElementDepotComponent}))
  .directive('mnElementCargo', downgradeComponent({component: MnElementCargoComponent}));
