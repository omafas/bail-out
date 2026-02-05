/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/
import angular from 'angular';

import {NgZone} from '@angular/core';
import {platformBrowserDynamic} from '@angular/platform-browser-dynamic';
import {UIRouter} from '@uirouter/core';

import {MnAppModule} from './mn.app.module.js';

platformBrowserDynamic().bootstrapModule(MnAppModule, {preserveWhitespaces: true}).then(platformRef => {
  const urlService = platformRef.injector.get(UIRouter).urlService;
  // Instruct UIRouter to listen to URL changes
  function startUIRouter() {
    urlService.listen();
    urlService.sync();
  }
  platformRef.injector.get(NgZone).run(startUIRouter);
});
