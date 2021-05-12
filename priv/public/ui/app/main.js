/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {NgZone} from '/ui/web_modules/@angular/core.js';
import {platformBrowserDynamic} from '/ui/web_modules/@angular/platform-browser-dynamic.js';
import {MnAppModule} from './mn.app.module.js';
import {UIRouter} from '/ui/web_modules/@uirouter/core.js';

platformBrowserDynamic().bootstrapModule(MnAppModule).then(platformRef => {
  const urlService = platformRef.injector.get(UIRouter).urlService;
  // Instruct UIRouter to listen to URL changes
  function startUIRouter() {
    urlService.listen();
    urlService.sync();
  }
  platformRef.injector.get(NgZone).run(startUIRouter);
});
