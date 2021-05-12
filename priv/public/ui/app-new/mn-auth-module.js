/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.modules = mn.modules || {};
mn.modules.MnAuth =
  (function () {
    "use strict";

    MnAuthModule.annotations = [
      new ng.core.NgModule({
        declarations: [
          mn.components.MnAuth
        ],
        imports: [
          ng.platformBrowser.BrowserModule,
          ng.forms.ReactiveFormsModule,
          mn.modules.MnShared
        ],
        entryComponents: [
          mn.components.MnAuth
        ],
        providers: [
          mn.services.MnAuth,
          ng.forms.Validators,
          ng.common.Location
        ]
      })
    ];

    return MnAuthModule;

    function MnAuthModule() {
    }
  })();
