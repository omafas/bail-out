/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.modules = mn.modules || {};
mn.modules.MnShared =
  (function () {
    "use strict";

    MnSharedModule.annotations = [
      new ng.core.NgModule({
        declarations: [
          mn.directives.MnFocus,
          mn.components.MnAutoCompactionForm,
          mn.components.MnPeriod,
          mn.components.MnServicesConfig,
          mn.components.MnSearch,
          mn.components.MnSearchField
        ],
        exports: [
          mn.components.MnServicesConfig,
          mn.directives.MnFocus,
          mn.components.MnAutoCompactionForm,
          mn.components.MnSearch,
          mn.components.MnSearchField
        ],
        imports: [
          ng.forms.ReactiveFormsModule,
          ng.platformBrowser.BrowserModule,
          ngb.NgbModule,
        ]
      })
    ];

    return MnSharedModule;

    function MnSharedModule() {
    }
  })();
