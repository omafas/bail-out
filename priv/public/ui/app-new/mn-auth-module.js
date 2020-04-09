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
