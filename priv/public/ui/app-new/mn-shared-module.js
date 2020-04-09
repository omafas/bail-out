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
