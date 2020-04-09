var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnNodeStorageConfig =
  (function (Rx) {
    "use strict";

    MnNodeStorageConfig.annotations = [
      new ng.core.Component({
        selector: "mn-node-storage-config",
        templateUrl: "app-new/mn-node-storage-config.html",
        inputs: [
          "group"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnNodeStorageConfig.parameters = [
      mn.services.MnWizard,
      mn.services.MnPools
    ];

    MnNodeStorageConfig.prototype.addCbasPathField = addCbasPathField;
    MnNodeStorageConfig.prototype.removeCbasPathField = removeCbasPathField;

    return MnNodeStorageConfig;

    function addCbasPathField() {
      var last = this.group.get('storage.cbas_path').length - 1;

      this.group
        .get('storage.cbas_path')
        .push(new ng.forms.FormControl(this.group.get('storage.cbas_path').value[last]));
    }

    function removeCbasPathField() {
      var last = this.group.get('storage.cbas_path').length - 1;
      this.group.get('storage.cbas_path').removeAt(last);
    }

    function MnNodeStorageConfig(mnWizardService, mnPoolsService) {
      this.focusFieldSubject = new Rx.BehaviorSubject(true);
      this.hostnameHttp = mnWizardService.stream.hostnameHttp;
      this.diskStorageHttp = mnWizardService.stream.diskStorageHttp;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
    }
  })(window.rxjs);
