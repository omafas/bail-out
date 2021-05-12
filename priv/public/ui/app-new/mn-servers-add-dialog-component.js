/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnServersAddDialog =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnServersAddDialog, mn.core.MnEventableComponent);

    MnServersAddDialog.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-servers-add-dialog.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnServersAddDialog.parameters = [
      ngb.NgbActiveModal,
      mn.services.MnServers,
      mn.services.MnGroups,
      mn.services.MnForm,
      mn.services.MnPools,
      mn.services.MnWizard
    ];

    MnServersAddDialog.prototype.pack = pack;
    MnServersAddDialog.prototype.unpack = unpack;
    MnServersAddDialog.prototype.generateServicesGroup = generateServicesGroup;

    return MnServersAddDialog;

    function MnServersAddDialog(activeModal, mnServersService, mnGroupsService, mnFormService, mnPoolsService, mnWizardService) {
      mn.core.MnEventableComponent.call(this);
      this.activeModal = activeModal;
      this.getServerGroups = mnGroupsService.stream.getServerGroups;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.focusFieldSubject = new Rx.BehaviorSubject();
      this.getServicesValues = mnWizardService.getServicesValues;
      this.addNodeError = mnServersService.stream.addNode.error;

      this.form = mnFormService.create(this);
      this.form
        .setFormGroup({
          group: '',
          hostname: ['', ng.forms.Validators.required],
          user: 'Administrator',
          password: '',
          services: this.form.builder.group({
            // flag:
          })
        })
        .setUnpackPipe(Rx.operators.map(this.unpack.bind(this)))
        .setPackPipe(Rx.operators.map(this.pack.bind(this)))
        .setSource(mnGroupsService.stream.getServerGroups)
        .setPostRequest(mnServersService.stream.addNode)
        .clearErrors()
        .successMessage("Server added successfully!");

      mnServersService.stream.addNode.success
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function () {
          activeModal.close();
        });

      mnPoolsService.stream.mnServices
        .pipe(Rx.operators.first())
        .subscribe(this.generateServicesGroup.bind(this));
    }

    function generateServicesGroup(services) {
      var group = new ng.forms.FormGroup(services.reduce(function (acc, name) {
        acc[name] = new ng.forms.FormControl(true);
        return acc;
      }, {}));
      this.form.group.get("services").addControl("flag", group);
    }

    function unpack(serverGroups) {
      return {
        group: serverGroups.groups[0]
      };
    }

    function pack() {
      var source = Object.assign({}, this.form.group.value);
      var uri = source.group.addNodeURI;
      delete source.group;
      source.services =
        this.getServicesValues(this.form.group.get("services.flag")).join(",");
      return [uri, source];
    }

  })(window.rxjs);
