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
mn.components.MnServers =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnServers, mn.core.MnEventableComponent);

    MnServers.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-servers.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnServers.parameters = [
      mn.services.MnAdmin,
      mn.services.MnHelper,
      mn.services.MnPermissions,
      mn.services.MnSettings,
      mn.services.MnForm,
      mn.services.MnTasks,
      mn.services.MnServers,
      mn.services.MnPools,
      ngb.NgbModal
    ];

    MnServers.prototype.trackByFn = trackByFn

    return MnServers;

    function trackByFn(node) {
      return node.otpNode;
    }

    function MnServers(mnAdminService, mnHelperService, mnPermissionsService, mnSettingsService, mnFormService, mnTasksService, mnServersService, mnPoolsService, modalService) {
      mn.core.MnEventableComponent.call(this)

      this.formHelper = new ng.forms.FormGroup({
        searchTerm: new ng.forms.FormControl("")
      });

      this.onAddServer = new Rx.Subject();
      this.onSortByClick = new Rx.BehaviorSubject("hostname");
      this.tasksRead = mnPermissionsService.createPermissionStream("tasks!read");
      this.poolsWrite = mnPermissionsService.createPermissionStream("pools!write");
      this.settingsWrite = mnPermissionsService.createPermissionStream("settings!write");
      this.settingsRead = mnPermissionsService.createPermissionStream("settings!read");
      this.serverGroupsRead = mnPermissionsService.createPermissionStream("server_groups!read");
      this.bucketRecoveryWrite = mnPermissionsService.createPermissionStream("recovery!write",
                                                                             bucketName);
      this.failoverWarnings = mnAdminService.stream.failoverWarnings;
      this.isRebalancing = mnAdminService.stream.isRebalancing;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.isNotCompatMode = mnAdminService.stream.isNotCompatMode;
      this.prettyClusterCompat = mnAdminService.stream.prettyClusterCompat;
      this.getAutoFailover = mnSettingsService.stream.getAutoFailover;
      this.isLoadingSampleBucket = mnTasksService.stream.isLoadingSampleBucket;
      this.activeNodesWithoutEjected = mnServersService.stream.activeNodesWithoutEjected;
      this.isRecoveryMode = mnTasksService.stream.isRecoveryMode;
      this.ejectedNodesLength = mnServersService.stream.ejectedNodesLength;
      this.isBalanced = mnAdminService.stream.isBalanced;
      this.isSubtypeGraceful = mnTasksService.stream.isSubtypeGraceful;
      this.toggleFailoverWarning = mnServersService.stream.toggleFailoverWarning;
      var bucketName = mnTasksService.stream.tasksRecovery.pipe(Rx.operators.pluck("bucket"));
      this.nodes = mnServersService.stream.nodes
        .pipe(mnHelperService.sortByStream(this.onSortByClick));

      this.resetAutofaiover = mnFormService.create(this)
        .setPostRequest(new mn.core.MnPostGroupHttp({
          postAutoFailoverReset: mnSettingsService.stream.postAutoFailoverReset,
          postAutoReprovisionReset: mnSettingsService.stream.postAutoReprovisionReset
        }).addSuccess().addError())
        .successMessage("Auto-failover quota reset successfully!")
        .errorMessage('Unable to reset Auto-failover quota!');

      this.postRebalance = mnFormService.create(this)
        .setPackPipe(
          Rx.operators.withLatestFrom(
            mnServersService.stream.nodes,
            mnServersService.stream.ejectedNodesByUI))
        .setUnpackErrorPipe(mnServersService.humanReadableRebalanceErrorsPipe)
        .setPostRequest(mnServersService.stream.postRebalance)
        .errorMessage();

      this.stopRebalance = mnFormService.create(this)
        .setPostRequest(mnServersService.stream.stopRebalance)
        .confirmation504(mn.components.MnServersStopRebalanceDialog);

      mnServersService.stream.updateEjectedNodes
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(mnServersService.stream.ejectedNodesByUI);

      this.onAddServer
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function () {
          modalService.open(mn.components.MnServersAddDialog);
        });

    }
  })(window.rxjs);
