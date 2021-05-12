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
mn.components.MnXDCRItem =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnXDCRItem, mn.core.MnEventableComponent);

    MnXDCRItem.annotations = [
      new ng.core.Component({
        selector: "mn-xdcr-item",
        templateUrl: "app-new/mn-xdcr-item.html",
        selector: '[mn-xdcr-item]',
        inputs: [
          "item"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnXDCRItem.parameters = [
      mn.services.MnPermissions,
      mn.services.MnPools,
      mn.services.MnXDCR,
      mn.services.MnForm,
      mn.services.MnTasks,
      ngb.NgbModal
    ];

    MnXDCRItem.prototype.getStatus = getStatus;

    return MnXDCRItem;

    function MnXDCRItem(mnPermissionsService, mnPoolsService, mnXDCRService, mnFormService, mnTasksService, modalService) {
      mn.core.MnEventableComponent.call(this);

      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.itemStream = this.mnOnChanges.pipe(Rx.operators.pluck("item", "currentValue"));
      var name = this.itemStream.pipe(Rx.operators.pluck("source"));
      this.xdcrExecute = mnPermissionsService.createPermissionStream("xdcr!execute", name);
      this.xdcrWrite = mnPermissionsService.createPermissionStream("xdcr!write", name);
      this.xdcrSettingsWrite = mnPermissionsService.createPermissionStream("xdcr.settings!write");

      this.onDeleteReplication = new Rx.Subject();
      this.onDeleteReplication
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function (reference) {
          var ref = modalService.open(mn.components.MnXDCRDelete);
          ref.componentInstance.replication = this.item;
        }.bind(this));

      this.onEditReplication = new Rx.Subject();
      this.onEditReplication
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function (reference) {
          var ref = modalService.open(mn.components.MnXDCREdit);
          ref.componentInstance.replication = this.item;
        }.bind(this));

      this.pausePlayReplication = mnFormService.create(this)
        .setPackPipe(Rx.operators.map(function (row) {
          return [row.id, {pauseRequested: row.status !== 'paused'}];
        }))
        .setPostRequest(mnXDCRService.stream.postPausePlayReplication)
        .success(function () {
          mnTasksService.stream.updateTasks.next();
        });

      this.version = this.itemStream.pipe(
        Rx.operators.pluck("replicationType"),
        Rx.operators.map(
          R.cond([
            [R.equals("xmem"), R.always("2")],
            [R.equals("capi"), R.always("1")],
            [R.T, R.always("unknown")]
          ])),
        Rx.operators.map(R.concat("Version ")));

      this.to = Rx.combineLatest(this.itemStream, mnXDCRService.stream.getRemoteClustersByUUID)
        .pipe(Rx.operators.map(function (source) {
          if (!source[0]) {
            return
          }
          var uuid = source[0].id.split("/")[0];
          var target = source[1][uuid][0];
          return (('bucket "' + source[0].target.split('buckets/')[1] + '" on cluster "') +
            (!target ? "unknown" : !target.deleted ? target.name : target.hostname) + '"');
        }));

      this.status = this.itemStream
        .pipe(Rx.operators.map(this.getStatus('pause', 'play', 'spinner').bind(this)),
              mn.core.rxOperatorsShareReplay(1));

      this.humanStatus = this.itemStream
        .pipe(Rx.operators.map(this.getStatus('Replicating', 'Paused', 'Starting Up').bind(this)),
              mn.core.rxOperatorsShareReplay(1));

    }

    function getStatus(running, paused, defaults) {
      return function (row) {
        if (row.pauseRequested && row.status != 'paused') {
          return defaults;
        } else {
          switch (row.status) {
          case 'running': return running;
          case 'paused': return paused;
          default: return defaults;
          }
        }
      }
    }

  })(window.rxjs);
