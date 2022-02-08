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
mn.components.MnServersItemDetails =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnServersItemDetails, mn.core.MnEventableComponent);

    MnServersItemDetails.annotations = [
      new ng.core.Component({
        selector: "mn-servers-item-details",
        templateUrl: "app-new/mn-servers-item-details.html",
        inputs: [
          "node",
          "hostname"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnServersItemDetails.parameters = [
      mn.services.MnServers,
      mn.pipes.MnPrettyVersion,
      mn.services.MnTasks,
      mn.services.MnPermissions,
      mn.services.MnAdmin,
      ngb.NgbModal
    ];

    MnServersItemDetails.prototype.getBaseConfig = getBaseConfig;

    return MnServersItemDetails;

    function MnServersItemDetails(mnServersService, mnPrettyVersionPipe, mnTasksService, mnPermissionsService, mnAdminService, modalService) {
      mn.core.MnEventableComponent.call(this);

      this.onEjectServer = new Rx.Subject();
      this.onFailoverServer = new Rx.Subject();
      this.tasksRead = mnPermissionsService.createPermissionStream("tasks!read");
      this.poolsWrite = mnPermissionsService.createPermissionStream("pools!write");
      this.isRebalancing = mnAdminService.stream.isRebalancing;
      this.isRecoveryMode = mnTasksService.stream.isRecoveryMode;
      this.ejectedNodesByUI = mnServersService.stream.ejectedNodesByUI;

      var nodeStream = this.mnOnChanges.pipe(Rx.operators.pluck("node", "currentValue"));

      this.isNodeInactiveFailed =
        nodeStream.pipe(Rx.operators.map(R.propEq("clusterMembership", "inactiveFailed")));
      this.isNodeInactiveAdded =
        nodeStream.pipe(Rx.operators.map(R.propEq("clusterMembership", "inactiveAdded")));
      this.isNodeUnhealthy =
        nodeStream.pipe(Rx.operators.map(R.propEq("status", "unhealthy")));
      this.isNodeActive =
        nodeStream.pipe(Rx.operators.map(R.propEq("clusterMembership", "active")));

      this.isKVNode = nodeStream.pipe(Rx.operators.map(R.pipe(R.prop("services"),
                                                              R.indexOf("kv"),
                                                              R.lt(-1))));
      this.isLastActiveKVNode =
        Rx.combineLatest(
          mnServersService.stream.serviceSpecificActiveNodesWithoutEjected
            .pipe(Rx.operators.switchMap(R.prop("kv")),
                  Rx.operators.map(R.pipe(R.prop("length"), R.equals(1)))),
          this.isKVNode
        ).pipe(Rx.operators.map(R.all(R.equals(true))));

      this.details = nodeStream.pipe(
        Rx.operators.pluck("otpNode"),
        Rx.operators.switchMap(mnServersService.getNodes.bind(mnServersService)),
        mn.core.rxOperatorsShareReplay(1));

      var storageTotalsRam =
          this.details.pipe(Rx.operators.map(R.path(["storageTotals", "ram"])));
      var storageTotalsHdd =
          this.details.pipe(Rx.operators.map(R.path(["storageTotals", "hdd"])));

      this.storageTotalsRamTotal =
        storageTotalsRam.pipe(Rx.operators.map(R.prop("total")));
      this.storageTotalsHddTotal =
        storageTotalsHdd.pipe(Rx.operators.map(R.prop("total")));

      this.ramUsage =
        storageTotalsRam.pipe(Rx.operators.map(this.getBaseConfig.bind(this)),
                              mn.core.rxOperatorsShareReplay(1));
      this.hddUsage =
        storageTotalsHdd.pipe(Rx.operators.map(this.getBaseConfig.bind(this)),
                              mn.core.rxOperatorsShareReplay(1));
      this.prettyVersion =
        nodeStream.pipe(
          Rx.operators.pluck("version"),
          Rx.operators.map(mnPrettyVersionPipe.transform.bind(mnPrettyVersionPipe)));

      this.rebalanceDetails =
        mnTasksService.stream.tasksRebalance.pipe(
          Rx.operators.withLatestFrom(nodeStream),
          Rx.operators.map(function (source) {
            return source[0] && source[0].status === 'running' &&
              source[0].detailedProgress && source[0].detailedProgress.perNode &&
              source[0].detailedProgress.perNode[source[1].otpNode];
          }));

      this.warmUpTasks =
        mnTasksService.stream.tasksWarmingUp.pipe(
          Rx.operators.withLatestFrom(nodeStream),
          Rx.operators.map(function (source) {
            return source[0].filter(function (task) {
              return task.node === source[1].otpNode;
            });
          }));

      this.onFailoverServer
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function () {
          var ref = modalService.open(mn.components.MnServersFailoverDialog);
          ref.componentInstance.nodeStream = nodeStream;
        });

      this.onEjectServer
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function () {
          var ref = modalService.open(mn.components.MnServersEjectDialog);
          ref.componentInstance.nodeStream = nodeStream;
        });
    }

    function getBaseConfig(totals) {
      if (!totals) {
        return {};
      } else {
        return {
          bottomLeft: {
            name: 'remaining',
            value: totals.total - totals.used,
          },
          items: [{
            name: 'used',
            value: totals.usedByData
          }]
        };
      }
    }

  })(window.rxjs);
