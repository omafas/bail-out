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
mn.components.MnServersItem =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnServersItem, mn.core.MnEventableComponent);

    MnServersItem.annotations = [
      new ng.core.Component({
        selector: "mn-servers-item",
        templateUrl: "app-new/mn-servers-item.html",
        inputs: [
          "node"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnServersItem.parameters = [
      mn.services.MnPermissions,
      window['@uirouter/angular'].UIRouter,
      mn.services.MnAdmin,
      mn.services.MnServers,
      mn.services.MnHelper,
      ng.common.DecimalPipe,
      mn.pipes.MnFormatQuantity,
      mn.services.MnTasks,
      mn.services.MnForm
    ];

    return MnServersItem;

    function MnServersItem(mnPermissionsService, uiRouter, mnAdminService, mnServersService, mnHelperService, ngDecimalPipe, mnFormatQuantityPipe, mnTasksService, mnFormService) {
      mn.core.MnEventableComponent.call(this);

      this.doCancelEjectNode = new Rx.Subject();
      this.nodesRead = mnPermissionsService.createPermissionStream("nodes!read");
      this.statsRead = mnPermissionsService.createPermissionStream("stats!read");
      this.tasksRead = mnPermissionsService.createPermissionStream("tasks!read");
      this.poolsWrite = mnPermissionsService.createPermissionStream("pools!write");
      this.nodeStream = this.mnOnChanges.pipe(Rx.operators.pluck("node", "currentValue"));
      this.bucketAnyStatsRead = mnPermissionsService.createPermissionStream("stats!read",
                                                                             ".");
      this.isRebalancing = mnAdminService.stream.isRebalancing;
      this.ejectedNodesByUI = mnServersService.stream.ejectedNodesByUI;

      this.isNodeInactiveFailed =
        this.nodeStream.pipe(Rx.operators.map(R.propEq("clusterMembership", "inactiveFailed")));
      this.isNodeInactiveAdded =
        this.nodeStream.pipe(Rx.operators.map(R.propEq("clusterMembership", "inactiveAdded")));
      this.isNodeUnhealthy =
        this.nodeStream.pipe(Rx.operators.map(R.propEq("status", "unhealthy")));
      this.isNodeRecoveryNone =
        this.nodeStream.pipe(Rx.operators.map(R.propEq("recoveryType", "none")));

      this.isRecoveryMode = mnTasksService.stream.isRecoveryMode;

      this.isKVNode = this.nodeStream.pipe(Rx.operators.map(R.pipe(R.prop("services"),
                                                                   R.indexOf("kv"),
                                                                   R.lt(-1))));
      this.runningTasksRebalance =
        mnTasksService.stream.tasksRebalance.pipe(
          Rx.operators.map(R.propEq("status", "running")));

      this.rebalanceTaskProgress =
        mnTasksService.stream.tasksRebalance.pipe(
          Rx.operators.withLatestFrom(this.nodeStream),
          Rx.operators.map(function (source) {
            var node = source[0] && source[0].perNode && source[0].perNode[source[1].otpNode];
            return node ? node.progress : 0;
          }));

      this.ramUsage =
        this.nodeStream.pipe(Rx.operators.map(function (node) {
          return (node.memoryTotal && node.memoryFree) ?
            ngDecimalPipe.transform((node.memoryTotal - node.memoryFree) /
                                     node.memoryTotal * 100, '1.0-1')  + "%" : "---";
        }));

      this.swapUsage =
        this.nodeStream.pipe(Rx.operators.map(function (node) {
          return node.systemStats.swap_used && node.systemStats.swap_total ?
            ngDecimalPipe.transform(node.systemStats.swap_used /
                                    node.systemStats.swap_total * 100, '1.0-1') + "%" : "---";
        }));

      this.cpuUsage =
        this.nodeStream.pipe(Rx.operators.map(function (node) {
          return node.systemStats.cpu_utilization_rate ?
            ngDecimalPipe.transform(node.systemStats.cpu_utilization_rate, '1.0-1') + "%" : "---";
        }));

      this.couchDiskUsage =
        this.nodeStream.pipe(Rx.operators.map(function (node) {
          var couchDiskUsage =
              node.interestingStats['couch_docs_actual_disk_size'] +
              node.interestingStats['couch_views_actual_disk_size'] +
              node.interestingStats['couch_spatial_disk_size'];
          return couchDiskUsage ? mnFormatQuantityPipe.transform(couchDiskUsage) : "---";
        }));

      this.hostname =
        Rx.combineLatest(
          mnServersService.stream.areAllPorts8091,
          this.nodeStream
        ).pipe(Rx.operators.map(function (source) {
          return source[0] ? source[1].hostname.replace(/:8091$/, '') : source[1].hostname;
        }));

      this.services =
        this.nodeStream.pipe(Rx.operators.map(R.pipe(R.prop("services"),
                                                     R.map(mnHelperService.getServiceVisibleName),
                                                     R.invoker(0, 'sort'))));

      this.doCancelEjectNode
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(mnServersService.removePendingEject.bind(mnServersService));

      this.ejectNode = mnFormService.create(this)
        .setPostRequest(mnServersService.stream.ejectNode)
        .hasNoHandler();

      this.postSetRecoveryType = mnFormService.create(this)
        .setPostRequest(mnServersService.stream.postSetRecoveryType)
        .hasNoHandler();

      this.postReFailover = mnFormService.create(this)
        .setPostRequest(mnServersService.stream.postReFailover)
        .hasNoHandler();

      this.detailsHashObserver =
        new mn.core.DetailsHashObserver(
          uiRouter,
          "app.admin.servers",
          "openedServers",
          this.mnOnDestroy,
          this.nodeStream.pipe(Rx.operators.pluck("hostname"))
        );

    }

  })(window.rxjs);
