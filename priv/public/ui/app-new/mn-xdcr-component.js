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
mn.components.MnXDCR =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnXDCR, mn.core.MnEventableComponent);

    MnXDCR.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-xdcr.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnXDCR.parameters = [
      mn.services.MnPermissions,
      mn.services.MnXDCR,
      mn.services.MnPools,
      mn.services.MnTasks,
      ngb.NgbModal
    ];

    MnXDCR.prototype.generateStatisticsLink = generateStatisticsLink;
    MnXDCR.prototype.trackByFn = trackByFn;
    MnXDCR.prototype.tasksTrackByFn = tasksTrackByFn;

    return MnXDCR;

    function MnXDCR(mnPermissionsService, mnXDCRService, mnPoolsService, mnTasksService, modalService) {
      mn.core.MnEventableComponent.call(this);

      this.onAddReference = new Rx.Subject();
      this.onAddReference
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function (reference) {
          var ref = modalService.open(mn.components.MnXDCRAddReference);
          if (reference) {
            ref.componentInstance.reference = reference;
          }
        });

      this.onDeleteReference = new Rx.Subject();
      this.onDeleteReference
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function (reference) {
          var ref = modalService.open(mn.components.MnXDCRDeleteReference);
          ref.componentInstance.reference = reference;
        });

      this.onAddReplication = new Rx.Subject();
      this.onAddReplication
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(function (reference) {
          modalService.open(mn.components.MnXDCRAddReplication);
        });

      this.tasksXDCR = mnTasksService.stream.tasksXDCR
      this.isEnterprise = mnPoolsService.stream.isEnterprise;

      this.xdcrRemoteClustersWrite =
        mnPermissionsService.createPermissionStream("xdcr.remote_clusters!write");

      this.xdcrRemoteClustersRead =
        mnPermissionsService.createPermissionStream("xdcr.remote_clusters!read");

      this.xdcrSettingsRead =
        mnPermissionsService.createPermissionStream("xdcr.settings!read");

      this.xdcrSettingsWrite =
        mnPermissionsService.createPermissionStream("xdcr.settings!write");

      this.xdcrBucketAnyWrite =
        mnPermissionsService.createPermissionStream("xdcr!write", ".");

      this.references = mnXDCRService.stream.getRemoteClustersFiltered;
    }

    function generateStatisticsLink(row) {
      return window.location.protocol + '//' +
        row.hostname + '/index.html#/analytics/?statsHostname=' +
        (encodeURIComponent(row.hostname))
    }

    function trackByFn(row) {
      return row.name;
    }

    function tasksTrackByFn(row) {
      return row.id;
    }

  })(window.rxjs);
