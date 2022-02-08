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
mn.components.MnServersFailoverDialog =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnServersFailoverDialog, mn.core.MnEventableComponent);

    MnServersFailoverDialog.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-servers-failover.html",
        inputs: [
          "nodeStream"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnServersFailoverDialog.parameters = [
      ngb.NgbActiveModal,
      mn.services.MnServers,
      mn.services.MnForm
    ];

    MnServersFailoverDialog.prototype.ngOnInit = ngOnInit;
    MnServersFailoverDialog.prototype.unpackGetAudit = unpackGetAudit;
    MnServersFailoverDialog.prototype.packPostFailover = packPostFailover;

    return MnServersFailoverDialog;

    function ngOnInit() {
      this.node = this.getNodeStatuses.pipe(Rx.operators.withLatestFrom(this.nodeStream),
                                            Rx.operators.map(function (source) {
                                              return source[0][source[1].hostname];
                                            }));

      this.down = this.node.pipe(Rx.operators.map(R.pipe(R.propEq("status", "healthy"), R.not)));
      this.backFill = this.node.pipe(Rx.operators.map(R.pipe(R.prop("replication"),
                                                             R.lt(1))));

      this.form = this.mnFormService.create(this)
        .setFormGroup({
          confirmation: false,
          failOver: ""
        })
        .setUnpackPipe(Rx.pipe(Rx.operators.withLatestFrom(this.node),
                               Rx.operators.map(this.unpackGetAudit.bind(this))))
        .setPackPipe(Rx.pipe(Rx.operators.withLatestFrom(this.nodeStream),
                             Rx.operators.map(this.packPostFailover.bind(this))))
        .setPostRequest(this.postFailover)
        .setSource(this.getNodeStatuses)
        .confirmation504(mn.components.MnServersFailoverConfirmationDialog)
        .success(this.activeModal.close.bind(this.activeModal));
    }

    function MnServersFailoverDialog(activeModal, mnServersService, mnFormService) {
      mn.core.MnEventableComponent.call(this);

      this.activeModal = activeModal;
      this.mnFormService = mnFormService;
      this.getNodeStatuses = mnServersService.stream.getNodeStatuses;
      this.postFailover = mnServersService.stream.postFailover;
    }

    function packPostFailover(node) {
      return [this.form.group.get("failOver").value, node[1].otpNode, node[0]];
    }

    function unpackGetAudit(node) {
      return {
        failOver: ((node[1].status != "healthy") || !node[1].gracefulFailoverPossible) ?
          "failOver" : "startGracefulFailover",
        confirmation: node[1].replication >= 1
      };
    }

  })(window.rxjs);
