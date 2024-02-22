/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';
import {ClipboardService} from 'ngx-clipboard';
import {takeUntil, filter, map} from 'rxjs/operators';
import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnAlerts} from './ajs.upgraded.providers.js';
import {MnLogsCollectInfoService} from './mn.logs.collectInfo.service.js';
import template from "./mn.cluster.summary.dialog.html";

export {MnClusterSummaryDialogComponent};

class MnClusterSummaryDialogComponent extends MnLifeCycleHooksToStream {

  static get annotations() { return [
    new Component({
      selector: "mn-cluster-summary-dialog",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    NgbActiveModal,
    ClipboardService,
    MnAlerts,
    MnLogsCollectInfoService
  ]}

  constructor(activeModal, clipboardService, mnAlerts, mnLogsCollectInfoService) {
    super();

    this.activeModal = activeModal;
    this.mnAlerts = mnAlerts;

    this.clusterInfo = mnLogsCollectInfoService.stream.clusterInfo
      .pipe(map(v => JSON.stringify(v, null, 2)));

    clipboardService.copyResponse$
      .pipe(filter(response => response.isSuccess),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.handleSuccessMessage.bind(this));

    clipboardService.copyResponse$
      .pipe(filter(response => !response.isSuccess),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.handleErrorMessage.bind(this));
  }

  handleSuccessMessage() {
    this.activeModal.close();
    this.mnAlerts.formatAndSetAlerts('Text copied successfully!',
                                     'success',
                                     2500);
  }

  handleErrorMessage() {
    this.activeModal.close();
    this.mnAlerts.formatAndSetAlerts('Unable to copy text!',
                                     'error',
                                     2500);
  }
}
