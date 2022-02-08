/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {UIRouter} from '@uirouter/angular';
import {switchMap, filter, skip} from 'rxjs/operators';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnLogsCollectInfoService} from './mn.logs.collectInfo.service.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnTasksService} from './mn.tasks.service.js';

import {MnLogsCollectInfoStopCollectionComponent} from './mn.logs.collectInfo.stop.collection.component.js';

export { MnLogsCollectInfoResultComponent };

class MnLogsCollectInfoResultComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-logs-collect-info-result",
      templateUrl: "/ui/app/mn.logs.collectInfo.result.html",
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnLogsCollectInfoService,
    MnAdminService,
    UIRouter,
    MnTasksService,
    NgbModal
  ]}

  constructor(mnLogsCollectInfoService, mnAdminService, uiRouter, mnTasksService, modalService) {
    super();

    this.modalService = modalService;

    this.mnLogsCollectInfoService = mnLogsCollectInfoService;
    this.uiRouter = uiRouter;

    this.postCancelLogsCollection = mnLogsCollectInfoService.stream.postCancelLogsCollection;
    this.taskCollectInfo = mnTasksService.stream.taskCollectInfo;
    this.nodes = mnAdminService.stream.getNodes;
    this.nodesByStatus = mnLogsCollectInfoService.stream.nodesByCollectInfoStatus;
    this.nodesErrors = mnLogsCollectInfoService.stream.nodesErrors;

    this.disableStopCollection = this.postCancelLogsCollection.success
      .pipe(switchMap(() => this.taskCollectInfo),
            filter(taskCollectInfo => taskCollectInfo.status === 'running'));

    this.collectInfoLoading = this.taskCollectInfo.pipe(skip(1));
  }

  identifyNode(index, node) {
    return node.nodeName;
  }

  identifyNodeError(index, nodeError) {
    return nodeError.key;
  }

  startNewCollection() {
    this.uiRouter.stateService.go('app.admin.logs.collectInfo.form');
  }

  stopCollection() {
    this.modalService.open(MnLogsCollectInfoStopCollectionComponent);
  }
}
