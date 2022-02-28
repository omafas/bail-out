/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {ChangeDetectionStrategy, Component} from '@angular/core';
import {Subject} from 'rxjs';
import {FormBuilder, FormControl, Validators} from '@angular/forms';
import {map, takeUntil, switchMap, filter, pairwise, startWith} from 'rxjs/operators';
import {UIRouter} from '@uirouter/angular';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';

import {MnLogsCollectInfoService} from './mn.logs.collectInfo.service.js';
import {MnAdminService} from "./mn.admin.service.js";
import {MnPoolsService} from "./mn.pools.service.js";
import {MnFormService} from "./mn.form.service.js";
import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {MnTasksService} from './mn.tasks.service.js';
import {MnClusterSummaryDialogComponent} from './mn.cluster.summary.dialog.component.js';
import {MnLogsCollectInfoStopCollectionComponent} from './mn.logs.collectInfo.stop.collection.component.js';
import template from "./mn.logs.collectInfo.form.html";

export {MnLogsCollectInfoFormComponent};

class MnLogsCollectInfoFormComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-logs-collect-info-form",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnLogsCollectInfoService,
    MnAdminService,
    MnFormService,
    FormBuilder,
    MnPoolsService,
    UIRouter,
    NgbModal,
    MnTasksService
  ]}

  constructor(mnLogsCollectInfoService, mnAdminService, mnFormService, formBuilder, mnPoolsService, uiRouter, modalService, mnTasksService) {
    super();

    this.taskCollectInfo = mnTasksService.stream.taskCollectInfo;
    this.postCancelLogsCollection = mnLogsCollectInfoService.stream.postCancelLogsCollection;

    this.mnLogsCollectInfoService = mnLogsCollectInfoService;
    this.modalService = modalService;
    this.postRequest = mnLogsCollectInfoService.stream.startLogsCollection;

    this.formData = mnLogsCollectInfoService.stream.formData;

    this.form = mnFormService.create(this)
      .setSource(this.formData)
      .setFormGroup(formBuilder.group({
        nodes: formBuilder.group({}, {validators: this.nodesCustomValidator.bind(this)}),
        logs: formBuilder.group({
          logRedactionLevel: null,
          enableTmpDir: null,
          tmpDir: [null, [Validators.required]],
          enableLogDir: null,
          logDir: [null, [Validators.required]]
        }),
        upload: formBuilder.group({
          upload: null,
          uploadHost: [null, [Validators.required]],
          customer: [null, [Validators.required]],
          uploadProxy: null,
          bypassReachabilityChecks: null,
          ticket: null
        })
      }))
      .setPackPipe(map(this.packData.bind(this)))
      .setPostRequest(this.postRequest)
      .success(() => uiRouter.stateService.go('app.admin.logs.collectInfo.result'));


    this.groups = mnLogsCollectInfoService.stream.groups;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.compatVersion55 = mnAdminService.stream.compatVersion55;
    this.clickGetClusterInfo = new Subject();
    this.clickGetClusterInfo
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.showClusterInfoDialog.bind(this));


    mnAdminService.stream.nodesByOtp
      .pipe(startWith([{}, {}]),
            pairwise(),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.addNodes.bind(this));

    this.disableStopCollection = this.postCancelLogsCollection.success
      .pipe(switchMap(() => this.taskCollectInfo),
            filter(taskCollectInfo => taskCollectInfo.status === 'running'));

    this.form.group.get('logs.enableTmpDir').valueChanges
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableField.bind(this, 'logs.tmpDir'));

    this.form.group.get('logs.enableLogDir').valueChanges.pipe(takeUntil(this.mnOnDestroy)).subscribe(this.maybeDisableField.bind(this, 'logs.logDir'));

    this.form.group.get('upload.upload').valueChanges.pipe(takeUntil(this.mnOnDestroy)).subscribe(this.maybeDisableField.bind(this, 'upload.customer'));

    this.form.group.get('upload.upload').valueChanges.pipe(takeUntil(this.mnOnDestroy)).subscribe(this.maybeDisableField.bind(this, 'upload.uploadHost'));
  }

  showClusterInfoDialog() {
    this.modalService.open(MnClusterSummaryDialogComponent);
  }

  packData() {
    let packedData = {};

    let nodes = this.form.group.controls.nodes.getRawValue();
    let logs = this.form.group.controls.logs.getRawValue();
    let upload = this.form.group.controls.upload.getRawValue();

    packedData.nodes = Object.keys(nodes).filter(node => nodes[node]).join(",");

    if (logs.logRedactionLevel) {
      packedData.logRedactionLevel = logs.logRedactionLevel;
    }

    if (logs.enableTmpDir) {
      packedData.tmpDir = logs.tmpDir;
    }
    if (logs.enableLogDir) {
      packedData.logDir = logs.logDir;
    }

    if (upload.upload) {
      packedData.uploadHost = upload.uploadHost;
      packedData.customer = upload.customer;
      packedData.ticket = upload.ticket || '';
      if (upload.bypassReachabilityChecks) {
        packedData.bypassReachabilityChecks = upload.bypassReachabilityChecks;
      }
      if (upload.uploadProxy) {
        packedData.uploadProxy = upload.uploadProxy;
      }
    }

    return packedData;
  }

  stopCollection() {
    this.modalService.open(MnLogsCollectInfoStopCollectionComponent);
  }

  addNodes([nodesByOtpOld, nodesByOtpNew]) {
    Object.keys(nodesByOtpNew).forEach(nodeOtp => {
      let control = this.form.group.get('nodes').controls[nodeOtp] || new FormControl();
      control[nodesByOtpNew[nodeOtp][0].status === 'unhealthy' ? "disable": "enable"]();

      if (nodesByOtpOld[nodeOtp]) {
        /* at the end of forEach nodesByOtpOld will contain the nodes that were removed (nodesByOtpOld - nodesByOtpNew)
           so we delete from nodesByOtpOld the nodes which are both in nodesByOtpOld and nodesByOtpNew */
        delete nodesByOtpOld[nodeOtp];
      } else {
        // new node added (in nodesByOtpNew, but not in nodesByOtpOld)
        this.form.group.get('nodes').addControl(nodeOtp, control);
      }
    });

    // look for removed nodes
    Object.keys(nodesByOtpOld).forEach(nodeOtp => {
      this.form.group.get('nodes').removeControl(nodeOtp);
    });
  }

  nodesCustomValidator(formGroup) {
    let nodes = formGroup.getRawValue();
    let invalid = !Object.values(nodes).some(v => v);
    return invalid ? {nodes: true} : null;
  }

  isFieldValid(formGroup, toggleField, field) {
    let groupValue = formGroup.getRawValue();
    return !(groupValue[toggleField] && !groupValue[field]);
  }

  maybeDisableField(field, enable) {
    this.form.group.get(field)[enable ? "enable": "disable"]();
  }
}
