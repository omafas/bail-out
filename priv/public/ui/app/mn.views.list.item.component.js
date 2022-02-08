/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Component, ChangeDetectionStrategy } from '@angular/core';
import { UIRouter } from '@uirouter/angular';
import { MnLifeCycleHooksToStream } from './mn.core.js';
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { map, pluck, shareReplay, takeUntil,
         filter, distinctUntilChanged,
         mapTo, switchMap, withLatestFrom, tap, catchError } from 'rxjs/operators';
import { combineLatest, timer, merge, Subject } from "rxjs";
import { is, isEmpty } from 'ramda';

import { MnTasksService } from './mn.tasks.service.js';
import { MnViewsListService } from './mn.views.list.service.js';

import { MnPermissions } from './ajs.upgraded.providers.js';

import { MnViewsCreateDialogComponent } from './mn.views.create.dialog.component.js';
import { MnViewsDeleteDialogDdocComponent } from './mn.views.delete.dialog.ddoc.component.js';
import { MnViewsDeleteDialogViewComponent } from './mn.views.delete.dialog.view.component.js';
import { MnViewsCopyDialogComponent } from './mn.views.copy.dialog.component.js';
import { MnViewsConfirmOverrideDialogComponent } from './mn.views.confirm.override.dialog.component.js';
import template from "./mn.views.list.item.html";

export { MnViewsListItemComponent };

class MnViewsListItemComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-views-list-item",
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        'row',
        'viewId',
        'ddocumentId'
      ]
    })
  ]}

  static get parameters() { return [
    UIRouter,
    MnPermissions,
    MnTasksService,
    MnViewsListService,
    NgbModal
  ]}

  constructor(uiRouter, mnPermissions, mnTasksService, mnViewsListService, modalService) {
    super();

    this.permissions = mnPermissions.stream;

    this.uiRouter = uiRouter;

    this.mnTasksService = mnTasksService;
    this.mnViewsListService = mnViewsListService;

    this.getDdocUrl = mnViewsListService.getDdocUrl;
    this.getDdoc = mnViewsListService.stream.getDdoc;
    this.postDdoc = mnViewsListService.stream.postDdoc;
    this.removeDesignPrefix = mnViewsListService.removeDesignPrefix;
    this.addDevPrefix = mnViewsListService.addDevPrefix;

    this.commonBucket = uiRouter.globals.params$
      .pipe(pluck('commonBucket'));

    this.type = uiRouter.globals.params$
      .pipe(pluck('type'));

    this.showEditOrShow = this.type
      .pipe(map(this.showEditOrShow.bind(this)));

    this.hasCompactPermission =
      combineLatest(this.permissions,
                    this.commonBucket)
      .pipe(map(this.hasCompactPermission.bind(this)));

    this.hasTasksReadPermission = this.permissions
      .pipe(map(this.hasTasksReadPermission.bind(this)));

    this.hasWritePermission =
      combineLatest(this.permissions,
                    this.commonBucket)
      .pipe(map(this.hasWritePermission.bind(this)));

    this.isDevelopmentViews =
      combineLatest(this.hasWritePermission,
                    this.type)
      .pipe(map(this.isDevelopmentViews.bind(this)));

    this.addViewDialog = new Subject();
    this.addViewDialog
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(data => {
        let ref = modalService.open(MnViewsCreateDialogComponent);
        ref.componentInstance.ddocName = data.doc.meta.id;
        ref.componentInstance.views = data.doc.json.views;
      });

    this.copyDialog = new Subject();
    this.copyDialog
      .pipe(takeUntil(this.mnOnDestroy),
            withLatestFrom(this.commonBucket))
      .subscribe(([row, bucket]) => {
        let url = this.getDdocUrl([bucket, this.addDevPrefix(row.doc.meta.id)]);
        let ref = modalService.open(MnViewsCopyDialogComponent);
        ref.componentInstance.json = row.doc.json;
        ref.componentInstance.url = url;
      });

    this.clickCopy= new Subject();
    this.clickCopy
      .pipe(takeUntil(this.mnOnDestroy),
            withLatestFrom(this.commonBucket),
            map(([row, bucket]) => {
              let url = this.getDdocUrl([bucket, this.addDevPrefix(row.doc.meta.id)])
              return { url, json: row.doc.json }
            }),
            switchMap(data => this.mnViewsListService.postDdoc({ url: data.url, json: data.json })))
      .subscribe(result => {
        if (JSON.parse(result).ok === true) {
          this.uiRouter.stateService.go('.', { type: 'development' });
        }
      })

    this.deleteDdocDialog = new Subject();
    this.deleteDdocDialog
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(data => {
        let ref = modalService.open(MnViewsDeleteDialogDdocComponent);
        ref.componentInstance.ddocName = data.doc.meta.id;
      });

    this.deleteViewDialog = new Subject();
    this.deleteViewDialog
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(data => {
        let ref = modalService.open(MnViewsDeleteDialogViewComponent);
        ref.componentInstance.ddocName = data.doc.meta.id;
        ref.componentInstance.views = data.doc.json.views;
        ref.componentInstance.viewName = data.key;
      });

    this.overrideDialog = new Subject();
    this.overrideDialog
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(data =>  {
        let ref = modalService.open(MnViewsConfirmOverrideDialogComponent);
        ref.componentInstance.ddocUrl = data.url;
        ref.componentInstance.json = data.json;
      })

    this.compactionTask =
      combineLatest(this.mnTasksService.stream.tasksCompactionByView,
                    this.commonBucket)
      .pipe(map(v => this.mnViewsListService.getCompactionTask(v)),
            distinctUntilChanged());

    this.compactionProgress = this.compactionTask
      .pipe(map(v => this.mnViewsListService.prepareCompactionProgressText(v)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.hasProgressAndTasksReadPermission =
      combineLatest(this.compactionProgress,
                    this.hasTasksReadPermission)
      .pipe(map(([progress, tasksRead]) => !!progress && !!tasksRead));

    this.showCompactBtn =
      combineLatest(this.compactionTask,
                    this.commonBucket,
                    this.permissions)
      .pipe(map(v => this.mnViewsListService.showCompactBtn(v)));

    this.clickCompact = new Subject();
    let postCompact = this.clickCompact
      .pipe(map(this.packCompactURL.bind(this)),
            switchMap((url) => this.mnViewsListService.postCompact(url)),
            shareReplay({refCount: true, bufferSize: 1}));

    let after10secsCompact = postCompact
      .pipe(switchMap(() => timer(10000)));

    this.disableCompactBtn =
      merge(postCompact.pipe(mapTo(true)),
            after10secsCompact.pipe(mapTo(false)));

    this.showCancelCompactBtn =
      combineLatest(this.compactionTask,
                    this.commonBucket,
                    this.permissions)
      .pipe(map(v => this.mnViewsListService.showCancelCompactBtn(v)));

    let cancelCompactURL = this.compactionTask
      .pipe(filter(v => !!v),
            pluck('cancelURI'),
            distinctUntilChanged(),
            shareReplay({refCount: true, bufferSize: 1}));

    this.clickCancelCompact = new Subject();
    let postCancelCompact =
      combineLatest(this.clickCancelCompact,
                    cancelCompactURL)
      .pipe(switchMap(([, url]) => this.mnViewsListService.postCompact(url)),
            shareReplay({refCount: true, bufferSize: 1}));

    let after10secsCancelCompact = postCancelCompact
      .pipe(switchMap(() => timer(10000)));

    this.disableCancelCompactBtn =
      merge(postCancelCompact.pipe(mapTo(true)),
            after10secsCancelCompact.pipe(mapTo(false)));

    this.createDialog = new Subject();
    this.createDialog
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(() => modalService.open(MnViewsCreateDialogComponent));


    this.clickPublish = new Subject();
    this.clickPublish
      .pipe(withLatestFrom(this.commonBucket),
            switchMap(([row, bucket]) => {
              let url = this.getDdocUrl([bucket, this.removeDesignPrefix(row.doc.meta.id), '_design/']);
              let json = { url, json: row.doc.json };
              return this.mnViewsListService.getDdoc(url)
                .pipe(tap(() => this.overrideDialog.next(json)),
                      catchError(() => this.mnViewsListService.postDdoc(json)))
            }))
    .subscribe(result => {
      if (!is(Object, result)) {
        this.uiRouter.stateService.go('.', { type: 'production'});
      }})
  }

  hasWritePermission([permissions, bucket]) {
    return permissions.cluster.bucket[bucket] &&
      permissions.cluster.bucket[bucket].views.write;
  }

  hasCompactPermission([permissions, bucket]) {
    return permissions.cluster.bucket[bucket] &&
      permissions.cluster.bucket[bucket].compact;
  }

  hasTasksReadPermission(permissions) {
    return permissions.cluster.tasks.read;
  }

  showPublishButton(row) {
    return isEmpty(row.doc.json.views);
  }

  showEditOrShow(type) {
    return type == 'development' ? 'Edit' : 'Show';
  }

  isDevelopmentViews([hasPermission, type]) {
    return hasPermission && type === "development";
  }

  packCompactURL(row) {
    return row.controllers.compact;
  }

  getCancelCompactURL(compactionTask) {
    return { cancelCompactURL: compactionTask && compactionTask.cancelURI };
  }

  packCancelCompactURL() {
    return this.clickCancelCompactForm.group.getRawValue().cancelCompactURL;
  }
}
