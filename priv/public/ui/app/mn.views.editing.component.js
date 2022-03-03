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
import { map, tap, pluck, takeUntil, switchMap, withLatestFrom, catchError } from 'rxjs/operators';
import { pipe, combineLatest, of, Subject, merge } from "rxjs";
import { is } from 'ramda';
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { MnPermissions } from './ajs.upgraded.providers.js';
import { MnLifeCycleHooksToStream } from './mn.core.js';

import { MnDocumentsService } from './mn.documents.service.js';
import { MnViewsListService } from './mn.views.list.service.js';
import { MnHelperService } from './mn.helper.service.js';
import { MnFormService } from './mn.form.service.js';
import { QwDialogService } from '../../_p/ui/query/angular-directives/qw.dialog.service.js';

import { MnViewsCreateDialogComponent } from './mn.views.create.dialog.component.js';

import js_beautify from "js-beautify";
import template from "./mn.views.editing.html";

export { MnViewsEditingComponent };

class MnViewsEditingComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    UIRouter,
    MnPermissions,
    MnDocumentsService,
    MnViewsListService,
    QwDialogService,
    MnFormService,
    MnHelperService,
    NgbModal
  ]}

  constructor(uiRouter, mnPermissions, mnDocumentsService, mnViewsListService, qwDialogService, mnFormService, mnHelperService, modalService) {
    super();

    this.permissions = mnPermissions.stream;
    this.qwDialogService = qwDialogService;
    this.mnViewsListService = mnViewsListService;
    this.mnDocumentsService = mnDocumentsService;
    this.mnHelperService = mnHelperService;

    this.postDdoc = mnViewsListService.stream.postDdoc;
    this.viewId = uiRouter.globals.params$.pipe(pluck('viewId'));
    this.ddocumentId = uiRouter.globals.params$.pipe(pluck('ddocumentId'));
    this.commonBucket = uiRouter.globals.params$.pipe(pluck('commonBucket'));
    this.type = uiRouter.globals.params$.pipe(pluck('type'));

    this.toggle = mnHelperService.createToggle(false);

    this.saveForm = mnFormService.create(this)
      .setFormGroup({
        docJson: null,
        metaJson: null,
        mapJson: null,
        reduceJson: null
      })
      .setPackPipe(pipe(
        withLatestFrom(this.mnViewsListService.stream.getDdoc,
                       this.commonBucket,
                       this.ddocumentId,
                       this.viewId),
        map(this.packSaveData.bind(this))))
      .setPostRequest(this.postDdoc)
      .successMessage("View saved successfully!")
      .clearErrors();

    this.randomForm = mnFormService.create(this)
      .setFormGroup({})
      .hasNoPostRequest();

    this.hasReadPermission =
      combineLatest(this.permissions,
                    this.commonBucket)
      .pipe(tap(([permissions, bucket]) => {
        if (!permissions.cluster.bucket[bucket]) {
          return uiRouter.stateService.go('app.admin.views.list', { type: 'development' })}}),
            map(this.hasReadPermission.bind(this)));

    this.hasWritePermission =
      combineLatest(this.permissions,
                    this.commonBucket)
      .pipe(map(this.hasWritePermission.bind(this)));

    this.isDevelopmentViews = this.type
      .pipe(map(type => type == 'development'));

    this.randomDocument =
      merge(this.mnDocumentsService.stream.getRandomDocument,
            this.mnDocumentsService.stream.getDocument);

    this.randomDocument.pipe(takeUntil(this.mnOnDestroy)).subscribe(doc => {
      if (doc.json) {
        this.saveForm.group.get('docJson').setValue(js_beautify(doc.json));
        this.saveForm.group.get('metaJson').setValue(js_beautify(JSON.stringify(doc.meta)));
      }
    });

    this.documentDoesNotExist =
      this.randomDocument.pipe(
        map(() => false),
        catchError(err => {
          if (err.status === 404) {
            return of(true);
          }}));

    this.thereAreNoDocs = this.randomDocument
      .pipe(map(doc => !doc.json));

    this.largeDocument = this.randomDocument
      .pipe(map(doc => this.mnHelperService.byteCount(doc.json) > 256 * 1024));

    this.hasWarnings =
      combineLatest(this.documentDoesNotExist,
                    this.largeDocument,
                    this.thereAreNoDocs)
      .pipe(map(warnings => warnings.some(v => v)));

    this.mnViewsListService.stream.getDdoc
      .pipe(takeUntil(this.mnOnDestroy),
            withLatestFrom(this.viewId))
      .subscribe(([ddoc, viewId]) => {
        this.saveForm.group.get('mapJson').setValue(js_beautify(ddoc.views[viewId].map, { indent_size: 2 }));
        this.saveForm.group.get('reduceJson').setValue(js_beautify(ddoc.views[viewId].reduce, { indent_size: 2 }));
      });

    this.clickEdit = new Subject();
    this.clickEdit
      .pipe(takeUntil(this.mnOnDestroy),
            withLatestFrom(this.commonBucket, this.randomDocument))
      .subscribe(this.openEditDialog.bind(this));

    this.clickRandom = new Subject();
    this.clickRandom
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(() => this.mnDocumentsService.stream.recalculateRandomDocument.next());

    this.copyViewDialog = new Subject();
    this.copyViewDialog
      .pipe(takeUntil(this.mnOnDestroy),
            withLatestFrom(this.mnViewsListService.stream.getDdoc,
                           this.ddocumentId))
      .subscribe(([, data, ddocumentId]) => {
        let ref = modalService.open(MnViewsCreateDialogComponent);
        ref.componentInstance.views = data.views;
        ref.componentInstance.ddocName = ddocumentId;
        ref.componentInstance.mapJson = this.saveForm.group.get('mapJson').value;
        ref.componentInstance.reduceJson = this.saveForm.group.get('reduceJson').value;
      });

    this.clickReduce = new Subject();
    this.clickReduce
      .pipe(takeUntil(this.mnOnDestroy),
            withLatestFrom(this.type))
      .subscribe(this.setReduceValue.bind(this));
  }

  ngAfterViewInit() {
    this.toggle.click.next();
  }

  hasWritePermission([permissions, bucket]) {
    return permissions.cluster.bucket[bucket] &&
      permissions.cluster.bucket[bucket].views.write;
  }

  hasReadPermission([permissions, bucket]) {
    let name = bucket + ":.:.";
    let perm = permissions.cluster.collection[name];
    return perm && perm.data.docs.read;
  }

  packSaveData([, data, commonBucket, ddocumentId, viewId]) {
    let url = this.mnViewsListService.getDdocUrl([commonBucket, ddocumentId]);
    let json = { views: data.views };

    if (this.saveForm.group.get('mapJson').value) {
      json.views[viewId].map = this.saveForm.group.get('mapJson').value;
    } else {
      delete json.views[viewId].map;
    }

    if (this.saveForm.group.get('reduceJson').value) {
      json.views[viewId].reduce = this.saveForm.group.get('reduceJson').value;
    } else {
      delete json.views[viewId].reduce;
    }

    return { url, json };
  }

  setReduceValue([value, type]) {
    if (type === "production") {
      return;
    }

    return this.saveForm.group.get('reduceJson').setValue(value);
  }

  openEditDialog([, commonBucket, randomDocument]) {
    this.qwDialogService.getAndShowDocument(false, "Edit Document", commonBucket, "_default", "_default", randomDocument.meta.id).then(payload => {
      if (is(String, payload)) {
        return;
      }

      this.mnDocumentsService.stream.getManualDocument.next([randomDocument.meta.id, commonBucket]);
    })
  }

}
