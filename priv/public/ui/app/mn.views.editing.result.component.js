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
import { pluck, map, withLatestFrom,
         takeUntil, take, filter, startWith } from 'rxjs/operators';
import { Subject, combineLatest } from 'rxjs';
import { reject, equals, is } from 'ramda';

import { MnFormService } from './mn.form.service.js';
import { MnViewsEditingService } from './mn.views.editing.service.js';
import { MnAdminService } from './mn.admin.service.js';
import { MnDocumentsService } from './mn.documents.service.js';

import { viewsPerPageLimit } from './constants/constants.js';

export { MnViewsEditingResultComponent };

class MnViewsEditingResultComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "views-editing-result",
      templateUrl: "app/mn.views.editing.result.html",
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    UIRouter,
    MnViewsEditingService,
    MnAdminService,
    MnDocumentsService
  ]}

  constructor(mnFormService, uiRouter, mnViewsEditingService, mnAdminService, mnDocumentsService) {
    super();

    this.uiRouter = uiRouter;
    this.capiBase = mnAdminService.stream.capiBase;
    this.mnDocumentsService = mnDocumentsService;
    this.getViewResult = mnViewsEditingService.stream.getViewResult.response;

    this.ddocumentId = uiRouter.globals.params$.pipe(pluck('ddocumentId'));
    this.viewId = uiRouter.globals.params$.pipe(pluck('viewId'));
    this.commonBucket = uiRouter.globals.params$.pipe(pluck('commonBucket'));
    this.pageNumber = uiRouter.globals.params$.pipe(pluck('pageNumber'));
    this.fullSet = uiRouter.globals.params$.pipe(pluck('full_set'));

    // If the pageNumber parameter is present,
    // the results should persist on page reload.
    this.areResultsPresent = this.pageNumber
      .pipe(map(page => is(Number, page)));

    this.defaultFormGroup = {
      conflicts: null,
      descending: null,
      startKey: null,
      endKey: null,
      startKeyDocId: null,
      endKeyDocId: null,
      stale: "false",
      group: null,
      group_level: null,
      inclusive_end: true,
      connection_timeout: 60000,
      key: null,
      keys: null,
      reduce: ""
    };

    this.form = mnFormService.create(this)
      .setFormGroup(this.defaultFormGroup)
      .hasNoPostRequest();

    this.isDevelopmentDocument = this.ddocumentId
      .pipe(map(id => id.includes('_design/dev_')));

    this.rows = this.getViewResult
      .pipe(map(response => response.status == "500" ? [] : response.rows));

    this.params = this.form.submit
      .pipe(startWith('stale=false&inclusive_end=true&connection_timeout=60000'),
            map(() => this.generateParamsString(this.form.group.value)));

    let upperRange =
      this.pageNumber.pipe(map(page => viewsPerPageLimit + (page * viewsPerPageLimit)));

    let lowerRange =
      upperRange.pipe(map(range => range - viewsPerPageLimit));

    this.paginateParams = combineLatest(lowerRange, upperRange);


    this.paginatedRows =
      combineLatest(this.rows,
                    this.paginateParams)
      .pipe(map(([rows, range]) => rows.slice(range[0], range[1])));

    this.maxPageNumber = this.rows
      .pipe(startWith([]),
            map(rows => rows.length ? Math.floor(rows.length / viewsPerPageLimit) : 1));

    this.url =
      combineLatest(this.params,
                    this.ddocumentId,
                    this.viewId,
                    this.commonBucket)
      .pipe(map(([params, ddocumentId, viewId, commonBucket]) =>
          this.buildFullUrl(params, ddocumentId, viewId, commonBucket)));

    this.disablePrev =
      combineLatest(this.pageNumber,
                    this.areResultsPresent)
      .pipe(map(this.disablePrev.bind(this)));

    this.disableNext =
      combineLatest(this.rows.pipe(startWith([])),
                    this.pageNumber,
                    this.maxPageNumber,
                    this.areResultsPresent)
      .pipe(map(this.disableNext.bind(this)));

    this.showNoResults =
      this.rows.pipe(map(rows => !rows.length));

    this.pageNumber.pipe(
      filter(page => is(Number, page)),
      take(1),
      withLatestFrom(this.pageNumber,
                     this.params,
                     this.ddocumentId,
                     this.viewId,
                     this.commonBucket,
                     this.fullSet))
      .subscribe(this.onClickResult.bind(this));

    this.clickResult = new Subject();
    this.clickResult
      .pipe(withLatestFrom(this.pageNumber,
                           this.params,
                           this.ddocumentId,
                           this.viewId,
                           this.commonBucket,
                           this.fullSet),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.onClickResult.bind(this));

    this.clickNextPage = new Subject();
    this.clickNextPage
      .pipe(withLatestFrom(this.pageNumber),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.onNextPage.bind(this));

    this.clickPrevPage = new Subject();
    this.clickPrevPage
      .pipe(withLatestFrom(this.pageNumber),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.onPrevPage.bind(this));

    this.clickLoadDocument = new Subject();
    this.clickLoadDocument
      .pipe(withLatestFrom(this.commonBucket),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.loadDocument.bind(this))
  }

  generateParamsString(value) {
    let params = reject(equals(null))(value);
    params = reject(equals(""))(params);

    return new URLSearchParams(params).toString();
  }

  disablePrev([pageNumber, resultsPresent]) {
    return !resultsPresent || (pageNumber == 0);
  }

  disableNext([rows, pageNumber, maxPage, resultsPresent]) {
    return !resultsPresent || !rows.length || (pageNumber >= maxPage);
  }

  onNextPage([, currentPage]) {
    this.uiRouter.stateService.go('.', { pageNumber: currentPage + 1 } , { notify: false });
   }

  onPrevPage([, currentPage]) {
    this.uiRouter.stateService.go('.', { pageNumber: currentPage - 1 } , { notify: false });
  }

  onClickResult([, pageNumber, params, ddocumentId, viewId, commonBucket, fullSet]) {
    if (!pageNumber) {
      this.uiRouter.stateService.go('.', { pageNumber: 0 } , { notify: false });
    }

    this.getViewResult.next(this.buildFullUrl(params, ddocumentId, viewId, commonBucket, fullSet));
  }

  buildViewUrl(ddocumentId, viewId, commonBucket) {
    if (ddocumentId.slice(0, "_design/".length) === "_design/") {
      ddocumentId = "_design/" + encodeURIComponent(ddocumentId.slice("_design/".length));
    }

    if (ddocumentId.slice(0, "_local/".length) === "_local/") {
      ddocumentId = "_local/" + encodeURIComponent(ddocumentId.slice("_local/".length));
    }

    return encodeURIComponent(commonBucket) + "/" + ddocumentId + "/_view/" + encodeURIComponent(viewId);
  }

  buildFullUrl(params, ddocumentId, viewId, commonBucket, fullSet) {
    if (fullSet) {
      params += "&full_set=true";
    }

    return '/couchBase/' + this.buildViewUrl(ddocumentId, viewId, commonBucket) + '?' + params;
  }

  loadDocument([id, bucket]) {
    this.mnDocumentsService.stream.getManualDocument.next([id, bucket]);
  }
}
