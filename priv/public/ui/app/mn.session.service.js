/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {fromEvent, merge, NEVER, timer} from 'rxjs';
import {throttleTime, takeUntil, filter,
        switchMap, map, shareReplay} from 'rxjs/operators';
import {not, compose} from 'ramda';

import {MnAuth} from './ajs.upgraded.providers.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnSessionTimeoutDialogComponent} from './mn.session.timeout.dialog.component.js';
import {singletonGuard} from './mn.core.js';

export {MnSessionService};

class MnSessionService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnAuth,
    MnAdminService,
    NgbModal
  ]}

  constructor(http, mnAuth, mnAdminService, modalService) {
    singletonGuard(MnSessionService);

    this.http = http;
    this.modalService = modalService;
    this.postUILogout = mnAuth.logout;

    this.stream = {};

    this.stream.storage = fromEvent(window, 'storage');
    this.stream.mousemove = fromEvent(window, 'mousemove');
    this.stream.keydown = fromEvent(window, 'keydown');
    this.stream.touchstart = fromEvent(window, 'touchstart');

    this.stream.userEvents =
      merge(this.stream.mousemove,
            this.stream.keydown,
            this.stream.touchstart)
      .pipe(throttleTime(300));

    this.stream.poolsSessionTimeout =
      mnAdminService.stream.uiSessionTimeout;

    this.stream.storageResetSessionTimeout =
      this.stream.storage.pipe(filter(this.isUiSessionTimeoutEvent.bind(this)));

    this.stream.resetSessionTimeout =
      merge(this.stream.storageResetSessionTimeout,
            this.stream.poolsSessionTimeout,
            this.stream.userEvents)
      .pipe(filter(compose(not, this.isDialogOpened.bind(this))),
            map(this.getUiSessionTimeout.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));
  }

  activate(mnOnDestroy) {
    this.stream.poolsSessionTimeout
      .pipe(map(this.minToSeconds.bind(this)),
            takeUntil(mnOnDestroy))
      .subscribe(this.setTimeout.bind(this));

    this.stream.userEvents
      .pipe(filter(compose(not, this.isDialogOpened.bind(this))),
            takeUntil(mnOnDestroy))
      .subscribe(this.resetAndSyncTimeout.bind(this));

    this.stream.storageResetSessionTimeout
      .pipe(filter(this.isDialogOpened.bind(this)),
            takeUntil(mnOnDestroy))
      .subscribe(this.dismissDialog.bind(this));

    this.stream.resetSessionTimeout
      .pipe(map(this.getDialogTimeout.bind(this)),
            switchMap(this.createTimer.bind(this)),
            takeUntil(mnOnDestroy))
      .subscribe(this.openDialog.bind(this));

    this.stream.resetSessionTimeout
      .pipe(switchMap(this.createTimer.bind(this)),
            takeUntil(mnOnDestroy))
      .subscribe(this.logout.bind(this));

  }

  setTimeout(uiSessionTimeout) {
    localStorage.setItem("uiSessionTimeout", Number(uiSessionTimeout));
  }

  resetAndSyncTimeout() {
    //localStorage triggers event "storage" only when storage value has been changed
    localStorage.setItem("uiSessionTimeoutReset",
                         (Number(localStorage.getItem("uiSessionTimeoutReset")) + 1) || 0);
  }

  getUiSessionTimeout() {
    return Number(localStorage.getItem("uiSessionTimeout")) || 0;
  }

  isUiSessionTimeoutEvent(e) {
    return e.key === "uiSessionTimeoutReset";
  }

  openDialog() {
    this.dialogRef = this.modalService.open(MnSessionTimeoutDialogComponent);
    this.dialogRef.result.then(this.removeDialog.bind(this), this.removeDialog.bind(this));
  }

  removeDialog() {
    this.dialogRef = null;
  }

  dismissDialog() {
    this.dialogRef.dismiss();
  }

  isDialogOpened() {
    return !!this.dialogRef;
  }

  getDialogTimeout(t) {
    return t - 30000;
  }

  minToSeconds(t) {
    return t * 1000;
  }

  createTimer(t) {
    return t && t > 0 ? timer(t) : NEVER;
  }

  logout() {
    this.postUILogout();
  }
}
