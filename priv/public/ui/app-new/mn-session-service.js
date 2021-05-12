/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnSession = (function (Rx) {
  "use strict";

  MnSessionService.annotations = [
    new ng.core.Injectable()
  ];

  MnSessionService.parameters = [
    ng.common.http.HttpClient,
    mn.services.MnAuth,
    mn.services.MnAdmin,
    ngb.NgbModal
  ];

  MnSessionService.prototype.createTimer = createTimer;
  MnSessionService.prototype.activate = activate;
  MnSessionService.prototype.getUiSessionTimeout = getUiSessionTimeout;
  MnSessionService.prototype.getDialogTimeout = getDialogTimeout;
  MnSessionService.prototype.removeDialog = removeDialog;
  MnSessionService.prototype.dismissDialog = dismissDialog;
  MnSessionService.prototype.isDialogOpened = isDialogOpened;
  MnSessionService.prototype.openDialog = openDialog;
  MnSessionService.prototype.setTimeout = setTimeout;
  MnSessionService.prototype.logout = logout;
  MnSessionService.prototype.isUiSessionTimeoutEvent = isUiSessionTimeoutEvent;
  MnSessionService.prototype.minToSeconds = minToSeconds;
  MnSessionService.prototype.resetAndSyncTimeout = resetAndSyncTimeout;

  return MnSessionService;

  function MnSessionService(http, mnAuthService, mnAdminService, modalService) {
    this.http = http;
    this.modalService = modalService;
    this.postUILogout = mnAuthService.stream.postUILogout;

    this.stream = {};

    this.stream.storage = Rx.fromEvent(window, 'storage');
    this.stream.mousemove = Rx.fromEvent(window, 'mousemove');
    this.stream.keydown = Rx.fromEvent(window, 'keydown');
    this.stream.touchstart = Rx.fromEvent(window, 'touchstart');

    this.stream.userEvents = Rx.merge(
      this.stream.mousemove,
      this.stream.keydown,
      this.stream.touchstart
    ).pipe(Rx.operators.throttleTime(300));

    this.stream.poolsSessionTimeout =
      mnAdminService.stream.uiSessionTimeout;

    this.stream.storageResetSessionTimeout =
      this.stream.storage
      .pipe(Rx.operators.filter(this.isUiSessionTimeoutEvent.bind(this)));

    this.stream.resetSessionTimeout =
      Rx.merge(
        this.stream.storageResetSessionTimeout,
        this.stream.poolsSessionTimeout,
        this.stream.userEvents)
      .pipe(
        Rx.operators.map(this.isDialogOpened.bind(this)),
        Rx.operators.filter(mn.helper.invert),
        Rx.operators.map(this.getUiSessionTimeout.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );
  }

  function activate(mnOnDestroy) {
    this.stream.poolsSessionTimeout
      .pipe(Rx.operators.map(this.minToSeconds.bind(this)),
            Rx.operators.takeUntil(mnOnDestroy))
      .subscribe(this.setTimeout.bind(this));

    this.stream.userEvents
      .pipe(Rx.operators.map(this.isDialogOpened.bind(this)),
            Rx.operators.filter(mn.helper.invert),
            Rx.operators.takeUntil(mnOnDestroy))
      .subscribe(this.resetAndSyncTimeout.bind(this));

    this.stream.storageResetSessionTimeout
      .pipe(Rx.operators.filter(this.isDialogOpened.bind(this)),
            Rx.operators.takeUntil(mnOnDestroy))
      .subscribe(this.dismissDialog.bind(this));

    this.stream.resetSessionTimeout
      .pipe(Rx.operators.map(this.getDialogTimeout.bind(this)),
            Rx.operators.switchMap(this.createTimer.bind(this)),
            Rx.operators.takeUntil(mnOnDestroy))
      .subscribe(this.openDialog.bind(this));

    this.stream.resetSessionTimeout
      .pipe(Rx.operators.switchMap(this.createTimer.bind(this)),
            Rx.operators.takeUntil(mnOnDestroy))
      .subscribe(this.logout.bind(this));

  }

  function setTimeout(uiSessionTimeout) {
    localStorage.setItem("uiSessionTimeout", Number(uiSessionTimeout));
  }

  function resetAndSyncTimeout() {
    //localStorage triggers event "storage" only when storage value has been changed
    localStorage.setItem("uiSessionTimeoutReset",
                         (Number(localStorage.getItem("uiSessionTimeoutReset")) + 1) || 0);
  }

  function getUiSessionTimeout() {
    return Number(localStorage.getItem("uiSessionTimeout")) || 0;
  }

  function isUiSessionTimeoutEvent(e) {
    return e.key === "uiSessionTimeoutReset";
  }

  function openDialog() {
    this.dialogRef = this.modalService.open(mn.components.MnSessionTimeoutDialog);
    this.dialogRef.result.then(this.removeDialog.bind(this), this.removeDialog.bind(this));
  }

  function removeDialog() {
    this.dialogRef = null;
  }

  function dismissDialog() {
    this.dialogRef.dismiss();
  }

  function isDialogOpened() {
    return !!this.dialogRef;
  }

  function getDialogTimeout(t) {
    return t - 30000;
  }

  function minToSeconds(t) {
    return t * 1000;
  }

  function createTimer(t) {
    return t && t > 0 ? Rx.timer(t) : Rx.NEVER;
  }

  function logout() {
    this.postUILogout.post();
  }

})(window.rxjs);
