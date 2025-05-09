/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {switchMap, shareReplay} from 'rxjs/operators';
import {BehaviorSubject} from 'rxjs';

import {MnHttpRequest} from './mn.http.request.js';
import {singletonGuard} from './mn.core.js';

export {MnSettingsAlertsService}

class MnSettingsAlertsService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient
  ]}

  constructor(http) {
    singletonGuard(MnSettingsAlertsService);

    this.http = http;
    this.stream = {};

    this.stream.getAlerts =
      (new BehaviorSubject()).pipe(
        switchMap(this.getAlerts.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));

    this.stream.saveAlerts =
      new MnHttpRequest(this.saveAlerts.bind(this))
      .addSuccess()
      .addError();

    this.stream.testMail =
      new MnHttpRequest(this.testMail.bind(this))
      .addSuccess()
      .addError();

  }

  getAlerts() {
    return this.http.get('/settings/alerts');
  }

  saveAlerts(params) {
    return this.http.post('/settings/alerts', params);
  }

  testMail(params) {
    return this.http.post('/settings/alerts/testEmail', params);
  }

}
