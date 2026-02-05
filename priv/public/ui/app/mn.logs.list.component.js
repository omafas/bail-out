/*
  Copyright 2021-Present Couchbase, Inc.

  Use of this software is governed by the Business Source License included in
  the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
  file, in accordance with the Business Source License, use of this software
  will be governed by the Apache License, Version 2.0, included in the file
  licenses/APL2.txt.
*/
import {Component, ChangeDetectionStrategy} from '@angular/core';
import {pluck, filter, shareReplay, map, withLatestFrom} from 'rxjs/operators';
import {DatePipe} from '@angular/common';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnLogsListService} from './mn.logs.list.service.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnTimezoneDetailsService} from './mn.timezone.details.service.js';
import {MnAdminService} from './mn.admin.service.js';
import template from "./mn.logs.list.html";

export {MnLogsListComponent};

class MnLogsListComponent extends MnLifeCycleHooksToStream {

  static get annotations() {
    return [
      new Component({
        template,
        changeDetection: ChangeDetectionStrategy.OnPush
      })
  ]}

  static get parameters() { return [
    MnLogsListService,
    MnHelperService,
    MnTimezoneDetailsService,
    DatePipe,
    MnAdminService
  ]}

  constructor(mnLogsService, mnHelperService, mnTimezoneDetailsService, datePipe, mnAdminService) {
    super();

    this.compatVersion79 = mnAdminService.stream.compatVersion79;
    this.mnTimezoneDetailsService = mnTimezoneDetailsService;
    this.datePipe = datePipe;
    this.textLimit = 1000;
    this.sorter = mnHelperService.createSorter('tstamp', true);
    this.filter = mnHelperService.createFilter(this,
                                               ['text', 'module', 'code', 'node', 'prettyTime'],
                                               true);

    this.logs = mnLogsService.stream.logs
      .pipe(pluck('list'),
            filter(logs => !!logs),
            withLatestFrom(this.compatVersion79),
            map(this.addPrettyTime.bind(this)),
            this.filter.pipe,
            this.sorter.pipe,
            shareReplay({refCount: true, bufferSize: 1}));

    this.serverTimeExample = this.logs.pipe(map(logs => logs.length ? logs[0].serverTime : ''));
  }

  trackByMethod(index, log) {
    return log.shortText +
           log.code +
           log.module +
           log.node +
           log.tstamp +
           log.serverTime +
           log.type;
  }

  addPrettyTime([logs, isCompat79]) {
    logs.forEach(log => {
      if (isCompat79) {
        log.prettyTime =
          this.datePipe.transform(log.serverTime, 'mediumTime') + " " +
          this.datePipe.transform(log.serverTime, 'd MMM, y');
      } else {
        log.prettyTime =
          this.datePipe.transform(log.serverTime, 'mediumTime', 'UTC', 'en-US') + " " +
          this.datePipe.transform(log.serverTime, 'd MMM, y', 'UTC', 'en-US');
      }
    });

    return logs;
  }
}
