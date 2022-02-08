/*
  Copyright 2021-Present Couchbase, Inc.

  Use of this software is governed by the Business Source License included in
  the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
  file, in accordance with the Business Source License, use of this software will
  be governed by the Apache License, Version 2.0, included in the file
  licenses/APL2.txt.
*/
import {Component, ChangeDetectionStrategy} from '@angular/core';
import {pluck, filter, shareReplay, map} from 'rxjs/operators';
import {DatePipe} from '@angular/common';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnLogsListService} from './mn.logs.list.service.js';
import {MnHelperService} from './mn.helper.service.js';

export {MnLogsListComponent};

class MnLogsListComponent extends MnLifeCycleHooksToStream {

  static get annotations() {
    return [
      new Component({
        templateUrl: "app/mn.logs.list.html",
        changeDetection: ChangeDetectionStrategy.OnPush
      })
  ]}

  static get parameters() { return [
    MnLogsListService,
    MnHelperService,
    DatePipe
  ]}

  constructor(mnLogsService, mnHelperService, datePipe) {
    super();

    this.datePipe = datePipe;
    this.textLimit = 1000;
    this.sorter = mnHelperService.createSorter('tstamp', true);
    this.filter = mnHelperService.createFilter(this,
                                               ['text', 'module', 'code', 'node', 'prettyTime'],
                                               true);

    this.logs = mnLogsService.stream.logs
      .pipe(pluck('list'),
            filter(logs => !!logs),
            map(this.addPrettyTime.bind(this)),
            this.filter.pipe,
            this.sorter.pipe,
            shareReplay({refCount: true, bufferSize: 1}));
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

  addPrettyTime(logs) {
    logs.forEach(log =>
                 (log.prettyTime =
                  this.datePipe.transform(log.serverTime, 'mediumTime', 'UTC', 'en-US') + " " +
                  this.datePipe.transform(log.serverTime, 'd MMM, y', 'UTC', 'en-US')));
    return logs;
  }
}
