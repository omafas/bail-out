/*
  Copyright 2025-Present Couchbase, Inc.

  Use of this software is governed by the Business Source License included in
  the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
  file, in accordance with the Business Source License, use of this software
  will be governed by the Apache License, Version 2.0, included in the file
  licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';

import {singletonGuard} from './mn.core.js';

export {MnTimezoneDetailsService}

class MnTimezoneDetailsService {
  static get annotations() { return [
    new Injectable()
  ];}

  static get parameters() { return [];}

  constructor() {
    singletonGuard(MnTimezoneDetailsService);
  }

  getLocalTimezoneLabel() {
    const dateString = (new Date()).toString();
    return dateString.substring(dateString.indexOf('(') + 1, dateString.indexOf(')')).trim();
  }

  getLocalGMTString() {
    const dateString = (new Date()).toString();
    const gmtIndex = dateString.indexOf('GMT');
    const descIndex = dateString.indexOf(' (');
    let result = dateString.substring(gmtIndex + 3, descIndex).trim();
    result = result.substring(0, result.length - 2) + ':' + result.slice(result.length - 2);
    return `(UTC ${result})`;
  }

  getServerGMTOffset(serverTime) {
    if (!serverTime) {
      return 'UTC +00:00';
    }
    if (serverTime.includes('Z')) {
      return 'UTC +00:00'
    }

    return `UTC ${serverTime.slice(-6)}`;
  }
}
