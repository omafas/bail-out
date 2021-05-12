/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Pipe, Injectable } from '../web_modules/@angular/core.js';
import { Subject, combineLatest, zip } from '../web_modules/rxjs.js';
import { scan, map  } from '../web_modules/rxjs/operators.js';
import { not } from '../web_modules/ramda.js';

export { MnHelperService };

class MnHelperService {
  static annotations = [
    new Injectable()
  ]

  daysOfWeek = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']

  IEC = {Ki: 1024, Mi: 1024 * 1024, Gi: 1024 * 1024 * 1024}

  constructor() {
  }

  invert(v) { //TODO: sould be replaced with Ramda.not
    return !v;
  }

  isJson(str) {
    try {
      JSON.parse(str);
    } catch (e) {
      return false;
    }
    return true;
  }

  calculateMaxMemorySize(totalRAMMegs) {
    return Math.floor(Math.max(totalRAMMegs * 0.8, totalRAMMegs - 1024));
  }

  pluckMemoryQuotas(source) {
    return source[1].reduce((acc, service) => {
      acc[service] = source[0][this.getServiceQuotaName(service)];
      return acc;
    }, {});
  }

  getServiceQuotaName(service) {
    switch (service) {
    case "kv": return "memoryQuota";
    default: return service + "MemoryQuota";
    }
  }

  getServiceVisibleName(service) {
    switch (service) {
    case "kv": return "Data";
    case "index": return "Index";
    case "fts": return "Search";
    case "n1ql": return "Query";
    case "eventing": return "Eventing";
    case "cbas": return "Analytics";
    case "backup": return "Backup";
    }
  }

  validateEqual(key1, key2, erroName) {
    return function (group) {
      if (group.get(key1).value !== group.get(key2).value) {
        var rv = {};
        rv[erroName] = true;
        return rv;
      }
    }
  }

  createToggle(version) {
    this.click = new Subject();
    this.state = this.click.pipe(scan(not, false), shareReplay(1));
  }

  doSort([array, [sortBy, isAsc]]) {
    var copyArray = array.slice();
    copyArray = _.sortBy(copyArray, sortBy); //TODO: sould be replaced with Ramda.not

    if (!isAsc) {
      return copyArray.reverse();
    } else {
      return copyArray;
    }
  }

  sortByStream(sortByStream) {
    return function (arrayStream) {
      var isAsc = sortByStream.pipe(scan(not, false));

      return combineLatest(arrayStream,
                           zip(sortByStream, isAsc)).pipe(map(this.doSort),
                                                          shareReplay(1));
    }
  }
}
// var mn = mn || {};
// mn.helper = mn.helper || {};
// mn.helper.createBucketTypePipe = (function () {
//   "use strict";

//   return function (bucketType) {
//     var capitalize = bucketType.charAt(0).toUpperCase() + bucketType.slice(1);

//     MnBucketType.annotations = [
//       new ng.core.Pipe({
//         name: "mnIs" + capitalize
//       })
//     ];

//     MnBucketType.prototype.transform = transform;

//     return MnBucketType;

//     function MnBucketType() {
//     }

//     function transform(bucket) {
//       if (bucket instanceof ng.forms.FormGroup) {
//         return bucket.get("bucketType").value === bucketType;
//       } else if (bucket instanceof Object) {
//         return bucket.bucketType === bucketType;
//       } else {
//         return bucket === bucketType;
//       }
//     }

//   }
  // })();
  // mn.pipes.MnIsMembase = mn.helper.createBucketTypePipe("membase")
// mn.pipes.MnIsEphemeral = mn.helper.createBucketTypePipe("ephemeral");
// mn.pipes.MnIsMemcached = mn.helper.createBucketTypePipe("memcached");
