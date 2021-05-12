/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/




var mn = mn || {};
mn.helper = mn.helper || {};
mn.helper.invert = function (v) {
  return !v;
};

var mn = mn || {};
mn.helper = mn.helper || {};
mn.helper.createBucketTypePipe = (function () {
  "use strict";

  return function (bucketType) {
    var capitalize = bucketType.charAt(0).toUpperCase() + bucketType.slice(1);

    MnBucketType.annotations = [
      new ng.core.Pipe({
        name: "mnIs" + capitalize
      })
    ];

    MnBucketType.prototype.transform = transform;

    return MnBucketType;

    function MnBucketType() {
    }

    function transform(bucket) {
      if (bucket instanceof ng.forms.FormGroup) {
        return bucket.get("bucketType").value === bucketType;
      } else if (bucket instanceof Object) {
        return bucket.bucketType === bucketType;
      } else {
        return bucket === bucketType;
      }
    }

  }
})();



var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnHelper = (function (Rx) {

  MnHelper.annotations = [
    new ng.core.Injectable()
  ];

  MnHelper.prototype.validateEqual = validateEqual;
  MnHelper.prototype.getServiceVisibleName = getServiceVisibleName;
  MnHelper.prototype.getServiceQuotaName = getServiceQuotaName;
  MnHelper.prototype.pluckMemoryQuotas = pluckMemoryQuotas;
  MnHelper.prototype.calculateMaxMemorySize = calculateMaxMemorySize;
  MnHelper.prototype.isJson = isJson;
  MnHelper.prototype.sortByStream = sortByStream;
  MnHelper.prototype.createToggle = createToggle;

  return MnHelper;

  function MnHelper() {
    this.daysOfWeek = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    this.IEC = {Ki: 1024, Mi: 1024 * 1024, Gi: 1024 * 1024 * 1024};
  }

  function isJson(str) {
    try {
      JSON.parse(str);
    } catch (e) {
      return false;
    }
    return true;
  }

  function calculateMaxMemorySize(totalRAMMegs) {
    return Math.floor(Math.max(totalRAMMegs * 0.8, totalRAMMegs - 1024));
  }

  function pluckMemoryQuotas(source) {
    return source[1].reduce(function (acc, service) {
      acc[service] = source[0][this.getServiceQuotaName(service)];
      return acc;
    }.bind(this), {});
  }

  function getServiceQuotaName(service) {
    switch (service) {
    case "kv": return "memoryQuota";
    default: return service + "MemoryQuota";
    }
  }

  function getServiceVisibleName(service) {
    switch (service) {
    case "kv": return "Data";
    case "index": return "Index";
    case "fts": return "Search";
    case "n1ql": return "Query";
    case "eventing": return "Eventing";
    case "cbas": return "Analytics";
    }
  }

  function validateEqual(key1, key2, erroName) {
    return function (group) {
      if (group.get(key1).value !== group.get(key2).value) {
        var rv = {};
        rv[erroName] = true;
        return rv;
      }
    }
  }

  function createToggle(version) {
    this.click = new Rx.Subject();
    this.state = this.click.pipe(Rx.operators.scan(R.not, false),
                                 mn.core.rxOperatorsShareReplay(1));
  }

  function sortByStream(sortByStream) {
    return function (arrayStream) {
      var isAsc = sortByStream.pipe(Rx.operators.scan(mn.helper.invert, false));

      return Rx
        .combineLatest(arrayStream,
                       Rx.zip(sortByStream, isAsc))
        .pipe(Rx.operators.map(doSort),
              mn.core.rxOperatorsShareReplay(1));

      function doSort(values) {
        var copyArray = values[0].slice();
        var sortBy = values[1][0];
        var isAsc = values[1][1];
        copyArray = _.sortBy(copyArray, sortBy);

        if (!isAsc) {
          return copyArray.reverse();
        } else {
          return copyArray;
        }
      }
    }
  }

})(window.rxjs);
