/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnParseVersion =
  (function () {
    "use strict";

    MnParseVersionPipe.annotations = [
      new ng.core.Pipe({
        name: "mnParseVersion"
      })
    ];

    MnParseVersionPipe.prototype.transform = transform;

    return MnParseVersionPipe;

    function MnParseVersionPipe() {
    }

    function transform(str) {
      if (!str) {
        return;
      }
      // Expected string format:
      //   {release version}-{build #}-{Release type or SHA}-{enterprise / community}
      // Example: "1.8.0-9-ga083a1e-enterprise"
      var a = str.split(/[-_]/);
      if (a.length === 3) {
        // Example: "1.8.0-9-enterprise"
        //   {release version}-{build #}-{enterprise / community}
        a.splice(2, 0, undefined);
      }
      a[0] = (a[0].match(/[0-9]+\.[0-9]+\.[0-9]+/) || ["0.0.0"])[0];
      a[1] = a[1] || "0";
      // a[2] = a[2] || "unknown";
      // We append the build # to the release version when we display in the UI so that
      // customers think of the build # as a descriptive piece of the version they're
      // running (which in the case of maintenance packs and one-off's, it is.)
      a[3] = (a[3] && (a[3].substr(0, 1).toUpperCase() + a[3].substr(1))) || "DEV";
      return a; // Example result: ["1.8.0-9", "9", "ga083a1e", "Enterprise"]
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnMBtoBytes =
  (function () {
    "use strict";

    MnMBtoBytes.annotations = [
      new ng.core.Pipe({
        name: "mnMBtoBytes"
      })
    ];

    MnMBtoBytes.parameters = [
      mn.services.MnHelper
    ];

    MnMBtoBytes.prototype.transform = transform;

    return MnMBtoBytes;
    function MnMBtoBytes(mnHelperService) {
      this.IEC = mnHelperService.IEC;
    }
    function transform(MB) {
      return MB * this.IEC.Mi;
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnBytesToMB =
  (function () {
    "use strict";

    MnBytesToMB.annotations = [
      new ng.core.Pipe({
        name: "mnBytesToMB"
      })
    ];

    MnBytesToMB.parameters = [
      mn.services.MnHelper
    ];

    MnBytesToMB.prototype.transform = transform;

    return MnBytesToMB;
    function MnBytesToMB(mnHelperService) {
      this.IEC = mnHelperService.IEC;
    }
    function transform(bytes) {
      return Math.floor(bytes / this.IEC.Mi);
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnObjectKeys =
  (function () {
    "use strict";

    MnObjectKeys.annotations = [
      new ng.core.Pipe({
        name: "mnObjectKeys"
      })
    ];

    MnObjectKeys.prototype.transform = transform;

    return MnObjectKeys;

    function MnObjectKeys() {}

    function transform(object) {
      if (object) {
        return Object.keys(object);
      } else {
        return [];
      }
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnPrettyVersion =
  (function () {
    "use strict";

    MnPrettyVersionPipe.annotations = [
      new ng.core.Pipe({
        name: "mnPrettyVersion"
      })
    ];

    MnPrettyVersionPipe.parameters = [
      mn.pipes.MnParseVersion
    ];

    MnPrettyVersionPipe.prototype.transform = transform;

    return MnPrettyVersionPipe;

    function MnPrettyVersionPipe(mnParseVersion) {
      this.mnParseVersion = mnParseVersion;
    }

    function transform(str, full) {
      if (!str) {
        return;
      }
      var a = this.mnParseVersion.transform(str);
      // Example default result: "Enterprise Edition 1.8.0-7  build 7"
      // Example full result: "Enterprise Edition 1.8.0-7  build 7-g35c9cdd"
      var suffix = "";
      if (full && a[2]) {
        suffix = '-' + a[2];
      }
      return [a[3], "Edition", a[0], "build",  a[1] + suffix].join(' ');
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnFormatProgressMessage =
  (function () {
    "use strict";

    function addNodeCount(perNode) {
      var serversCount = (_.keys(perNode) || []).length;
      return serversCount + " " + (serversCount === 1 ? 'node' : 'nodes');
    }

    MnFormatProgressMessage.annotations = [
      new ng.core.Pipe({
        name: "mnFormatProgressMessage"
      })
    ];

    MnFormatProgressMessage.prototype.transform = transform;

    return MnFormatProgressMessage;

    function MnFormatProgressMessage() {
    }

    function transform(task) {
      switch (task.type) {
      case "indexer":
        return "building view index " + task.bucket + "/" + task.designDocument;
      case "global_indexes":
        return "building index " + task.index  + " on bucket " + task.bucket;
      case "view_compaction":
        return "compacting view index " + task.bucket + "/" + task.designDocument;
      case "bucket_compaction":
        return "compacting bucket " + task.bucket;
      case "loadingSampleBucket":
        return "loading sample: " + task.bucket;
      case "orphanBucket":
        return "orphan bucket: " + task.bucket;
      case "clusterLogsCollection":
        return "collecting logs from " + addNodeCount(task.perNode);
      case "rebalance":
        var serversCount = (_.keys(task.perNode) || []).length;
        return (task.subtype == 'gracefulFailover') ?
          "failing over 1 node" :
          ("rebalancing " + addNodeCount(task.perNode));
      }
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnFormatStorageModeError =
  (function () {
    "use strict";

    MnFormatStorageModeError.annotations = [
      new ng.core.Pipe({
        name: "mnFormatStorageModeError"
      })
    ];

    MnFormatStorageModeError.prototype.transform = transform;

    return MnFormatStorageModeError;

    function MnFormatStorageModeError() {
    }

    function transform(error) {
      if (!error) {
        return;
      }
      var errorCode =
          error.indexOf("Storage mode cannot be set to") > -1 ? 1 :
          error.indexOf("storageMode must be one of") > -1 ? 2 :
          0;
      switch (errorCode) {
      case 1:
        return "please choose another index storage mode";
      case 2:
        return "please choose an index storage mode";
      default:
        return error;
      }
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnPrepareQuantity =
  (function () {
    "use strict";

    MnPrepareQuantity.annotations = [
      new ng.core.Pipe({
        name: "mnPrepareQuantity"
      })
    ];

    MnPrepareQuantity.prototype.transform = transform;

    return MnPrepareQuantity;

    function MnPrepareQuantity() {
    }

    function transform(value, K) {
      K = K || 1024;

      var M = K*K;
      var G = M*K;
      var T = G*K;

      if (K !== 1024 && K !== 1000) {
        throw new Error("Unknown number system");
      }

      var t = _.detect([[T,'T'],[G,'G'],[M,'M'],[K,'K']], function (t) {
        return value >= t[0];
      }) || [1, ''];

      if (K === 1024) {
        t[1] += 'B';
      }

      return t;
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnFormatUptime =
  (function () {
    "use strict";

    MnFormatUptime.annotations = [
      new ng.core.Pipe({
        name: "mnFormatUptime"
      })
    ];

    MnFormatUptime.prototype.transform = transform;

    return MnFormatUptime;

    function MnFormatUptime() {
    }
    function transform(seconds, precision) {
      precision = precision || 8;

      var arr = [[86400, "days", "day"],
                 [3600, "hours", "hour"],
                 [60, "minutes", "minute"],
                 [1, "seconds", "second"]];

      var rv = [];

      _.each(arr, function (item) {
        var period = item[0];
        var value = (seconds / period) >> 0;
        seconds -= value * period;
        if (value) {
            rv.push(String(value) + ' ' + (value > 1 ? item[1] : item[2]));
          }
          return !!--precision;
        });
        return rv.join(', ');
    }
  })();

var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnFormatQuantity =
  (function () {
    "use strict";

    MnFormatQuantity.annotations = [
      new ng.core.Pipe({
        name: "mnFormatQuantity"
      })
    ];

    MnFormatQuantity.parameters = [
      mn.pipes.MnPrepareQuantity,
      ng.common.DecimalPipe
    ];

    MnFormatQuantity.prototype.transform = transform;

    return MnFormatQuantity;

    function MnFormatQuantity(mnPrepareQuantity, decimalPipe) {
      this.mnPrepareQuantity = mnPrepareQuantity;
      this.decimalPipe = decimalPipe;
    }

    function transform(value, spacing, numberSystem) {
      if (!value && !_.isNumber(value)) {
        return value;
      }
      if (spacing == null) {
        spacing = '';
      }
      if (numberSystem === 1000 && value <= 9999 && value % 1 === 0) { // MB-11784
        return value;
      }

      var t = this.mnPrepareQuantity.transform(value, numberSystem);
      return [this.decimalPipe.transform(value/t[0]), spacing, t[1]].join('');
    }
  })();


var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnLeftEllipsis =
  (function () {
    "use strict";

    MnLeftEllipsis.annotations = [
      new ng.core.Pipe({
        name: "mnLeftEllipsis"
      })
    ];

    MnLeftEllipsis.prototype.transform = transform;

    return MnLeftEllipsis;

    function MnLeftEllipsis() {}

    function transform(text, length) {
      if (!text) {
        return;
      }
      if (length <= 3) {
        // asking for stupidly short length will cause this to do
        // nothing
        return text;
      }
      if (text.length > length) {
        return "..." + text.slice(3-length);
      }
      return text;
    }
  })();


var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnFormatWarmupMessage =
  (function () {
    "use strict";

    MnFormatWarmupMessage.annotations = [
      new ng.core.Pipe({
        name: "mnFormatWarmupMessage"
      })
    ];

    MnFormatWarmupMessage.prototype.transform = transform;

    return MnFormatWarmupMessage;

    function MnFormatWarmupMessage() {}

    function transform(task) {
      var message = task.stats.ep_warmup_state;
      switch (message) {
      case "loading keys":
        return message + " (" + task.stats.ep_warmup_key_count + " / " + task.stats.ep_warmup_estimated_key_count + ")";
      case "loading data":
        return message + " (" + task.stats.ep_warmup_value_count + " / " + task.stats.ep_warmup_estimated_value_count + ")";
      default:
        return message;
      }
    }
  })();


var mn = mn || {};
mn.pipes = mn.pipes || {};
mn.pipes.MnBucketsType =
  (function () {
    "use strict";

    MnBucketsType.annotations = [
      new ng.core.Pipe({
        name: "mnBucketsType"
      })
    ];

    MnBucketsType.prototype.transform = transform;

    return MnBucketsType;

    function MnBucketsType() {
    }

    function transform(type) {
      switch (type) {
      case "membase":
        return "Couchbase";
      case "ephemeral":
      case "memcached":
        return type.charAt(0).toUpperCase() + type.slice(1);
      }
    }
  })();

mn.pipes.MnIsMembase = mn.helper.createBucketTypePipe("membase")
mn.pipes.MnIsEphemeral = mn.helper.createBucketTypePipe("ephemeral");
mn.pipes.MnIsMemcached = mn.helper.createBucketTypePipe("memcached");


var mn = mn || {};
mn.modules = mn.modules || {};
mn.modules.MnPipesModule =
  (function () {
    "use strict";

    MnPipesModule.annotations = [
      new ng.core.NgModule({
        declarations: [
          mn.pipes.MnFormatStorageModeError,
          mn.pipes.MnParseVersion,
          mn.pipes.MnPrettyVersion,
          mn.pipes.MnFormatProgressMessage,
          mn.pipes.MnFormatQuantity,
          mn.pipes.MnBucketsType,
          mn.pipes.MnIsMembase,
          mn.pipes.MnIsMemcached,
          mn.pipes.MnIsEphemeral,
          mn.pipes.MnFormatWarmupMessage,
          mn.pipes.MnObjectKeys,
          mn.pipes.MnFormatUptime,
          mn.pipes.MnLeftEllipsis
        ],
        exports: [
          mn.pipes.MnLeftEllipsis,
          mn.pipes.MnFormatUptime,
          mn.pipes.MnFormatStorageModeError,
          mn.pipes.MnParseVersion,
          mn.pipes.MnPrettyVersion,
          mn.pipes.MnFormatProgressMessage,
          mn.pipes.MnFormatQuantity,
          mn.pipes.MnBucketsType,
          mn.pipes.MnIsMembase,
          mn.pipes.MnIsMemcached,
          mn.pipes.MnIsEphemeral,
          mn.pipes.MnFormatWarmupMessage,
          mn.pipes.MnObjectKeys
        ],
        imports: [],
        providers: [
          mn.pipes.MnParseVersion,
          mn.pipes.MnPrettyVersion,
          mn.pipes.MnPrepareQuantity,
          mn.pipes.MnBytesToMB,
          mn.pipes.MnFormatQuantity,
          ng.common.DecimalPipe,
        ]
      })
    ];

    return MnPipesModule;

    function MnPipesModule() {
    }
  })();
