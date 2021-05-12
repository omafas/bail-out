/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Pipe } from '../web_modules/@angular/core.js';
import { DecimalPipe } from '../web_modules/@angular/common.js';
import { MnHelperService } from './mn.helper.service.js';
import { is } from '../web_modules/ramda.js';

export {
  MnParseVersion,
  MnMBtoBytes,
  MnBytesToMB,
  MnObjectKeys,
  MnPrettyVersion,
  MnFormatProgressMessage,
  MnFormatStorageModeError,
  MnPrepareQuantity,
  MnFormatUptime,
  MnFormatQuantity,
  MnLeftEllipsis,
  MnFormatWarmupMessage,
  MnBucketsType
}

class MnParseVersion {
  static annotations = [
    new Pipe({name: "mnParseVersion"})
  ]

  transform(str) {
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
}


class MnMBtoBytes {
  static annotations = [
    new Pipe({name: "mnMBtoBytes"})
  ]

  static parameters = [
    MnHelperService
  ]

  constructor(mnHelperService) {
    this.IEC = mnHelperService.IEC;
  }

  transform(MB) {
    return MB * this.IEC.Mi;
  }
}


class MnBytesToMB {
  static annotations = [
    new Pipe({name: "mnBytesToMB"})
  ]

  static parameters = [
    MnHelperService
  ]

  constructor(mnHelperService) {
    this.IEC = mnHelperService.IEC;
  }

  transform(bytes) {
    return Math.floor(bytes / this.IEC.Mi);
  }
}


class MnObjectKeys {
  static annotations = [
    new Pipe({name: "mnObjectKeys"})
  ]

  transform(object) {
    if (object) {
      return Object.keys(object);
    } else {
      return [];
    }
  }
}


class MnPrettyVersion {
  static annotations = [
    new Pipe({name: "mnPrettyVersion"})
  ]

  static parameters = [
    MnParseVersion
  ]

  constructor(mnParseVersion) {
    this.mnParseVersion = mnParseVersion;
  }

  transform(str, full) {
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
}


class MnFormatProgressMessage {
  static annotations = [
    new Pipe({name: "mnFormatProgressMessage"})
  ]

  addNodeCount(perNode) {
    var serversCount = Object.keys(task.perNode || {}).length;
    return serversCount + " " + (serversCount === 1 ? 'node' : 'nodes');
  }

  transform(task) {
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
      return "collecting logs from " + this.addNodeCount(task.perNode);
    case "rebalance":
      return (task.subtype == 'gracefulFailover') ?
        "failing over 1 node" :
        ("rebalancing " + this.addNodeCount(task.perNode));
    }
  }
}


class MnFormatStorageModeError {
  static annotations = [
    new Pipe({name: "mnFormatStorageModeError"})
  ]

  transform(error) {
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
}



class MnPrepareQuantity {
  static annotations = [
    new Pipe({name: "mnPrepareQuantity"})
  ]

  transform(value, K) {
    K = K || 1024;

    var M = K*K;
    var G = M*K;
    var T = G*K;

    if (K !== 1024 && K !== 1000) {
      throw new Error("Unknown number system");
    }

    var t = ([[T,'T'],[G,'G'],[M,'M'],[K,'K']]).find(function (t) {
      return value >= t[0];
    }) || [1, ''];

    if (K === 1024) {
      t[1] += 'B';
    }

    return t;
  }
}



class MnFormatUptime {
  static annotations = [
    new Pipe({name: "mnFormatUptime"})
  ]

  transform(seconds, precision) {
    precision = precision || 8;

    var arr = [[86400, "days", "day"],
               [3600, "hours", "hour"],
               [60, "minutes", "minute"],
               [1, "seconds", "second"]];

    var rv = [];

    arr.forEach(function (item) {
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
}



class MnFormatQuantity {
  static annotations = [
    new Pipe({name: "mnFormatQuantity"})
  ]

  static parameters = [
    MnPrepareQuantity,
    DecimalPipe
  ]

  constructor(mnPrepareQuantity, decimalPipe) {
    this.mnPrepareQuantity = mnPrepareQuantity;
    this.decimalPipe = decimalPipe;
  }

  transform(value, spacing, numberSystem) {
    if (!value && !is(Number, value)) {
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
}



class MnLeftEllipsis {
  static annotations = [
    new Pipe({name: "mnLeftEllipsis"})
  ]

  transform(text, length) {
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
}



class MnFormatWarmupMessage {
  static annotations = [
    new Pipe({name: "mnFormatWarmupMessage"})
  ]

  transform(task) {
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
}



class MnBucketsType {
  static annotations = [
    new Pipe({name: "mnBucketsType"})
  ]

  transform(type) {
    switch (type) {
    case "membase":
      return "Couchbase";
    case "ephemeral":
    case "memcached":
      return type.charAt(0).toUpperCase() + type.slice(1);
    }
  }
}
