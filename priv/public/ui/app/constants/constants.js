/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

let bucketsFormConfiguration = {
  name: '',
  conflictResolutionType: 'seqno',
  bucketType: 'membase',
  evictionPolicy: 'valueOnly',
  evictionPolicyEphemeral: 'noEviction',
  replicaNumber: "1",
  replicaIndex: "0",
  threadsNumber: "3",
  flushEnabled: "0",
  ramQuota: "0",
  uri: '/pools/default/buckets',
  purgeInterval: "3",
  compressionMode: "passive",
  durabilityMinLevel: "none",
  maxTTL: 0,
  storageBackend: 'couchstore',
  fragmentationPercentage: 50
};

let daysOfWeek = [
  'Monday',
  'Tuesday',
  'Wednesday',
  'Thursday',
  'Friday',
  'Saturday',
  'Sunday'
];

let knownAlerts = [
  'auto_failover_node',
  'auto_failover_maximum_reached',
  'auto_failover_other_nodes_down',
  'auto_failover_cluster_too_small',
  'auto_failover_disabled',
  'ip',
  'disk',
  'overhead',
  'ep_oom_errors',
  'ep_item_commit_failed',
  'audit_dropped_events',
  'indexer_ram_max_usage',
  'ep_clock_cas_drift_threshold_exceeded',
  'communication_issue',
  'time_out_of_sync'
];

let timeUnitToSeconds = {
  minute: 60,
  hour: 3600,
  day: 86400,
  week: 691200,
  month: 2678400,
  year: 31622400
};

let docsLimit = 1000;

let docBytesLimit = 256 * 1024;

let viewsPerPageLimit = 6;

let IEC = {
  Ki: 1024,
  Mi: 1048576,
  Gi: 1073741824
};

export {
  bucketsFormConfiguration,
  daysOfWeek,
  knownAlerts,
  timeUnitToSeconds,
  docsLimit,
  docBytesLimit,
  viewsPerPageLimit,
  IEC
};
