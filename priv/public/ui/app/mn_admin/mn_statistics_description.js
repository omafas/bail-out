/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var labelOperators = {
  "@kv-.kv_dcp_backoff_views+indexes.connection_type": "=~",
  "@kv-.kv_dcp_connection_count_views+indexes.connection_type": "=~",
  "@kv-.kv_dcp_items_remaining_views+indexes.connection_type": "=~",
  "@kv-.kv_dcp_items_sent_views+indexes.connection_type": "=~",
  "@kv-.kv_dcp_producer_count_views+indexes.connection_type": "=~",
  "@kv-.kv_dcp_total_data_size_bytes_views+indexes.connection_type": "=~",

  "@kv-.kv_dcp_backoff_other.connection_type": "not_any",
  "@kv-.kv_dcp_producer_count_other.connection_type": "not_any",
  "@kv-.kv_dcp_connection_count_other.connection_type": "not_any",
  "@kv-.kv_dcp_items_remaining_other.connection_type": "not_any",
  "@kv-.kv_dcp_items_sent_other.connection_type": "not_any",
  "@kv-.kv_dcp_total_data_size_bytes_other.connection_type": "not_any"
};

var compat65 = get65CompatDesc();

var compat70 = get70CompatDesc();

var mapping70 = get70Mapping();

var mapping65 = get65Mapping();

var compat70Combined = propertiesToArray(compat65.stats)
    .concat(propertiesToArray(compat70.stats))
    .reduce((acc, statPath) => {
      let statPath70 = mapping65[statPath] || statPath;
      let config = getStatAdditionalConfig(statPath70);
      let path = statPath70.split(".");
      let parent = acc;
      path.forEach((key, index) => {
        if (index == (path.length - 1)) {
          parent[key] = Object.assign({
            nodesAggregation: "sum",
            metric: {name: key}
          }, readByPath(statPath, !mapping65[statPath]), config);
        } else {
          parent[key] = parent[key] || {};
          parent = parent[key];
        }
      });

      return acc;
    }, {});

//per @items Modifier
var stats70LabelsModifier = {
  "eventing": (cfg) => {
    if (!cfg.metric["eventing"]) {
      delete cfg.metric["eventing"];
      return cfg;
    }

    cfg.metric["functionName"] = cfg.metric["eventing"];
    delete cfg.metric["eventing"];
    return cfg;
  },

  "fts": (cfg) => {
    //metric[service] indicates per item stats
    if (!cfg.metric["fts"]) {
      delete cfg.metric["fts"];
      return cfg;
    }
    let ids = cfg.metric["fts"].split("/");
    let index = ids[0] == "fts" ? 1 : 0;
    cfg.metric["index"] = ids[index];
    delete cfg.metric["fts"];
    return cfg;
  },

  "xdcr": (cfg) => {
    if (!cfg.metric["xdcr"]) {
      delete cfg.metric["xdcr"];
      return cfg;
    }
    //cfg is constructed in mnStatisticsNewService.packStatsConfig function
    let ids = cfg.metric["xdcr"].split("/");
    let index = ids[0] == "replications" ? 1 : 0;
    cfg.metric["targetClusterUUID"] = ids[index];
    cfg.metric["sourceBucketName"] = ids[index + 1];
    cfg.metric["targetBucketName"]  = ids[index + 2];
    delete cfg.metric.bucket;
    delete cfg.metric["xdcr"];
    return cfg;
  },
  "index": (cfg) => {
    //metric[service] indicates per item stats
    if (!cfg.metric["index"]) {
      delete cfg.metric["index"];
      return cfg;
    }
    let ids = cfg.metric["index"].split("/");
    let index = ids[0] == "index" ? 1 : 0;
    cfg.metric["index"] = ids[index];
    return cfg;
  },
  "kv": (cfg) => {
    if (!cfg.metric["kv"]) {
      delete cfg.metric["kv"];
      return cfg;
    }
    let name = cfg.metric.name;
    let ids = cfg.metric["kv"].split("/");

    if (ids[0] == "views") {
      cfg.metric["name"] = ("couch_views_" + (name == "accesses" ? "ops" : name));
    } else {
      if (name == "accesses") {
        cfg.metric["name"] = "spatial_views_ops";
      } else {
        cfg.metric["name"] = "couch_spatial_" + name;
      }
    }

    delete cfg.metric["kv"];

    cfg.metric.signature = ids[1];
    return cfg;
  }
}

let service = {
  "7.0": {
    "kvGroups": Object.keys(compat65.kvGroups).reduce((acc, group) => {
      acc[group] = compat65.kvGroups[group].reduce((acc, stat65) => {
        let descPath70 = mapping65["@kv-." + stat65].split(".");
        if (descPath70[0] == "@kv-") {
          acc.push(descPath70[1]);
        }
        return acc;
      }, []);
      return acc;
    },{}),
    "stats": compat70Combined
  },
  "6.5": compat65,
  mapping70: function (name) {
    return mapping70[name] || name;
  },
  mapping65: function (name) {
    return mapping65[name] || name;
  },
  maybeGetLabelsModifier: function (service) {
    return stats70LabelsModifier[service];
  },
  maybeGetLabelOperator: function (statNamePlusLabelName) {
    return labelOperators[statNamePlusLabelName];
  }
};

export default service;

function propertiesToArray(obj) {
  let isObject = val => val !== null && typeof val === "object" && !val.title;
  let addDelimiter = (a, b) => a ? (a + "." + b) : b;

  let paths = (obj = {}, head = '') => {
    return Object.entries(obj).reduce((acc, [key, value]) => {
      let fullPath = addDelimiter(head, key);
      return isObject(value) ? acc.concat(paths(value, fullPath)) : acc.concat(fullPath);
    }, []);
  }
  return paths(obj);
}

function getStatAdditionalConfig(statName) {
  switch (statName) {
  case "@kv-.kv_ep_active_ahead_exceptions":
    return {metric: {name: "kv_ep_ahead_exceptions", state:"active"}, applyFunctions: ["irate"]};
  case "@kv-.kv_ep_replica_ahead_exceptions":
    return {metric: {name: "kv_ep_ahead_exceptions", state:"replica"}, applyFunctions: ["irate"]};

  case "@kv-.kv_vb_avg_replica_queue_age_seconds":
    return {metric: {name: "kv_vb_avg_queue_age_seconds", state: "replica"},
            nodesAggregation: "special"};
  case "@kv-.kv_vb_avg_active_queue_age_seconds":
    return {metric: {name: "kv_vb_avg_queue_age_seconds", state: "active"},
            nodesAggregation: "special"};
  case "@kv-.kv_vb_avg_pending_queue_age_seconds":
    return {metric: {name: "kv_vb_avg_queue_age_seconds", state: "pending"},
            nodesAggregation: "special"};

  case "@kv-.kv_vb_active_eject":
    return {metric: {name: "kv_vb_eject", state: "active"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_active_ops_create":
    return {metric: {name: "kv_vb_ops_create", state: "active"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_active_queue_drain":
    return {metric: {name: "kv_vb_queue_drain", state: "active"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_active_queue_fill":
    return {metric: {name: "kv_vb_queue_fill", state: "active"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_active_sync_write_accepted_count":
    return {metric: {name: "kv_vb_sync_write_accepted_count", state: "active"},
            applyFunctions: ["irate"]};
  case "@kv-.kv_vb_active_sync_write_committed_count":
    return {metric: {name: "kv_vb_sync_write_committed_count", state: "active"},
            applyFunctions: ["irate"]};
  case "@kv-.kv_vb_active_sync_write_aborted_count":
    return {metric: {name: "kv_vb_sync_write_aborted_count", state: "active"},
            applyFunctions: ["irate"]};
  case "@kv-.kv_vb_pending_eject":
    return {metric: {name: "kv_vb_eject", state: "pending"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_pending_ops_create":
    return {metric: {name: "kv_vb_ops_create", state: "pending"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_pending_queue_drain":
    return {metric: {name: "kv_vb_queue_drain", state: "pending"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_pending_queue_fill":
    return {metric: {name: "kv_vb_queue_fill", state: "pending"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_replica_eject":
    return {metric: {name: "kv_vb_eject", state: "replica"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_replica_ops_create":
    return {metric: {name: "kv_vb_ops_create", state: "replica"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_replica_queue_drain":
    return {metric: {name: "kv_vb_queue_drain", state: "replica"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_replica_queue_fill":
    return {metric: {name: "kv_vb_queue_fill", state: "replica"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_active_queue_size":
    return {metric: {name: "kv_vb_queue_size", state: "active"}};
  case "@kv-.kv_vb_pending_queue_size":
    return {metric: {name: "kv_vb_queue_size", state: "pending"}};
  case "@kv-.kv_vb_replica_curr_items":
    return {metric: {name: "kv_vb_curr_items", state: "replica"}};
  case "@kv-.kv_vb_pending_curr_items":
    return {metric: {name: "kv_vb_curr_items", state: "pending"}};
  case "@kv-.kv_vb_replica_queue_size":
    return {metric: {name: "kv_vb_queue_size", state: "replica"}};
  case "@kv-.kv_vb_active_itm_memory_bytes":
    return {metric: {name: "kv_vb_itm_memory_bytes", state: "active"}};
  case "@kv-.kv_vb_active_meta_data_memory_bytes":
    return {metric: {name: "kv_vb_meta_data_memory_bytes", state: "active"}};
  case "@kv-.kv_vb_pending_itm_memory_bytes":
    return {metric: {name: "kv_vb_itm_memory_bytes", state: "pending"}};
  case "@kv-.kv_vb_pending_meta_data_memory_bytes":
    return {metric: {name: "kv_vb_meta_data_memory_bytes", state: "pending"}};
  case "@kv-.kv_vb_replica_itm_memory_bytes":
    return {metric: {name: "kv_vb_itm_memory_bytes", state: "replica"}};
  case "@kv-.kv_vb_replica_meta_data_memory_bytes":
    return {metric: {name: "kv_vb_meta_data_memory_bytes", state: "replica"}};

  case "@kv-.kv_avg_active_timestamp_drift_seconds":
    return {metric: {name: "kv_avg_timestamp_drift_seconds", state: "active"},
            nodesAggregation: "special"};
  case "@kv-.kv_avg_replica_timestamp_drift_seconds":
    return {metric: {name: "kv_avg_timestamp_drift_seconds", state: "replica"},
            nodesAggregation: "special"};
  case "@kv-.kv_vb_pending_resident_items_ratio":
    return {metric: {name: "kv_vb_resident_items_ratio", state: "pending"},
            nodesAggregation: "special"};
  case "@kv-.kv_vb_active_resident_items_ratio":
    return {metric: {name: "kv_vb_resident_items_ratio", state: "active"},
            nodesAggregation: "special"};
  case "@kv-.kv_vb_replica_resident_items_ratio":
    return {metric: {name: "kv_vb_resident_items_ratio", state: "replica"},
            nodesAggregation: "special"};

  case "@xdcr-.@items.xdcr_percent_completeness":
  case "@kv-.couch_views_fragmentation":
    return {nodesAggregation: "special", applyFunctions: ["sum"]};

  case "@query.n1ql_avg_req_time":
  case "@query.n1ql_avg_svc_time":
  case "@query.n1ql_avg_response_size":
  case "@query.n1ql_avg_result_count":
  case "@index.index_ram_percent":
  case "@index.index_remaining_ram":
  case "@index-.@items.index_num_docs_pending_and_queued":
  case "@index-.@items.index_cache_miss_ratio":
  case "@index-.index_fragmentation":
  case "@index-.@items.index_resident_percent":
  case "@kv-.couch_total_disk_size":
  case "@kv-.couch_docs_fragmentation":
  case "@kv-.kv_ep_resident_items_ratio":
  case "@kv-.kv_vb_avg_total_queue_age_seconds":
  case "@kv-.kv_disk_write_queue":
  case "@kv-.kv_ep_ops_create":
  case "@kv-.kv_ep_ops_update":
    return {nodesAggregation: "special"};

  case "@kv-.kv_hit_ratio":
  case "@kv-.kv_ep_cache_miss_ratio":
  case "@kv-.kv_avg_bg_wait_time_seconds":
  case "@kv-.kv_xdc_ops":
    return {nodesAggregation: "special", applyFunctions: ["irate"]};

  case "@system.sys_cpu_utilization_rate":
  case "@cbas.cbas_system_load_average":
    return {nodesAggregation: "avg"};

  case "@index-.@items.index_fragmentation":
    return {nodesAggregation: "special", applyFunctions: ["sum"]};

  case "@cbas-.cbas_incoming_records_count":
  case "@cbas-.cbas_failed_to_parse_records_count":
  case "@index-.index_num_docs_indexed":
  case "@index-.index_num_requests":
  case "@fts-.fts_total_bytes_indexed":
  case "@fts-.fts_total_queries":
  case "@kv-.kv_ops":
  case "@kv-.couch_views_ops":
  case "@xdcr-.@items.xdcr_data_replicated_bytes":
  case "@xdcr-.@items.xdcr_docs_filtered_total":
  case "@xdcr-.@items.xdcr_docs_checked_total":
  case "@xdcr-.@items.xdcr_docs_opt_repd_total":
  case "@xdcr-.@items.xdcr_docs_received_from_dcp_total":
    return {applyFunctions: ["irate", "sum"]};

    //sum of get, store, delete
  case "@kv-.kv_collection_ops":
    return {applyFunctions: ["irate", "sum"], metric: {name: "kv_collection_ops"}};

  case "@cbas.cbas_gc_count_total":
  case "@cbas.cbas_gc_time_milliseconds_total":
  case "@kv-.kv_read_bytes":
  case "@kv-.kv_written_bytes":
  case "@kv-.kv_ep_bg_fetched":
  case "@kv-.kv_ep_diskqueue_drain":
  case "@kv-.kv_ep_diskqueue_fill":
  case "@kv-.kv_ep_num_value_ejects":
  case "@kv-.kv_ep_tmp_oom_errors":
  case "@kv-.@items.accesses":
  case "@index-.@items.index_num_requests":
  case "@index-.@items.index_num_docs_indexed":
  case "@index-.@items.index_num_rows_returned":
  case "@fts-.@items.fts_total_bytes_indexed":
  case "@fts-.@items.fts_total_bytes_query_results":
  case "@fts-.@items.fts_total_compaction_written_bytes":
  case "@fts-.@items.fts_total_queries":
  case "@fts-.@items.fts_total_queries_error":
  case "@fts-.@items.fts_total_queries_slow":
  case "@fts-.@items.fts_total_queries_timeout":
  case "@fts-.@items.fts_total_term_searchers":
  case "@query.n1ql_errors":
  case "@query.n1ql_invalid_requests":
  case "@query.n1ql_requests":
  case "@query.n1ql_selects":
  case "@query.n1ql_warnings":
  case "@system.cm_rest_request_leaves_total":
  case "@cbas.cbas_io_reads_total":
  case "@cbas.cbas_io_writes_total":
    return {applyFunctions: ["irate"]};

  case "@query.n1ql_requests_1000ms":
  case "@query.n1ql_requests_250ms":
  case "@query.n1ql_requests_5000ms":
  case "@query.n1ql_requests_500ms":
    return {timeWindow: "1m", applyFunctions: ["increase"]};

  case "@fts-.fts_num_bytes_used_disk":
  case "@fts-.fts_num_files_on_disk":
  case "@index-.index_num_rows_returned":
  case "@xdcr-.@items.xdcr_docs_failed_cr_source_total":
  case "@xdcr-.@items.xdcr_docs_written_total":
  case "@xdcr-.@items.xdcr_changes_left_total":
  case "@xdcr-.@items.xdcr_wtavg_docs_latency_seconds":
  case "@xdcr-.@items.xdcr_wtavg_meta_latency_seconds":
    return {applyFunctions: ["sum"]};

  case "@xdcr-.xdcr_changes_left_total":
    return {metric: {name: "xdcr_changes_left_total"}, applyFunctions: ["sum"], bucketLabel: "sourceBucketName"};

  case "@cbas-.cbas_incoming_records_count_total":
    return {metric: {name: "cbas_incoming_records_count"}, applyFunctions: ["sum"]};

  case "@cbas-.cbas_failed_to_parse_records_count_total":
    return {metric: {name: "cbas_failed_to_parse_records_count"}, applyFunctions: ["sum"]};

  case "@xdcr-.@items.xdcr_rate_replicated_docs_per_second":
    return {metric: {name: "xdcr_docs_written_total"}, applyFunctions: ["irate", "sum"]};

  case "@kv-.kv_memory_used_bytes":
    return {metric: {for: "hashtable"}};

  case "@kv-.kv_avg_disk_time_seconds_update":
    return {metric: {name: "kv_avg_disk_time_seconds", op: "update"},
            nodesAggregation: "special", applyFunctions: ["irate"]};

  case "@kv-.kv_avg_disk_time_seconds_commit":
    return {metric: {name: "kv_avg_disk_time_seconds", op: "commit"},
            nodesAggregation: "special", applyFunctions: ["irate"]};

  case "@kv-.kv_curr_connections":
    return {bucket: null};

  case "@eventing.eventing_timeout_count":
    return {applyFunctions: ["sum"], bucket: null};

  case "@eventing.eventing_failed_count":
    return {applyFunctions: ["sum"], bucket: null, nodesAggregation: "special"};

  case "@eventing.eventing_processed_count":
    return {applyFunctions: ["irate", "sum"], bucket: null, nodesAggregation: "special"};

  case "@eventing-.@items.eventing_processed_count_rate":
    return {applyFunctions: ["irate"], bucket: null, metric: {name: "eventing_processed_count"},
            nodesAggregation: "special"};
  case "@eventing-.@items.eventing_failed_count_rate":
    return {applyFunctions: ["irate"], bucket: null, metric: {name: "eventing_failed_count"},
            nodesAggregation: "special"};
  case "@eventing-.@items.eventing_timeout_count_rate":
    return {applyFunctions: ["irate"], bucket: null, metric: {name: "eventing_timeout_count"}};

  case "@eventing-.@items.eventing_failed_count":
    return {bucket: null, nodesAggregation: "special"};

  case "@eventing-.@items.eventing_processed_count":
  case "@eventing-.@items.eventing_timeout_count":
    return {bucket: null};

  case "@kv-.kv_cas_hits":
    return {metric: {name: "kv_ops", op: "cas", result: "hit"}, applyFunctions: ["irate"]};
  case "@kv-.kv_cas_badval":
    return {metric: {name: "kv_ops", op: "cas", result: "badval"}, applyFunctions: ["irate"]};
  case "@kv-.kv_cas_misses":
    return {metric: {name: "kv_ops", op: "cas", result: "miss"}, applyFunctions: ["irate"]};
  case "@kv-.kv_decr_hits":
    return {metric: {name: "kv_ops", op: "decr", result: "hit"}, applyFunctions: ["irate"]};
  case "@kv-.kv_decr_misses":
    return {metric: {name: "kv_ops", op: "decr", result: "miss"}, applyFunctions: ["irate"]};
  case "@kv-.kv_delete_hits":
    return {metric: {name: "kv_ops", op: "delete", result: "hit"}, applyFunctions: ["irate"]};
  case "@kv-.kv_delete_misses":
    return {metric: {name: "kv_ops", op: "delete", result: "miss"}, applyFunctions: ["irate"]};
  case "@kv-.kv_incr_hits":
    return {metric: {name: "kv_ops", op: "incr", result: "hit"}, applyFunctions: ["irate"]};
  case "@kv-.kv_incr_misses":
    return {metric: {name: "kv_ops", op: "incr", result: "miss"}, applyFunctions: ["irate"]};
  case "@kv-.kv_get_hits":
    return {metric: {name: "kv_ops", op: "get", result: "hit"}, applyFunctions: ["irate"]};
  case "@kv-.kv_get_misses":
    return {metric: {name: "kv_ops", op: "get", result: "miss"}, applyFunctions: ["irate"]};
  case "@kv-.kv_cmd_get":
    return {metric: {name: "kv_ops", op: "get"}, applyFunctions: ["irate", "sum"]};
  case "@kv-.kv_cmd_set":
    return {metric: {name: "kv_ops", op: "set"}, applyFunctions: ["irate", "sum"]};
  case "@kv-.kv_cmd_total_gets":
    return {metric: {name: "kv_ops", op: "get"}, applyFunctions: ["irate", "sum"], bucket: null};
  case "@kv-.kv_ep_num_ops_del_meta":
    return {metric: {name: "kv_ops", op: "del_meta"}, applyFunctions: ["irate"]};
  case "@kv-.kv_ep_num_ops_get_meta":
    return {metric: {name: "kv_ops", op: "get_meta"}, applyFunctions: ["irate"]};
  case "@kv-.kv_ep_num_ops_set_meta":
    return {metric: {name: "kv_ops", op: "set_meta"}, applyFunctions: ["irate"]};
  case "@kv-.kv_vb_active_num":
    return {metric: {name: "kv_num_vbuckets", state: "active"}};
  case "@kv-.kv_vb_pending_num":
    return {metric: {name: "kv_num_vbuckets", state: "pending"}};
  case "@kv-.kv_vb_replica_num":
    return {metric: {name: "kv_num_vbuckets", state: "replica"}};

  default:
    if (statName.includes("@kv-.kv_dcp_")) {
      let name = statName.split(".").pop().split("_");
      let type = name.pop();
      let rv = {metric: {name: name.join("_")}};
      let applyFunctions = [];

      if (statName.includes("@kv-.kv_dcp_items_sent_") ||
          statName.includes("@kv-.kv_dcp_total_data_size_bytes_")) {
        applyFunctions.push("irate");
      }
      if (statName.includes("@kv-.kv_dcp_backoff_cbas") ||
          statName.includes("@kv-.kv_dcp_backoff_eventing")) {
        applyFunctions.push("irate");
      }
      if (type == "other") {
        type = ["replication","xdcr","fts","cbas",
                "eventing","secidx","mapreduce_view","spatial_view"];
        applyFunctions.push("sum");
      }
      if (type == "views+indexes") {
        type = "mapreduce_view|spatial_view|secidx|fts";
        applyFunctions.push("sum");
      }
      rv.metric.connection_type = type;
      rv.applyFunctions = applyFunctions;
      return rv;
    }
    if (statName.includes("@index-.index_")) {
      return {applyFunctions: ["sum"]};
    }
    return {};
  }
}


function readByPath(descPath, is70Stat) {
  var paths = descPath.split('.');
  var statsDesc = is70Stat ? compat70.stats : compat65.stats;
  var i;

  for (i = 0; i < paths.length; ++i) {
    if (statsDesc[paths[i]] == undefined) {
      return undefined;
    } else {
      statsDesc = statsDesc[paths[i]];
    }
  }
  return statsDesc;
}

function get65Mapping() {
  let mapping = get70Mapping();
  return Object.keys(mapping).reduce((acc, key) => {
    acc[mapping[key]] = key;
    return acc;
  }, {});
}

function get70Mapping() {
  var rv = {};
  Object.keys(compat65.stats["@index-"]["@items"]).forEach(key => {
    let name = "@index-.@items." + key;
    let name70 = "@index-.@items.";
    if (!key.includes("index_")) {
      name70 += "index_";
    }
    name70 += key;
    rv[name70] = rv[name70] || name;
  });

  Object.keys(compat65.stats["@index-"]).forEach(key => {
    let name = "@index-." + key;
    let name70 = "@index-." + key.replace("/", "_");
    rv[name70] = rv[name70] || name;
  });

  Object.keys(compat65.stats["@query"]).forEach(key => {
    let name = "@query." + key;
    let name70 = "@query.n1ql_";
    if (key.includes("query_")) {
      name70 += key.split("query_")[1];
    } else {
      name70 += key;
    }
    rv[name70] = rv[name70] || name;
  });

  Object.keys(compat65.stats["@fts-"]["@items"]).forEach(key => {
    let name = "@fts-.@items." + key;
    let name70 = "@fts-.@items.fts_" + key;
    rv[name70] = rv[name70] || name;
  });

  return Object.assign(rv, {
    "@system.sys_cpu_utilization_rate": "@system.cpu_utilization_rate",
    "@system.sys_hibernated_requests": "@system.hibernated_requests",
    "@system.sys_hibernated_waked": "@system.hibernated_waked",
    "@system.sys_mem_actual_free": "@system.mem_actual_free",
    "@system.cm_rest_request_leaves_total": "@system.rest_requests",
    "@system.sys_swap_used": "@system.swap_used",

    "@cbas.cbas_disk_used_bytes_total": "@cbas.cbas_disk_used",
    "@cbas.cbas_gc_count_total": "@cbas.cbas_gc_count",
    "@cbas.cbas_gc_time_milliseconds_total": "@cbas.cbas_gc_time",
    "@cbas.cbas_heap_memory_used_bytes": "@cbas.cbas_heap_used",
    "@cbas.cbas_system_load_average": "@cbas.cbas_system_load_average",
    "@cbas.cbas_thread_count": "@cbas.cbas_thread_count",
    "@cbas.cbas_io_reads_total": "@cbas.cbas_io_reads", //linux only
    "@cbas.cbas_io_writes_total": "@cbas.cbas_io_writes", //linux only

    "@cbas-.cbas_incoming_records_count": "@cbas-.cbas/incoming_records_count",
    "@cbas-.cbas_incoming_records_count_total": "@cbas-.cbas/incoming_records_count_total",
    "@cbas-.cbas_failed_to_parse_records_count_total": "@cbas-.cbas/failed_at_parser_records_count_total",

    "@index.index_memory_used_total": "@index.index_memory_used",
    "@index-.index_fragmentation": "@index-.index/fragmentation",
    "@index-.@items.index_fragmentation": "@index-.@items.index_frag_percent",
    "@index.index_ram_percent": "@index.index_ram_percent",
    "@index.index_remaining_ram": "@index.index_remaining_ram",

    "@index-.@items.index_num_docs_pending_and_queued": "@index-.@items.num_docs_pending+queued",
    "@index-.@items.index_cache_miss_ratio": "@index-.@items.cache_miss_ratio",

    "@system.couch_docs_actual_disk_size": "@kv-.couch_docs_actual_disk_size",
    "@system.couch_docs_data_size": "@kv-.couch_docs_data_size",
    "@system.couch_views_actual_disk_size": "@kv-.couch_views_actual_disk_size",
    "@system.couch_views_data_size": "@kv-.couch_views_data_size",

    "@kv-.kv_memcache_evictions": "@kv-.evictions",
    "@kv-.couch_total_disk_size": "@kv-.couch_total_disk_size",
    "@kv-.couch_docs_fragmentation": "@kv-.couch_docs_fragmentation",
    "@kv-.couch_views_fragmentation": "@kv-.couch_views_fragmentation",
    "@kv-.kv_hit_ratio": "@kv-.hit_ratio",
    "@kv-.kv_ep_cache_miss_ratio": "@kv-.ep_cache_miss_rate",
    "@kv-.kv_vb_avg_pending_queue_age_seconds": "@kv-.vb_avg_pending_queue_age",
    "@kv-.kv_vb_avg_active_queue_age_seconds": "@kv-.vb_avg_active_queue_age",
    "@kv-.kv_vb_avg_replica_queue_age_seconds": "@kv-.vb_avg_replica_queue_age",
    "@kv-.kv_vb_avg_total_queue_age_seconds": "@kv-.vb_avg_total_queue_age",
    "@kv-.kv_ep_resident_items_ratio": "@kv-.ep_resident_items_rate",
    "@kv-.kv_vb_pending_resident_items_ratio": "@kv-.vb_pending_resident_items_ratio",
    "@kv-.kv_vb_active_resident_items_ratio": "@kv-.vb_active_resident_items_ratio",
    "@kv-.kv_vb_replica_resident_items_ratio": "@kv-.vb_replica_resident_items_ratio",
    "@kv-.kv_avg_disk_time_seconds_update": "@kv-.avg_disk_update_time",
    "@kv-.kv_avg_disk_time_seconds_commit": "@kv-.avg_disk_commit_time",
    "@kv-.kv_read_bytes": "@kv-.bytes_read",
    "@kv-.kv_written_bytes": "@kv-.bytes_written",
    "@kv-.kv_cas_badval": "@kv-.cas_badval",
    "@kv-.kv_cas_hits": "@kv-.cas_hits",
    "@kv-.kv_cas_misses": "@kv-.cas_misses",
    "@kv-.kv_get_hits": "@kv-.get_hits",
    "@kv-.kv_get_misses": "@kv-.get_misses",
    "@kv-.kv_cmd_get": "@kv-.cmd_get",
    "@kv-.kv_cmd_set": "@kv-.cmd_set",
    "@kv-.kv_cmd_total_gets": "@kv-.cmd_total_gets",
    "@kv-.couch_views_ops": "@kv-.couch_views_ops",
    "@kv-.kv_ep_num_ops_del_meta": "@kv-.ep_num_ops_del_meta",
    "@kv-.kv_ep_num_ops_get_meta": "@kv-.ep_num_ops_get_meta",
    "@kv-.kv_ep_num_ops_set_meta": "@kv-.ep_num_ops_set_meta",
    "@kv-.kv_vb_active_num": "@kv-.vb_active_num",
    "@kv-.kv_vb_pending_num": "@kv-.vb_pending_num",
    "@kv-.kv_vb_replica_num": "@kv-.vb_replica_num",
    "@kv-.@items.accesses": "@kv-.@items.accesses",
    "@kv-.@items.data_size": "@kv-.@items.data_size",
    "@kv-.@items.disk_size": "@kv-.@items.disk_size",

    "@kv-.kv_memory_used_bytes": "@kv-.ep_kv_size",

    "@kv-.kv_curr_connections": "@kv-.curr_connections",
    "@kv-.kv_curr_items": "@kv-.curr_items",
    "@kv-.kv_curr_items_tot": "@kv-.curr_items_tot",
    "@kv-.kv_decr_hits": "@kv-.decr_hits",
    "@kv-.kv_decr_misses": "@kv-.decr_misses",
    "@kv-.kv_delete_hits": "@kv-.delete_hits",
    "@kv-.kv_delete_misses": "@kv-.delete_misses",
    "@kv-.kv_incr_hits": "@kv-.incr_hits",
    "@kv-.kv_incr_misses": "@kv-.incr_misses",
    "@kv-.kv_ep_active_ahead_exceptions": "@kv-.ep_active_ahead_exceptions",
    "@kv-.kv_ep_bg_fetched": "@kv-.ep_bg_fetched",
    "@kv-.kv_ep_data_read_failed": "@kv-.ep_data_read_failed",
    "@kv-.kv_ep_data_write_failed": "@kv-.ep_data_write_failed",
    "@kv-.kv_ep_diskqueue_drain": "@kv-.ep_diskqueue_drain",
    "@kv-.kv_ep_diskqueue_fill": "@kv-.ep_diskqueue_fill",
    "@kv-.kv_ep_diskqueue_items": "@kv-.ep_diskqueue_items",
    "@kv-.kv_ep_mem_high_wat": "@kv-.ep_mem_high_wat",
    "@kv-.kv_ep_mem_low_wat": "@kv-.ep_mem_low_wat",
    "@kv-.kv_ep_num_value_ejects": "@kv-.ep_num_value_ejects",
    "@kv-.kv_ep_replica_ahead_exceptions": "@kv-.ep_replica_ahead_exceptions",
    "@kv-.kv_ep_tmp_oom_errors": "@kv-.ep_tmp_oom_errors",
    "@kv-.kv_ep_vb_total": "@kv-.ep_vb_total",
    "@kv-.kv_ops": "@kv-.ops",
    "@kv-.kv_vb_active_eject": "@kv-.vb_active_eject",
    "@kv-.kv_vb_active_ops_create": "@kv-.vb_active_ops_create",
    "@kv-.kv_vb_active_queue_drain": "@kv-.vb_active_queue_drain",
    "@kv-.kv_vb_active_queue_fill": "@kv-.vb_active_queue_fill",
    "@kv-.kv_vb_active_queue_size": "@kv-.vb_active_queue_size",
    "@kv-.kv_vb_active_sync_write_accepted_count": "@kv-.vb_active_sync_write_accepted_count",
    "@kv-.kv_vb_active_sync_write_committed_count": "@kv-.vb_active_sync_write_committed_count",
    "@kv-.kv_vb_active_sync_write_aborted_count": "@kv-.vb_active_sync_write_aborted_count",
    "@kv-.kv_vb_pending_curr_items": "@kv-.vb_pending_curr_items",
    "@kv-.kv_vb_pending_eject": "@kv-.vb_pending_eject",
    "@kv-.kv_vb_pending_ops_create": "@kv-.vb_pending_ops_create",
    "@kv-.kv_vb_pending_queue_drain": "@kv-.vb_pending_queue_drain",
    "@kv-.kv_vb_pending_queue_fill": "@kv-.vb_pending_queue_fill",
    "@kv-.kv_vb_pending_queue_size": "@kv-.vb_pending_queue_size",
    "@kv-.kv_vb_replica_curr_items": "@kv-.vb_replica_curr_items",
    "@kv-.kv_vb_replica_eject": "@kv-.vb_replica_eject",
    "@kv-.kv_vb_replica_ops_create": "@kv-.vb_replica_ops_create",
    "@kv-.kv_vb_replica_queue_drain": "@kv-.vb_replica_queue_drain",
    "@kv-.kv_vb_replica_queue_fill": "@kv-.vb_replica_queue_fill",
    "@kv-.kv_vb_replica_queue_size": "@kv-.vb_replica_queue_size",

    "@kv-.kv_mem_used_bytes": "@kv-.mem_used",
    "@kv-.kv_ep_meta_data_memory_bytes": "@kv-.ep_meta_data_memory",
    "@kv-.kv_vb_active_itm_memory_bytes": "@kv-.vb_active_itm_memory",
    "@kv-.kv_vb_active_meta_data_memory_bytes": "@kv-.vb_active_meta_data_memory",
    "@kv-.kv_vb_pending_itm_memory_bytes": "@kv-.vb_pending_itm_memory",
    "@kv-.kv_vb_pending_meta_data_memory_bytes": "@kv-.vb_pending_meta_data_memory",
    "@kv-.kv_vb_replica_itm_memory_bytes": "@kv-.vb_replica_itm_memory",
    "@kv-.kv_vb_replica_meta_data_memory_bytes": "@kv-.vb_replica_meta_data_memory",

    "@kv-.kv_avg_bg_wait_time_seconds": "@kv-.avg_bg_wait_time",
    "@kv-.kv_avg_active_timestamp_drift_seconds": "@kv-.avg_active_timestamp_drift",
    "@kv-.kv_avg_replica_timestamp_drift_seconds": "@kv-.avg_replica_timestamp_drift",
    "@kv-.kv_disk_write_queue": "@kv-.disk_write_queue",
    "@kv-.kv_ep_ops_create": "@kv-.ep_ops_create",
    "@kv-.kv_ep_ops_update": "@kv-.ep_ops_update",
    "@kv-.kv_xdc_ops": "@kv-.xdc_ops",

    "@kv-.kv_dcp_backoff_replication": "@kv-.ep_dcp_replica_backoff",
    "@kv-.kv_dcp_connection_count_replication": "@kv-.ep_dcp_replica_count",
    "@kv-.kv_dcp_items_remaining_replication": "@kv-.ep_dcp_replica_items_remaining",
    "@kv-.kv_dcp_items_sent_replication": "@kv-.ep_dcp_replica_items_sent",
    "@kv-.kv_dcp_producer_count_replication": "@kv-.ep_dcp_replica_producer_count",
    "@kv-.kv_dcp_total_data_size_bytes_replication": "@kv-.ep_dcp_replica_total_bytes",
    "@kv-.kv_dcp_backoff_xdcr": "@kv-.ep_dcp_xdcr_backoff",
    "@kv-.kv_dcp_connection_count_xdcr": "@kv-.ep_dcp_xdcr_count",
    "@kv-.kv_dcp_items_remaining_xdcr": "@kv-.ep_dcp_xdcr_items_remaining",
    "@kv-.kv_dcp_items_sent_xdcr": "@kv-.ep_dcp_xdcr_items_sent",
    "@kv-.kv_dcp_producer_count_xdcr": "@kv-.ep_dcp_xdcr_producer_count",
    "@kv-.kv_dcp_total_data_size_bytes_xdcr": "@kv-.ep_dcp_xdcr_total_bytes",
    "@kv-.kv_dcp_backoff_cbas": "@kv-.ep_dcp_cbas_backoff",
    "@kv-.kv_dcp_connection_count_cbas": "@kv-.ep_dcp_cbas_count",
    "@kv-.kv_dcp_items_remaining_cbas": "@kv-.ep_dcp_cbas_items_remaining",
    "@kv-.kv_dcp_items_sent_cbas": "@kv-.ep_dcp_cbas_items_sent",
    "@kv-.kv_dcp_producer_count_cbas": "@kv-.ep_dcp_cbas_producer_count",
    "@kv-.kv_dcp_total_data_size_bytes_cbas": "@kv-.ep_dcp_cbas_total_bytes",
    "@kv-.kv_dcp_backoff_eventing": "@kv-.ep_dcp_eventing_backoff",
    "@kv-.kv_dcp_connection_count_eventing": "@kv-.ep_dcp_eventing_count",
    "@kv-.kv_dcp_items_remaining_eventing": "@kv-.ep_dcp_eventing_items_remaining",
    "@kv-.kv_dcp_items_sent_eventing": "@kv-.ep_dcp_eventing_items_sent",
    "@kv-.kv_dcp_producer_count_eventing": "@kv-.ep_dcp_eventing_producer_count",
    "@kv-.kv_dcp_total_data_size_bytes_eventing": "@kv-.ep_dcp_eventing_total_bytes",
    "@kv-.kv_dcp_backoff_views+indexes": "@kv-.ep_dcp_views+indexes_backoff",
    "@kv-.kv_dcp_connection_count_views+indexes": "@kv-.ep_dcp_views+indexes_count",
    "@kv-.kv_dcp_items_remaining_views+indexes": "@kv-.ep_dcp_views+indexes_items_remaining",
    "@kv-.kv_dcp_items_sent_views+indexes": "@kv-.ep_dcp_views+indexes_items_sent",
    "@kv-.kv_dcp_producer_count_views+indexes": "@kv-.ep_dcp_views+indexes_producer_count",
    "@kv-.kv_dcp_total_data_size_bytes_views+indexes": "@kv-.ep_dcp_views+indexes_total_bytes",

    "@kv-.kv_dcp_backoff_other": "@kv-.ep_dcp_other_backoff",
    "@kv-.kv_dcp_connection_count_other": "@kv-.ep_dcp_other_count",
    "@kv-.kv_dcp_items_remaining_other": "@kv-.ep_dcp_other_items_remaining",
    "@kv-.kv_dcp_items_sent_other": "@kv-.ep_dcp_other_items_sent",
    "@kv-.kv_dcp_producer_count_other": "@kv-.ep_dcp_other_producer_count",
    "@kv-.kv_dcp_total_data_size_bytes_other": "@kv-.ep_dcp_other_total_bytes",

    "@xdcr-.xdcr_changes_left_total": "@xdcr-.replication_changes_left",
    "@xdcr-.@items.xdcr_changes_left_total": "@xdcr-.@items.changes_left",
    "@xdcr-.@items.xdcr_data_replicated_bytes": "@xdcr-.@items.bandwidth_usage",
    "@xdcr-.@items.xdcr_docs_failed_cr_source_total": "@xdcr-.@items.docs_failed_cr_source",
    "@xdcr-.@items.xdcr_docs_filtered_total": "@xdcr-.@items.docs_filtered",
    "@xdcr-.@items.xdcr_docs_written_total": "@xdcr-.@items.docs_written",
    "@xdcr-.@items.xdcr_docs_checked_total": "@xdcr-.@items.rate_doc_checks",
    "@xdcr-.@items.xdcr_docs_opt_repd_total": "@xdcr-.@items.rate_doc_opt_repd",
    "@xdcr-.@items.xdcr_docs_received_from_dcp_total": "@xdcr-.@items.rate_received_from_dcp",
    "@xdcr-.@items.xdcr_rate_replicated_docs_per_second": "@xdcr-.@items.rate_replicated",
    "@xdcr-.@items.xdcr_wtavg_docs_latency_seconds": "@xdcr-.@items.wtavg_docs_latency", //?s-ms
    "@xdcr-.@items.xdcr_wtavg_meta_latency_seconds": "@xdcr-.@items.wtavg_meta_latency", //?s-ms

    "@xdcr-.@items.xdcr_percent_completeness": "@xdcr-.@items.percent_completeness",

    "@fts-.fts_num_bytes_used_disk": "@fts-.fts/num_bytes_used_disk",
    "@fts-.fts_num_files_on_disk": "@fts-.fts/num_files_on_disk",
    "@fts-.fts_total_bytes_indexed": "@fts-.fts/total_bytes_indexed",
    "@fts-.fts_total_queries": "@fts-.fts/total_queries",

    "@fts.fts_num_bytes_used_ram": "@fts.fts_num_bytes_used_ram",
    "@fts.fts_total_queries_rejected_by_herder": "@fts.fts_total_queries_rejected_by_herder",
    "@fts.fts_curr_batches_blocked_by_herder": "@fts.fts_curr_batches_blocked_by_herder",

    "@eventing.eventing_timeout_count": "@eventing.eventing/timeout_count",
    "@eventing.eventing_processed_count": "@eventing.eventing/processed_count",
    "@eventing.eventing_failed_count": "@eventing.eventing/failed_count"

  });
}

function get70CompatDesc() {
  return {
    "stats": {
      "@kv-": {
        "kv_collection_item_count": null,
        "kv_collection_mem_used_bytes": null,
        "kv_collection_data_size_bytes": null,
        "kv_collection_ops": null
      },
      "@index-": {
        "@items": {
          "index_num_docs_queued": {
            unit: "number",
            title: "Index Write Queue",
            desc: "Number of documents queued to be indexed at the Indexer. Per Index."
          }
        }
      },
      "@eventing": {
        "eventing_processed_count": {
          unit: "number",
          title: "Successful Function Invocations",
          desc: "Count of times the function was invoked successfully."
        },
        "eventing_failed_count": {
          unit: "number",
          title: "Failed Function Invocations",
          desc: "Count of times the function invocation failed."
        },
        "eventing_timeout_count": {
          unit: "number",
          title: "Eventing Timeouts",
          desc: "Execution timeouts while processing mutations."
        }
      },
      "@eventing-": {
        "@items": {
          "eventing_processed_count_rate": {
            unit: "number/sec",
            title: "Successful Function Invocations Rate",
            desc: "Count of times the function was invoked successfully per second."
          },
          "eventing_failed_count_rate": {
            unit: "number/sec",
            title: "Failed Function Invocations Rate",
            desc: "Count of times the function invocation failed per second."
          },
          "eventing_timeout_count_rate": {
            unit: "number/sec",
            title: "Eventing Timeouts Rate",
            desc: "Execution timeouts while processing mutations per second."
          }
        }
      },
      "@cbas-": {
        "cbas_failed_to_parse_records_count": {
          unit: "number/sec",
          title: "Analytics Parse Fail Rate",
          desc: "Number of records Analytics failed to parse per second."
        }
      },
      "@cbas": {
        "cbas_pending_merge_ops": {
          unit: "number",
          title: "Analytics Pending Merge Operations",
          desc: "Number of pending merge operations per node."
        },
        "cbas_pending_flush_ops": {
          unit: "number",
          title: "Analytics Pending Flush Operations",
          desc: "Number of pending flush operations per node."
        }
      }
    }
  };
}

function get65CompatDesc() {
  return {
    "kvGroups": {
      "Ops":
      ["ops","cmd_get","cmd_set","hit_ratio","delete_hits","cas_hits","ep_cache_miss_rate","couch_views_ops","ep_num_ops_del_meta","ep_num_ops_get_meta","ep_num_ops_set_meta","ep_ops_create","ep_ops_update","vb_active_ops_create","vb_pending_ops_create","vb_replica_ops_create","xdc_ops","curr_connections"],
      "Memory":
      ["mem_used","ep_kv_size","ep_meta_data_memory","ep_tmp_oom_errors","ep_mem_low_wat","ep_mem_high_wat","vb_active_itm_memory","vb_active_meta_data_memory","vb_pending_itm_memory","vb_pending_meta_data_memory","vb_replica_itm_memory","vb_replica_meta_data_memory"],
      "Disk":
      ["couch_total_disk_size","couch_docs_fragmentation","couch_views_fragmentation","ep_cache_miss_rate","vb_avg_total_queue_age","avg_disk_update_time","avg_disk_commit_time","couch_docs_actual_disk_size","couch_views_actual_disk_size",
       "disk_write_queue","ep_bg_fetched","ep_data_read_failed","ep_data_write_failed","ep_num_value_ejects","ep_ops_create","ep_ops_update"],
      "vBuckets":
      ["ep_vb_total","vb_active_num","curr_items","vb_active_ops_create","vb_active_resident_items_ratio","vb_active_eject","vb_active_sync_write_accepted_count","vb_active_sync_write_committed_count","vb_active_sync_write_aborted_count","avg_active_timestamp_drift","ep_active_ahead_exceptions","vb_pending_num","vb_pending_curr_items","vb_pending_ops_create","vb_pending_resident_items_ratio","vb_pending_eject","vb_replica_num","vb_replica_curr_items","vb_replica_ops_create","vb_replica_resident_items_ratio","vb_replica_eject","avg_replica_timestamp_drift","ep_replica_ahead_exceptions"],
      "Disk Queues":
      ["ep_diskqueue_fill","ep_diskqueue_drain","ep_diskqueue_items","vb_active_queue_fill","vb_active_queue_drain","vb_active_queue_size","vb_replica_queue_fill","vb_replica_queue_drain","vb_replica_queue_size","vb_pending_queue_fill","vb_pending_queue_drain","vb_pending_queue_size"],
      "DCP Queues":
      ["ep_dcp_views+indexes_count","ep_dcp_views+indexes_producer_count","ep_dcp_views+indexes_items_remaining","ep_dcp_views+indexes_total_bytes","ep_dcp_views+indexes_backoff","ep_dcp_cbas_count","ep_dcp_cbas_producer_count","ep_dcp_cbas_items_remaining","ep_dcp_cbas_total_bytes","ep_dcp_cbas_backoff","ep_dcp_replica_count","ep_dcp_replica_producer_count","ep_dcp_replica_items_remaining","ep_dcp_replica_total_bytes","ep_dcp_replica_backoff","ep_dcp_xdcr_count","ep_dcp_xdcr_producer_count","ep_dcp_xdcr_items_remaining","ep_dcp_xdcr_total_bytes","ep_dcp_xdcr_backoff","ep_dcp_eventing_count","ep_dcp_eventing_producer_count","ep_dcp_eventing_items_remaining","ep_dcp_eventing_total_bytes","ep_dcp_eventing_backoff"]
    },

    "stats": {
      "@system":{
        "cpu_utilization_rate": {
          unit: "percent",
          title: "CPU",
          desc: "Percentage of CPU in use across all available cores on this server."
        },
        "hibernated_requests": {
          unit: "number",
          title: "Idle Streaming Requests",
          desc: "Number of streaming requests on management port (usually 8091) now idle."
        },
        "hibernated_waked": {
          unit: "number/sec",
          title: "Streaming Wakeups",
          desc: "Number of streaming request wakeups per second on management port (usually 8091)."
        },
        "mem_actual_free": {
          unit: "bytes",
          title: "Available RAM",
          desc: "Bytes of RAM available to Couchbase on this server."
        },
        "rest_requests": {
          unit: "number/sec",
          title: "HTTP Request Rate",
          desc: "Number of http requests per second on management port (usually 8091)."
        },
        "swap_used": {
          unit: "bytes",
          title: "Swap Used",
          desc: "Bytes of swap space in use on this server."
        },
      },

      "@kv-": {
        "couch_total_disk_size": {
          unit: "bytes",
          title: "Data/Views On Disk",
          desc: "The total size on disk of all data and view files for this bucket. (measured from couch_total_disk_size)"
        },
        "couch_docs_fragmentation": {
          unit: "percent",
          title: "Docs Fragmentation",
          desc: "Percentage of fragmented data to be compacted compared to real data for the data files in this bucket. (measured from couch_docs_fragmentation)"
        },
        "couch_views_fragmentation": {
          unit: "percent",
          title: "Views Fragmentation",
          desc: "Percentage of fragmented data to be compacted compared to real data for the view index files in this bucket. (measured from couch_views_fragmentation)"
        },
        "hit_ratio": {
          unit: "percent",
          title: "Get Ratio",
          desc: "Percentage of get requests served with data from this bucket. (measured from get_hits * 100/cmd_get)"
        },
        "ep_cache_miss_rate": {
          unit: "percent",
          title: "Cache Miss Ratio",
          desc: "Percentage of reads per second to this bucket from disk as opposed to RAM. (measured from ep_bg_fetches / gets * 100)"
        },
        "ep_resident_items_rate": {
          unit: "percent",
          title: "Resident Ratio",
          desc: "Percentage of all items cached in RAM in this bucket. (measured from ep_resident_items_rate)"
        },
        "vb_avg_active_queue_age": {
          unit: "second",
          title: "Active Queue Age",
          desc: "Average age in seconds of active items in the active item queue for this bucket. (measured from vb_avg_active_queue_age)"
        },
        "vb_avg_replica_queue_age": {
          unit: "second",
          title: "Replica Queue Age",
          desc: "Average age in seconds of replica items in the replica item queue for this bucket. (measured from vb_avg_replica_queue_age)"
        },
        "vb_avg_pending_queue_age": {
          unit: "second",
          title: "Pending Queue Age",
          desc: "Average age in seconds of pending items in the pending item queue for this bucket. Should be transient during rebalancing. (measured from vb_avg_pending_queue_age)"
        },
        "vb_avg_total_queue_age": {
          unit: "second",
          title: "Disk Write Queue Age",
          desc: "Average age in seconds of all items in the disk write queue for this bucket. (measured from vb_avg_total_queue_age)"
        },
        "vb_active_resident_items_ratio": {
          unit: "percent",
          title: "Active Resident Ratio",
          desc: "Percentage of active items cached in RAM in this bucket. (measured from vb_active_resident_items_ratio)"
        },
        "vb_replica_resident_items_ratio": {
          unit: "percent",
          title: "Replica Resident Ratio",
          name: "vb_replica_resident_items_ratio",
          desc: "Percentage of replica items cached in RAM in this bucket. (measured from vb_replica_resident_items_ratio)"
        },
        "vb_pending_resident_items_ratio": {
          unit: "percent",
          title: "Pending Resident Ratio",
          desc: "Percentage of items cached in RAM for pending vBuckets in this bucket. (measured from vb_pending_resident_items_ratio)"
        },
        "avg_disk_update_time": {
          unit: "microsecond",
          title: "Disk Update Time",
          desc: "Average disk update time in microseconds as from disk_update histogram of timings. (measured from avg_disk_update_time)"
        },
        "avg_disk_commit_time": {
          unit: "percent",
          title: "Disk Commit Time",
          desc: "Average disk commit time in seconds as from disk_update histogram of timings. (measured from avg_disk_commit_time)"
        },
        "avg_bg_wait_time": {
          unit: "microsecond",
          title: "Background Fetch Time",
          desc: "Average background fetch time in microseconds. (measured from avg_bg_wait_time)"
        },
        "avg_active_timestamp_drift": {
          unit: "second",
          title: "Active Timestamp Drift",
          name: "avg_active_timestamp_drift",
          desc: "Average drift (in seconds) between mutation timestamps and the local time for active vBuckets. (measured from ep_active_hlc_drift and ep_active_hlc_drift_count)"
        },
        "avg_replica_timestamp_drift": {
          unit: "second",
          title: "Replica Timestamp Drift",
          desc: "Average drift (in seconds) between mutation timestamps and the local time for replica vBuckets. (measured from ep_replica_hlc_drift and ep_replica_hlc_drift_count)"
        },
        "ep_dcp_views+indexes_count": {
          unit: "number",
          title: "DCP Indexes Connections",
          desc: "Number of internal views/gsi/search index DCP connections in this bucket (measured from ep_dcp_views_count + ep_dcp_2i_count + ep_dcp_fts_count)"
        },
        "ep_dcp_views+indexes_items_remaining": {
          unit: "number",
          title: "DCP Indexes Items Remaining",
          desc: "Number of items remaining to be sent to consumer in this bucket (measured from ep_dcp_views_items_remaining + ep_dcp_2i_items_remaining + ep_dcp_fts_items_remaining)"
        },
        "ep_dcp_views+indexes_producer_count": {
          unit: "number",
          title: "DCP Indexes Senders",
          desc: "Number of views/gsi/search index senders for this bucket (measured from ep_dcp_views_producer_count + ep_dcp_2i_producer_count + ep_dcp_fts_producer_count)"
        },
        "ep_dcp_views+indexes_items_sent": {
          unit: "number/sec",
          title: "DCP Indexes Items Sent",
          desc: "Number of items per second being sent for a producer for this bucket (measured from ep_dcp_views_items_sent + ep_dcp_2i_items_sent + ep_dcp_fts_items_sent)"
        },
        "ep_dcp_views+indexes_total_bytes": {
          unit: "bytes/sec",
          title: "DCP Indexes Drain Rate",
          desc: "Number of bytes per second being sent for views/gsi/search index DCP connections for this bucket (measured from ep_dcp_views_total_bytes + ep_dcp_2i_total_bytes + ep_dcp_fts_total_bytes)"
        },
        "ep_dcp_views+indexes_backoff": {
          unit: "number",
          title: "DCP Indexes Backoffs",
          desc: "Number of backoffs for views/gsi/search index DCP connections (measured from ep_dcp_views_backoff + ep_dcp_2i_backoff + ep_dcp_fts_backoff)"
        },
        "bytes_read": {
          unit: "bytes/sec",
          name: "bytes_read",
          title: "Memcached RX Rate",
          desc: "Bytes per second received in this bucket. (measured from bytes_read)"
        },
        "bytes_written": {
          unit: "bytes/sec",
          title: "Memcached TX Rate",
          desc: "Number of bytes per second sent from this bucket. (measured from bytes_written)"
        },
        "cas_badval": {
          unit: "number/sec",
          title: "CAS Badval Rate",
          desc: "Number of CAS operations per second using an incorrect CAS ID for data that this bucket contains. (measured from cas_badval)"
        },
        "cas_hits": {
          unit: "number/sec",
          title: "CAS Ops Rate",
          desc: "Number of operations with a CAS id per second for this bucket. (measured from cas_hits)"
        },
        "cas_misses": {
          unit: "number/sec",
          title: "CAS Miss Rate",
          desc: "Number of CAS operations per second for data that this bucket does not contain. (measured from cas_misses)"
        },
        "cmd_get": {
          unit: "number/sec",
          title: "Gets",
          desc: "Number of reads (get operations) per second from this bucket. (measured from cmd_get)"
        },
        "cmd_total_gets": {
          unit: "number/sec",
          title: "Total Gets",
          desc: "Number of total get operations per second from this bucket (measured from cmd_total_gets). This includes additional get operations such as get locked that are not included in cmd_get"
        },
        "cmd_set": {
          unit: "number/sec",
          title: "Sets",
          desc: "Number of writes (set operations) per second to this bucket. (measured from cmd_set)"
        },
        "couch_docs_actual_disk_size": {
          unit: "bytes",
          title: "Data Total Disk Size",
          desc: "The size of all data service files on disk for this bucket, including the data itself, metadata, and temporary files. (measured from couch_docs_actual_disk_size)"
        },
        "couch_docs_data_size": {
          unit: "bytes",
          title: "Active Data Size",
          desc: "Bytes of active data in this bucket. (measured from couch_docs_data_size)"
        },
        "couch_views_actual_disk_size": {
          unit: "bytes",
          title: "Views Disk Size",
          desc: "Bytes of active items in all the views for this bucket on disk (measured from couch_views_actual_disk_size)"
        },
        "couch_views_data_size": {
          unit: "bytes",
          title: "Views Data",
          desc: "Bytes of active data for all the views in this bucket. (measured from couch_views_data_size)"
        },
        "couch_views_ops": {
          unit: "number/sec",
          title: "Views Read Rate Total",
          desc: "All the views reads for all design documents including scatter gather. (measured from couch_views_ops)"
        },
        "curr_connections": {
          unit: "number",
          title: "Current Connections",
          desc: "Number of currrent connections to this server including connections from external client SDKs, proxies, DCP requests and internal statistic gathering. (measured from curr_connections)"
        },
        "curr_items": {
          unit: "number",
          title: "Active Items",
          desc: "Number of active items in this bucket. (measured from curr_items)"
        },
        "curr_items_tot": {
          unit: "number",
          title: "Total Items",
          desc: "Total number of items in this bucket. (measured from curr_items_tot)"
        },
        "decr_hits": {
          unit: "number/sec",
          title: "Decr Hit Rate",
          desc: "Number of decrement operations per second for data that this bucket contains. (measured from decr_hits)"
        },
        "decr_misses": {
          unit: "number/sec",
          title: "Decr Miss Rate",
          desc: "Number of decr operations per second for data that this bucket does not contain. (measured from decr_misses)"
        },
        "delete_hits": {
          unit: "number/sec",
          title: "Delete Rate",
          desc: "Number of delete operations per second for this bucket. (measured from delete_hits)"
        },
        "delete_misses": {
          unit: "number/sec",
          title: "Delete Miss Rate",
          desc: "Number of delete operations per second for data that this bucket does not contain. (measured from delete_misses)"
        },
        "disk_write_queue": {
          unit: "number",
          title: "Disk Write Queue",
          desc: "Number of items waiting to be written to disk in this bucket. (measured from ep_queue_size+ep_flusher_todo)"
        },
        "ep_active_ahead_exceptions": {
          unit: "number/sec",
          title: "Active Ahead Exception Rate",
          desc: "Total number of ahead exceptions (when timestamp drift between mutations and local time has exceeded 5000000 μs) per second for all active vBuckets."
        },
        "ep_bg_fetched": {
          unit: "number/sec",
          title: "Disk Read Rate",
          desc: "Number of reads per second from disk for this bucket. (measured from ep_bg_fetched)"
        },
        "ep_data_read_failed": {
          unit: "number",
          title: "Disk Read Failures",
          desc: "Number of disk read failures. (measured from ep_data_read_failed)"
        },
        "ep_data_write_failed": {
          unit: "number",
          title: "Disk Write Failures",
          desc: "Number of disk write failures. (measured from ep_data_write_failed)"
        },
        "ep_dcp_cbas_backoff": {
          unit: "number/sec",
          title: "DCP Analytics Backoffs",
          desc: "Number of backoffs per second for analytics DCP connections (measured from ep_dcp_cbas_backoff)"
        },
        "ep_dcp_cbas_count": {
          unit: "number",
          title: "DCP Analytics Connections",
          desc: "Number of internal analytics DCP connections in this bucket (measured from ep_dcp_cbas_count)"
        },
        "ep_dcp_cbas_items_remaining": {
          unit: "number",
          title: "DCP Analytics Items Remaining",
          desc: "Number of items remaining to be sent to consumer in this bucket (measured from ep_dcp_cbas_items_remaining)"
        },
        "ep_dcp_cbas_items_sent": {
          unit: "number/sec",
          title: "DCP Analytics Items Sent",
          desc: "Number of items per second being sent for a producer for this bucket (measured from ep_dcp_cbas_items_sent)"
        },
        "ep_dcp_cbas_producer_count": {
          unit: "number",
          title: "DCP Analytics Senders",
          desc: "Number of analytics senders for this bucket (measured from ep_dcp_cbas_producer_count)"
        },
        "ep_dcp_cbas_total_bytes": {
          unit: "bytes/sec",
          title: "DCP Analytics Drain Rate",
          desc:"Number of bytes per second being sent for analytics DCP connections for this bucket (measured from ep_dcp_cbas_total_bytes)"
        },
        "ep_dcp_eventing_backoff": {
          unit: "number/sec",
          title: "DCP Eventing Backoffs",
          desc: "Number of backoffs per second for eventing DCP connections (measured from ep_dcp_eventing_backoff)"
        },
        "ep_dcp_eventing_count": {
          unit: "number",
          title: "DCP Eventing Connections",
          desc: "Number of internal eventing DCP connections in this bucket (measured from ep_dcp_eventing_count)"
        },
        "ep_dcp_eventing_items_remaining": {
          unit: "number",
          title: "DCP Eventing Items Remaining",
          desc: "Number of items remaining to be sent to consumer in this bucket (measured from ep_dcp_eventing_items_remaining)"
        },
        "ep_dcp_eventing_items_sent": {
          unit: "number/sec",
          title: "DCP Eventing Items Sent",
          desc: "Number of items per second being sent for a producer for this bucket (measured from ep_dcp_eventing_items_sent)"
        },
        "ep_dcp_eventing_producer_count": {
          unit: "number",
          title: "DCP Eventing Senders",
          desc: "Number of eventing senders for this bucket (measured from ep_dcp_eventing_producer_count)"
        },
        "ep_dcp_eventing_total_bytes": {
          unit: "bytes/sec",
          title: "DCP Eventing Drain Rate",
          desc:"Number of bytes per second being sent for eventing DCP connections for this bucket (measured from ep_dcp_eventing_total_bytes)"
        },
        "ep_dcp_other_backoff": {
          unit: "number",
          title: "DCP Other Backoffs",
          desc: "Number of backoffs for other DCP connections"
        },
        "ep_dcp_other_count": {
          unit: "number",
          title: "DCP Other Connections",
          desc: "Number of other DCP connections in this bucket (measured from ep_dcp_other_count)"
        },
        "ep_dcp_other_items_remaining": {
          unit: "number",
          title: "DCP Other Items Remaining",
          desc: "Number of items remaining to be sent to consumer in this bucket (measured from ep_dcp_other_items_remaining)"
        },
        "ep_dcp_other_items_sent": {
          unit: "number/sec",
          title: "DCP Other Items Sent",
          desc: "Number of items per second being sent for a producer for this bucket (measured from ep_dcp_other_items_sent)"
        },
        "ep_dcp_other_producer_count": {
          unit: "number",
          title: "DCP Other Senders",
          desc: "Number of other senders for this bucket (measured from ep_dcp_other_producer_count)"
        },
        "ep_dcp_other_total_bytes": {
          unit: "bytes/sec",
          title: "DCP Other Drain Rate",
          desc: "Number of bytes per second being sent for other DCP connections for this bucket (measured from ep_dcp_other_total_bytes)"
        },
        "ep_dcp_replica_backoff": {
          unit: "number",
          title: "DCP Replication Backoffs",
          desc: "Number of backoffs for replication DCP connections"
        },
        "ep_dcp_replica_count": {
          unit: "number",
          title: "DCP Replication Connections",
          desc: "Number of internal replication DCP connections in this bucket (measured from ep_dcp_replica_count)"
        },
        "ep_dcp_replica_items_remaining": {
          unit: "number",
          title: "DCP Replication Items Remaining",
          desc: "Number of items remaining to be sent to consumer in this bucket (measured from ep_dcp_replica_items_remaining)"
        },
        "ep_dcp_replica_items_sent": {
          unit: "number/sec",
          title: "DCP Replication Items Sent",
          desc: "Number of items per second being sent for a producer for this bucket (measured from ep_dcp_replica_items_sent)"
        },
        "ep_dcp_replica_producer_count": {
          unit: "number",
          title: "DCP Replication Senders",
          desc: "Number of replication senders for this bucket (measured from ep_dcp_replica_producer_count)"
        },
        "ep_dcp_replica_total_bytes": {
          unit: "bytes/sec",
          title: "DCP Replication Drain Rate",
          desc: "Number of bytes per second being sent for replication DCP connections for this bucket (measured from ep_dcp_replica_total_bytes)"
        },
        "ep_dcp_xdcr_backoff": {
          unit: "number",
          title: "DCP XDCR Backoffs",
          desc: "Number of backoffs for XDCR DCP connections"
        },
        "ep_dcp_xdcr_count": {
          unit: "number",
          title: "DCP XDCR Connections",
          desc: "Number of internal XDCR DCP connections in this bucket (measured from ep_dcp_xdcr_count)"
        },
        "ep_dcp_xdcr_items_remaining": {
          unit: "number",
          title: "DCP XDCR Items Remaining",
          desc: "Number of items remaining to be sent to consumer in this bucket (measured from ep_dcp_xdcr_items_remaining)"
        },
        "ep_dcp_xdcr_items_sent": {
          unit: "number/sec",
          title: "DCP XDCR Items Sent",
          desc: "Number of items per second being sent for a producer for this bucket (measured from ep_dcp_xdcr_items_sent)"
        },
        "ep_dcp_xdcr_producer_count": {
          unit: "number",
          title: "DCP XDCR Senders",
          desc: "Number of XDCR senders for this bucket (measured from ep_dcp_xdcr_producer_count)"
        },
        "ep_dcp_xdcr_total_bytes": {
          unit: "bytes/sec",
          title: "DCP XDCR Drain Rate",
          desc: "Number of bytes per second being sent for XDCR DCP connections for this bucket (measured from ep_dcp_xdcr_total_bytes)"
        },
        "ep_diskqueue_drain": {
          unit: "number/sec",
          title: "Disk Queue Total Drain Rate",
          desc: "Total number of items per second being written to disk in this bucket (measured from ep_diskqueue_drain)"
        },
        "ep_diskqueue_fill": {
          unit: "number/sec",
          title: "Disk Queue Total Fill Rate",
          desc: "Total number of items per second being put on the disk queue in this bucket (measured from ep_diskqueue_fill)"
        },
        "ep_diskqueue_items": {
          unit: "number",
          title: "Disk Queue Total Items",
          desc: "Total number of items waiting (in queue) to be written to disk in this bucket (measured from ep_diskqueue_items)"
        },
        "ep_kv_size": {
          unit: "bytes",
          title: "User Data in RAM",
          desc: "Total amount of user data cached in RAM in this bucket. (measured from ep_kv_size)"
        },
        "ep_mem_high_wat": {
          unit: "bytes",
          title: "High Water Mark",
          desc: "High water mark (in bytes) for auto-evictions. (measured from ep_mem_high_wat)"
        },
        "ep_mem_low_wat": {
          unit: "bytes",
          title: "Low Water Mark",
          desc: "Low water mark (in bytes) for auto-evictions. (measured from ep_mem_low_wat)"
        },
        "ep_meta_data_memory": {
          unit: "bytes",
          title: "Total Metadata in RAM",
          desc: "Bytes of item metadata consuming RAM in this bucket (measured from ep_meta_data_memory)"
        },
        "ep_num_ops_del_meta": {
          unit: "number/sec",
          title: "XDCR Incoming Delete Rate",
          desc: "Number of delete operations per second for this bucket as the target for XDCR. (measured from ep_num_ops_del_meta)"
        },
        "ep_num_ops_get_meta": {
          unit: "number/sec",
          title: "XDCR Incoming Metadata Read Rate",
          desc: "Number of metadata read operations per second for this bucket as the target for XDCR. (measured from ep_num_ops_get_meta)"
        },
        "ep_num_ops_set_meta": {
          unit: "number/sec",
          title: "XDCR Incoming Set Rate",
          desc: "Number of set operations per second for this bucket as the target for XDCR. (measured from ep_num_ops_set_meta)"
        },
        "ep_num_value_ejects": {
          unit: "number/sec",
          title: "Ejection Rate",
          desc: "Number of items per second being ejected to disk in this bucket. (measured from ep_num_value_ejects)"
        },
        "ep_ops_create": {
          unit: "number/sec",
          title: "Total Disk Create Rate",
          desc: "Number of new items created on disk per second for this bucket. (measured from vb_active_ops_create + vb_replica_ops_create + vb_pending_ops_create)"
        },
        "ep_ops_update": {
          unit: "number/sec",
          title: "Disk Update Rate",
          desc: "Number of items updated on disk per second for this bucket. (measured from vb_active_ops_update + vb_replica_ops_update + vb_pending_ops_update)"
        },
        "ep_replica_ahead_exceptions": {
          unit: "number/sec",
          title: "Replica Ahead Exception Rate",
          desc: "Total number of ahead exceptions (when timestamp drift between mutations and local time has exceeded 5000000 μs) per second for all replica vBuckets."
        },
        "ep_tmp_oom_errors": {
          unit: "number/sec",
          title: "Temp OOM Rate",
          desc: "Number of back-offs sent per second to client SDKs due to \"out of memory\" situations from this bucket. (measured from ep_tmp_oom_errors)"
        },
        "ep_vb_total": {
          unit: "number",
          title: "vBuckets Total",
          desc: "Total number of vBuckets for this bucket. (measured from ep_vb_total)"
        },
        "evictions": {
          unit: "number/sec",
          title: "Eviction Rate",
          desc: "Number of items per second evicted from this bucket. (measured from evictions)"
        },
        "get_hits": {
          unit: "number/sec",
          title: "Get Hit Rate",
          desc: "Number of get operations per second for data that this bucket contains. (measured from get_hits)"
        },
        "get_misses": {
          unit: "number/sec",
          title: "Get Miss Rate",
          desc: "Number of get operations per second for data that this bucket does not contain. (measured from get_misses)",
        },
        "incr_hits": {
          unit: "number/sec",
          title: "Incr Hit Rate",
          desc: "Number of increment operations per second for data that this bucket contains. (measured from incr_hits)"
        },
        "incr_misses": {
          unit: "number/sec",
          title: "Incr Miss Rate",
          desc: "Number of increment operations per second for data that this bucket does not contain. (measured from incr_misses)"
        },
        "mem_used": {
          unit: "bytes",
          title: "Data Total RAM Used",
          desc: "Total memory used in bytes. (as measured from mem_used)"
        },
        "ops": {
          unit: "number/sec",
          title: "Total Ops",
          desc: "Total operations per second (including XDCR) to this bucket. (measured from cmd_get + cmd_set + incr_misses + incr_hits + decr_misses + decr_hits + delete_misses + delete_hits + ep_num_ops_del_meta + ep_num_ops_get_meta + ep_num_ops_set_meta)"
        },
        "vb_active_eject": {
          unit: "number/sec",
          title: "Active Ejection Rate",
          desc: "Number of items per second being ejected to disk from active vBuckets in this bucket. (measured from vb_active_eject)"
        },
        "vb_active_itm_memory": {
          unit: "bytes",
          title: "Active User Data in RAM",
          desc: "Amount of active user data cached in RAM in this bucket. (measured from vb_active_itm_memory)"
        },
        "vb_active_meta_data_memory": {
          unit: "bytes",
          title: "Active Metadata in RAM",
          desc: "Amount of active item metadata consuming RAM in this bucket. (measured from vb_active_meta_data_memory)"
        },
        "vb_active_num": {
          unit: "number",
          title: "vBuckets Active",
          desc: "Number of active vBuckets in this bucket. (measured from vb_active_num)"
        },
        "vb_active_ops_create": {
          unit: "number/sec",
          title: "Active Create Rate",
          desc: "New items per second being inserted into active vBuckets in this bucket. (measured from vb_active_ops_create)"
        },
        "vb_active_queue_drain": {
          unit: "number/sec",
          title: "Disk Queue Active Drain Rate",
          desc: "Number of active items per second being written to disk in this bucket. (measured from vb_active_queue_drain)"
        },
        "vb_active_queue_fill": {
          unit: "number/sec",
          title: "Disk Queue Active Fill Rate",
          desc: "Number of active items per second being put on the active item disk queue in this bucket. (measured from vb_active_queue_fill)"
        },
        "vb_active_queue_size": {
          unit: "number",
          title: "Disk Queue Active Items",
          desc: "Number of active items waiting to be written to disk in this bucket. (measured from vb_active_queue_size)"
        },
        "vb_active_sync_write_accepted_count": {
          unit: "number/sec",
          title: "Accepted Sync Writes Rate",
          desc: "Number of accepted synchronous write per second into active vBuckets in this bucket. (measured from vb_active_sync_write_accepted_count)"
        },
        "vb_active_sync_write_committed_count": {
          unit: "number/sec",
          title: "Committed Sync Writes Rate",
          desc: "Number of committed synchronous writes per second into active vBuckets in this bucket. (measured from vb_active_sync_write_committed_count)"
        },
        "vb_active_sync_write_aborted_count": {
          unit: "number/sec",
          title: "Aborted Sync Writes Rate",
          desc: "Number of aborted synchronous writes per second into active vBuckets in this bucket. (measured from vb_active_sync_write_aborted_count)"
        },
        "vb_pending_curr_items": {
          unit: "number",
          title: "Pending Items",
          desc: "Number of items in pending vBuckets in this bucket. Should be transient during rebalancing. (measured from vb_pending_curr_items)"
        },
        "vb_pending_eject": {
          unit: "number/sec",
          title: "Pending Ejection Rate",
          desc: "Number of items per second being ejected to disk from pending vBuckets in this bucket. Should be transient during rebalancing. (measured from vb_pending_eject)"
        },
        "vb_pending_itm_memory": {
          unit: "bytes",
          title: "Pending User Data in RAM",
          desc: "Amount of pending user data cached in RAM in this bucket. Should be transient during rebalancing. (measured from vb_pending_itm_memory)"
        },
        "vb_pending_meta_data_memory": {
          unit: "bytes",
          title: "Pending Metadata in RAM",
          desc: "Amount of pending item metadata consuming RAM in this bucket. Should be transient during rebalancing. (measured from vb_pending_meta_data_memory)"
        },
        "vb_pending_num": {
          unit: "number",
          title: "vBuckets Pending",
          desc: "Number of pending vBuckets in this bucket. Should be transient during rebalancing. (measured from vb_pending_num)"
        },
        "vb_pending_ops_create": {
          unit: "number/sec",
          title: "Pending Create Rate",
          desc: "New items per second being instead into pending vBuckets in this bucket. Should be transient during rebalancing. (measured from vb_pending_ops_create)"
        },
        "vb_pending_queue_drain": {
          unit: "number/sec",
          title: "Disk Queue Pending Drain Rate",
          desc: "Number of pending items per second being written to disk in this bucket. Should be transient during rebalancing. (measured from vb_pending_queue_drain)"
        },
        "vb_pending_queue_fill": {
          unit: "number/sec",
          title: "Disk Queue Pending Fill Rate",
          desc: "Number of pending items per second being put on the pending item disk queue in this bucket. Should be transient during rebalancing (measured from vb_pending_queue_fill)"
        },
        "vb_pending_queue_size": {
          unit: "number",
          title: "Disk Queue Pending Items",
          desc: "Number of pending items waiting to be written to disk in this bucket and should be transient during rebalancing  (measured from vb_pending_queue_size)"
        },
        "vb_replica_curr_items": {
          unit: "number",
          title: "Replica Items",
          desc: "Number of items in replica vBuckets in this bucket. (measured from vb_replica_curr_items)"
        },
        "vb_replica_eject": {
          unit: "number/sec",
          title: "Replica Ejection Rate",
          desc: "Number of items per second being ejected to disk from replica vBuckets in this bucket. (measured from vb_replica_eject)"
        },
        "vb_replica_itm_memory": {
          unit: "bytes",
          title: "Replica User Data in RAM",
          desc: "Amount of replica user data cached in RAM in this bucket. (measured from vb_replica_itm_memory)"
        },
        "vb_replica_meta_data_memory": {
          unit: "bytes",
          title: "Replica Metadata in RAM",
          desc: "Amount of replica item metadata consuming in RAM in this bucket. (measured from vb_replica_meta_data_memory)"
        },
        "vb_replica_num": {
          unit: "number",
          title: "vBuckets Replica",
          desc: "Number of replica vBuckets in this bucket. (measured from vb_replica_num)"
        },
        "vb_replica_ops_create": {
          unit: "number/sec",
          title: "Replica Item Create Rate",
          desc: "New items per second being inserted into \"replica\" vBuckets in this bucket (measured from vb_replica_ops_create"
        },
        "vb_replica_queue_drain": {
          unit: "number/sec",
          title: "Disk Queue Replica Drain Rate",
          desc: "Number of replica items per second being written to disk in this bucket (measured from vb_replica_queue_drain)"
        },
        "vb_replica_queue_fill": {
          unit: "number/sec",
          title: "Disk Queue Replica Fill Rate",
          desc: "Number of replica items per second being put on the replica item disk queue in this bucket (measured from vb_replica_queue_fill)"
        },
        "vb_replica_queue_size": {
          unit: "number",
          title: "Disk Queue Replica Items",
          desc: "Number of replica items waiting to be written to disk in this bucket (measured from vb_replica_queue_size)"
        },
        "xdc_ops": {
          unit: "number/sec",
          title: "XDCR Incoming Op Rate",
          desc: "Number of incoming XDCR operations per second for this bucket. (measured from xdc_ops)"
        },
        "@items": {
          "accesses": {
            unit: "number/sec",
            title: "Views Read Rate",
            desc: "Traffic to the views in this design doc."
          },
          "data_size": {
            unit: "bytes",
            title: "Views Data Size",
            desc: "Bytes stored in memory for views in this design doc."
          },
          "disk_size": {
            unit: "bytes",
            title: "Views Disk Size",
            desc: "Bytes stored on disk for views in this design doc."
          }
        }
      },

      "@index":{
        "index_memory_quota": null, //able in system but not in builder
        "index_memory_used": null,
        "index_ram_percent": { //doesn't exist in prometheus
          unit: "percent",
          title: "Index RAM Quota Used",
          desc: "Percentage of Index RAM quota in use across all indexes on this server."
        },
        "index_remaining_ram": {
          unit: "bytes",
          title: "Index RAM Quota Available",
          desc: "Bytes of Index RAM quota still available on this server."
        }
      },

      "@index-":{
        "@items": {
          "num_docs_pending+queued": {
            unit: "number",
            title: "Index Mutations Remaining",
            desc: "Number of documents pending to be indexed. Per index."
          },
          "num_docs_indexed": {
            unit: "number/sec",
            title: "Index Drain Rate",
            desc: "Number of documents indexed by the indexer per second. Per index."
          },
          "index_resident_percent": {
            unit: "percent",
            title: "Index Resident Percent",
            desc: "Percentage of index data resident in memory. Per index."
          },
          "memory_used": {
            unit: "bytes",
            title: "Index RAM Used",
            desc: "Bytes in memory for this index. Per index."
          },
          "items_count": {
            unit: "number",
            title: "Indexed Items",
            desc: "Current total indexed documents. Per index."
          },
          "data_size": {
            unit: "bytes",
            title: "Index Data Size",
            desc: "Bytes of data in this index. Per index."
          },
          "disk_size": {
            unit: "bytes",
            title: "Index Disk Size",
            desc: "Bytes on disk for this index. Per index."
          },
          "avg_item_size": {
            unit: "bytes",
            title: "Index Item Size",
            desc: "Average size of each index item. Per index."
          },
          "avg_scan_latency": {
            unit: "nanoseconds",
            title: "Index Scan Latency",
            desc: "Average time (in nanoseconds) to serve a scan request. Per index."
          },
          "num_requests": {
            unit: "number/sec",
            title: "Index Request Rate",
            desc: "Number of requests served by the indexer per second. Per index."
          },
          "num_rows_returned": {
            unit: "number/sec",
            title: "Index Scan Items",
            desc: "Number of index items scanned by the indexer per second. Per index."
          },
          "scan_bytes_read": {
            unit: "number/sec",
            title: "Index Scan Bytes",
            desc: "Bytes per second read by a scan. Per index."
          },
          "index_frag_percent": {
            unit: "percent",
            title: "Index Fragmentation",
            desc: "Percentage fragmentation of the index. Note: at small index sizes of less than a hundred kB, the static overhead of the index disk file will inflate the index fragmentation percentage. Per index."
          },
          "cache_miss_ratio": {
            unit: "percent",
            title: "Index Cache Miss Ratio",
            desc: "Percentage of accesses to this index data from disk as opposed to RAM. (measured from cache_misses * 100 / (cache_misses + cache_hits))"
          }
        },
        "index/data_size": {
          unit: "bytes",
          title: "Index Total RAM Used",
          desc: "Bytes in memory used by Index across all indexes and buckets."
        },
        "index/disk_size": {
          unit: "bytes",
          title: "Index Total Disk Size",
          desc: "Bytes on disk used by Index across all indexes and buckets."
        },
        "index/fragmentation": {
          unit: "percent",
          title: "Index Total Fragmentation",
          desc: "Percentage fragmentation for all indexes. Note: at small index sizes of less than a hundred kB, the static overhead of the index disk file will inflate the index fragmentation percentage."
        },
        "index/items_count": {
          unit: "number",
          title: "Index Doc Count",
          desc: "Current total number of indexed documents"
        },
        "index/memory_used": {
          unit: "bytes",
          title: "Index RAM Used",
          desc: "Total memory used by the index."
        },
        "index/num_docs_indexed": {
          unit: "number/sec",
          title: "Indexing Rate",
          desc: "Number of documents indexed by the indexer per second."
        },
        "index/num_requests": {
          unit: "number/sec",
          title: "Index Request Rate",
          desc: "Number of requests served by the indexer per second"
        },
        "index/num_rows_returned": {
          unit: "number/sec",
          title: "Index Total Scan Rate",
          desc: "Number of index items scanned by the indexer per second across all indexes."
        },
        "index/scan_bytes_read": {
          unit: "bytes/sec",
          title: "Index Scan Bytes",
          desc: "Number of bytes/sec scanned by the index."
        }
      },

      "@query":{
        "query_avg_req_time": {
          unit: "nanoseconds",
          title: "Query Request Time",
          desc: "Average end-to-end time to process a query (in seconds)."
        },
        "query_avg_svc_time": {
          unit: "nanoseconds",
          title: "Query Execution Time",
          desc: "Average time to execute a query (in seconds)."
        },
        "query_avg_response_size": {
          unit: "bytes",
          title: "Query Result Size",
          desc: "Average size (in bytes) of the data returned by a query"
        },
        "query_avg_result_count": {
          unit: "number",
          title: "Query Result Items",
          desc: "Average number of results (items/documents) returned by a query."
        },
        "query_errors": {
          unit: "number/sec",
          title: "N1QL Error Rate",
          desc: "Number of N1QL errors returned per second."
        },
        "query_invalid_requests": {
          unit: "number/sec",
          title: "N1QL Invalid Request Rate",
          desc: "Number of requests for unsupported endpoints per second, specifically HTTP requests for all endpoints not supported by the query engine. For example, a request for http://localhost:8093/foo will be included. Potentially useful in identifying DOS attacks."
        },
        "query_requests": {
          unit: "number/sec",
          title: "N1QL Request Rate",
          desc: "Number of N1QL requests processed per second."
        },
        "query_requests_1000ms": {
          unit: "number/sec",
          title: "Queries > 1000ms",
          desc: "Number of queries that take longer than 1000 ms per second"
        },
        "query_requests_250ms": {
          unit: "number/sec",
          title: "Queries > 250ms",
          desc: "Number of queries that take longer than 250 ms per second."
        },
        "query_requests_5000ms": {
          unit: "number/sec",
          title: "Queries > 5000ms",
          desc: "Number of queries that take longer than 5000 ms per second."
        },
        "query_requests_500ms": {
          unit: "number/sec",
          title: "Queries > 500ms",
          desc: "Number of queries that take longer than 500 ms per second."
        },
        "query_selects": {
          unit: "number/sec",
          title: "N1QL Select Rate",
          desc: "Number of N1QL selects processed per second."
        },
        "query_warnings": {
          unit: "number/sec",
          title: "N1QL Warning Rate",
          desc: "Number of N1QL warnings returned per second."
        }
      },

      "@fts-": {
        "@items": {
          "avg_queries_latency": {
            unit: "millisecond",
            title: "Search Query Latency",
            desc: "Average milliseconds to answer a Search query. Per index. (measured from avg_queries_latency)"
          },
          "doc_count": {
            unit: "number",
            title: "Search Docs",
            desc: "Number of documents examined. Per index. (measured from doc_count)"
          },
          "num_bytes_used_disk": {
            unit: "bytes",
            title:"Search Disk Size",
            desc: "Bytes on disk for this index. Per index. (measured from num_bytes_used_disk)"
          },
          "num_files_on_disk": {
            unit: "number",
            title: "Search Disk Files",
            desc: "Number of search files on disk across all partitions. (measured from num_files_on_disk)"
          },
          "num_root_memorysegments": {
            unit: "number",
            title: "Search Memory Segments",
            desc: "Number of memory segments in the index across all partitions. (measured from num_root_memorysegments)"
          },
          "num_root_filesegments": {
            unit: "number",
            title: "Search Disk Segments",
            desc: "Number of file segments in the index across all partitions. (measured from num_root_filesegments)"
          },
          "num_mutations_to_index": {
            unit: "number",
            title: "Search Mutations Remaining",
            desc: "Number of mutations not yet indexed. Per index. (measured from num_mutations_to_index)"
          },
          "num_pindexes_actual": {
            unit: "number",
            title: "Search Partitions",
            desc: "Number of index partitions. Per index. (including replica partitions, measured from num_pindexes_actual)"
          },
          "num_pindexes_target": {
            unit: "number",
            title: "Search Partitions Expected",
            desc: "Number of index partitions expected. Per index. (including replica partitions, measured from num_pindexes_target)"
          },
          "num_recs_to_persist": {
            unit: "number",
            title: "Search Records to Persist",
            desc: "Number of index records not yet persisted to disk. Per index. (measured from num_recs_to_persist)"
          },
          "total_bytes_indexed": {
            unit: "bytes/sec",
            title: "Search Index Rate",
            desc: "Bytes of plain text indexed per second. Per index. (measured from total_bytes_indexed)"
          },
          "total_bytes_query_results": {
            unit: "bytes/sec",
            title: "Search Result Rate",
            desc: "Bytes returned in results per second. Per index. (measured from total_bytes_query_results)"
          },
          "total_compaction_written_bytes": {
            unit: "bytes/sec",
            title: "Search Compaction Rate",
            desc: "Compaction bytes written per second. Per index. (measured from total_compaction_written_bytes)",
          },
          "total_queries": {
            unit: "number/sec",
            title: "Search Query Rate",
            desc: "Number of queries per second. Per index. (measured from total_queries)"
          },
          "total_queries_error": {
            unit: "number/sec",
            title: "Search Query Error Rate",
            desc: "Number of queries per second (including timeouts) that resulted in errors. Per index. (measured from total_queries_error)"
          },
          "total_queries_slow": {
            unit: "number/sec",
            title: "Search Slow Queries",
            desc: "Number of slow queries (> 5s to run) per second. Per index. (measured from total_queries_slow)"
          },
          "total_queries_timeout": {
            unit: "number/sec",
            title: "Search Query Timeout Rate",
            desc: "Number of queries that timeout per second. Per index. (measured from total_queries_timeout)"
          },
          "total_term_searchers": {
            unit: "number/sec",
            title: "Term Searchers Start Rate",
            desc: "Number of term searchers started per second. Per index. (measured from total_term_searchers)"
          }
        },
        "fts/num_bytes_used_disk": {
          unit: "bytes",
          title: "Search Total Disk Used",
          desc: "Bytes stored on disk for all Search indexes in this bucket."
        },
        "fts/num_files_on_disk": {
          unit: "number",
          title: "Search Disk Files",
          desc: "Number of search files on disk across all partitions."
        },
        "fts/total_bytes_indexed": {
          unit: "bytes/sec",
          title: "Search Index Rate",
          desc: "Search bytes indexed per second for all Search indexes in this bucket."
        },
        "fts/total_queries": {
          unit: "number/sec",
          title: "Search Query Rate",
          desc: "Search queries per second for all Search indexes in this bucket."
        }
      },

      "@fts": {
        "fts_num_bytes_used_ram": {
          unit: "bytes",
          title: "Search Total RAM Used",
          desc: "Bytes of RAM used by Search across all indexes and all buckets on this server."
        },
        "fts_total_queries_rejected_by_herder": {
          unit: "number",
          title: "Search Queries Rejected",
          desc: "Number of queries rejected by throttler due to high memory consumption."
        },
        "fts_curr_batches_blocked_by_herder": {
          unit: "number",
          title: "DCP batches blocked by FTS throttler",
          desc: "DCP batches blocked by throttler due to high memory consumption."
        }
      },

      "@cbas-":{
        "cbas/failed_at_parser_records_count_total": {
          unit: "number",
          title: "Analytics Parse Fail Since Connect",
          desc: "Number of records Analytics failed to parse during bucket synchronization - since last bucket connect."
        },
        "cbas/incoming_records_count": {
          unit: "number/sec",
          title: "Analytics Ops Rate",
          desc: "Operations (gets + sets + deletes) per second processed by Analytics for this bucket."
        },
        "cbas/incoming_records_count_total": {
          unit: "number",
          title: "Analytics Ops Since Connect",
          desc: "Number of operations (gets + sets + deletes) processed by Analytics for this bucket since last bucket connect."
        }
      },

      "@cbas":{
        "cbas_disk_used": {
          unit: "bytes",
          title: "Analytics Total Disk Size",
          desc: "The total disk size used by Analytics."
        },
        "cbas_gc_count": {
          unit: "number/sec",
          title: "Analytics Garbage Collection Rate",
          desc: "Number of JVM garbage collections per second for this Analytics node."
        },
        "cbas_gc_time": {
          unit: "millisecond/sec",
          title: "Analytics Garbage Collection Time",
          desc: "The amount of time in milliseconds spent performing JVM garbage collections for Analytics node."
        },
        "cbas_heap_used": {
          unit: "bytes",
          title: "Analytics Heap Used",
          desc: "Bytes of JVM heap used by Analytics on this server."
        },
        "cbas_system_load_average": {
          unit: "bytes",
          title: "Analytics System Load",
          desc: "System load in bytes for Analytics node."
        },
        "cbas_thread_count": {
          unit: "number",
          title: "Analytics Thread Count",
          desc: "Number of threads for Analytics node."
        },
        "cbas_io_reads": {
          unit: "bytes/sec",
          title: "Analytics Read Rate",
          desc: "Number of disk bytes read on Analytics node per second."
        },
        "cbas_io_writes": {
          unit: "bytes/sec",
          title: "Analytics Write Rate",
          desc: "Number of disk bytes written on Analytics node per second."
        }
      },

      "@eventing":{
        "eventing/processed_count": {
          unit: "number",
          title: "Successful Function Invocations",
          desc: "Count of times the function was invoked successfully. Per function."
        },
        "eventing/failed_count": {
          unit: "number",
          title: "Failed Function Invocations",
          desc: "Count of times the function invocation failed. Per function."
        },
        "eventing/dcp_backlog": { //deprecated in 7.0
          unit: "number",
          title: "Eventing Backlog",
          desc: "Remaining mutations to be processed by the function. Per function."
        },
        "eventing/timeout_count": {
          unit: "number",
          title: "Eventing Timeouts",
          desc: "Execution timeouts while processing mutations. Per function."
        }
      },

      "@xdcr-":{
        "replication_changes_left": {
          unit: "number/sec",
          title: "XDCR Total Outbound Mutations",
          desc: "Number of mutations to be replicated to other clusters. (measured from replication_changes_left)"
        },
        "@items": {
          "percent_completeness": {
            unit: "percent",
            title: "XDCR Checked Ratio",
            desc: "Percentage of checked items out of all checked and to-be-replicated items. Per-replication. (measured from percent_completeness)"
          },
          "bandwidth_usage": {
            unit: "bytes/sec",
            title: "XDCR Replication Rate",
            desc: "Rate of replication in terms of bytes replicated per second. Per-replication. (measured from bandwidth_usage)"
          },
          "changes_left": {
            unit: "number",
            title: "XDCR Replication Mutations",
            desc: "Number of mutations to be replicated to other clusters. Per-replication. (measured from changes_left)"
          },
          "docs_failed_cr_source": {
            unit: "number",
            title: "XDCR Mutations Skipped",
            desc: "Number of mutations that failed conflict resolution on the source side and hence have not been replicated to other clusters. Per-replication. (measured from per-replication stat docs_failed_cr_source)"
          },
          "docs_filtered": {
            unit: "number/sec",
            title: "XDCR Mutations Filtered Rate",
            desc: "Number of mutations per second that have been filtered out and have not been replicated to other clusters. Per-replication. (measured from per-replication stat docs_filtered)"
          },
          "docs_written": {
            unit: "number",
            title: "XDCR Mutations Replicated",
            desc: "Number of mutations that have been replicated to other clusters. Per-replication. (measured from docs_written)"
          },
          "rate_doc_checks": {
            unit: "number/sec",
            title: "XDCR Doc Check Rate",
            desc: "Number of doc checks per second. Per-replication."
          },
          "rate_doc_opt_repd": {
            unit: "number/sec",
            title: "XDCR Optimistic Replication Rate",
            desc: "Number of replicated mutations per second. Per-replication."
          },
          "rate_received_from_dcp": {
            unit: "number/sec",
            title: "Doc Reception Rate",
            desc: "Rate of mutations received from dcp in terms of number of mutations per second. Per-replication."
          },
          "rate_replicated": {
            unit: "number/sec",
            title: "XDCR Mutations Replicated Rate",
            desc:"Number of replicated mutations per second. Per-replication. (measured from rate_replicated)"
          },
          "wtavg_docs_latency": {
            unit: "millisecond",
            title: "XDCR Doc Batch Latency",
            desc: "Weighted average latency in ms of sending replicated mutations to remote cluster. Per-replication. (measured from wtavg_docs_latency)"
          },
          "wtavg_meta_latency": {
            unit: "millisecond",
            title: "XDCR Meta Batch Latency",
            desc: "Weighted average latency in ms of sending getMeta and waiting for a conflict solution result from remote cluster. Per-replication. (measured from wtavg_meta_latency)"
          }
        }
      }
    }
  }
}
