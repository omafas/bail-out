%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-include("ns_common.hrl").

%% stats logs are parsed by the supportal tools - avoid any log trimming on them.

-define(stats_debug(Msg), ale:debug(?STATS_LOGGER, Msg)).
-define(stats_debug(Fmt, Args), ale:debug(?STATS_LOGGER, Fmt, Args, [{chars_limit, -1}])).

-define(stats_info(Msg), ale:info(?STATS_LOGGER, Msg)).
-define(stats_info(Fmt, Args), ale:info(?STATS_LOGGER, Fmt, Args, [{chars_limit, -1}])).

-define(stats_warning(Msg), ale:warn(?STATS_LOGGER, Msg)).
-define(stats_warning(Fmt, Args), ale:warn(?STATS_LOGGER, Fmt, Args, [{chars_limit, -1}])).

-define(stats_error(Msg), ale:error(?STATS_LOGGER, Msg)).
-define(stats_error(Fmt, Args), ale:error(?STATS_LOGGER, Fmt, Args, [{chars_limit, -1}])).

-define(STAT_GAUGES,
        %% Num items in active vbuckets.
        curr_items,

        mem_used,
        curr_connections,

        ep_mem_high_wat,
        ep_mem_low_wat,

        %% Num current items including those not active (replica, (NOT
        %% including dead) and pending states)
        curr_items_tot,

        %% Extra memory used by rep queues, etc..
        ep_overhead,
        ep_max_size,
        %% number of oom errors since boot. _not_ used by GUI, but
        %% used for oom alerts
        ep_oom_errors,

        %% Number of active vBuckets
        vb_active_num,
        %% Number of replica vBuckets
        vb_replica_num,
        %% Number of pending vBuckets
        vb_pending_num,
        ep_vb_total,

        % used for disk_write_queue computation in menelaus_stats
        ep_queue_size,
        ep_flusher_todo,

        %% Number of in memory items for replica vBuckets
        vb_replica_curr_items,
        %% Number of in memory items for pending vBuckets
        vb_pending_curr_items,

        %% Total item memory (K-V memory on UI)
        vb_active_itm_memory,
        %% Total item memory (K-V memory on UI)
        vb_replica_itm_memory,
        %% Total item memory (K-V memory on UI)
        vb_pending_itm_memory,

        %% Memory used to store keys and values. (K-V memory for total
        %% column) (includes dead vbuckets, apparently)
        ep_kv_size,

        %% Number of non-resident items in active vbuckets.
        vb_active_num_non_resident,
        %% Number of non-resident items in replica vbuckets
        vb_replica_num_non_resident,
        %% Number of non-resident items in pending vbuckets
        vb_pending_num_non_resident,
        %% The number of non-resident items (does not include dead
        %% vbuckets)
        ep_num_non_resident,

        %% Memeory used to store keys and values
        ep_meta_data_memory,
        %% Memory used to store keys and values (hashtable memory)
        vb_active_meta_data_memory,
        %% Memory used to store keys and values
        vb_replica_meta_data_memory,
        %% Memory used to store keys and values
        vb_pending_meta_data_memory,

        %% Active items in disk queue (incremented when item is
        %% dirtied and decremented when it's cleaned)
        vb_active_queue_size,
        %% Replica items in disk queue
        vb_replica_queue_size,
        %% Pending items in disk queue
        vb_pending_queue_size,
        %% aggregated by collector: vb_total_queue_size
        ep_diskqueue_items,

        %% Sum of disk queue item age in milliseconds
        vb_active_queue_age,
        %% Sum of disk queue item age in milliseconds
        vb_replica_queue_age,
        %% Sum of disk queue item age in milliseconds
        vb_pending_queue_age,
        %% aggregated by collector: vb_total_queue_age

        %% used by alerts
        ep_item_commit_failed,
        ep_clock_cas_drift_threshold_exceeded,

        %% Number of data disk read and write failures
        ep_data_read_failed,
        ep_data_write_failed
).

-define(STAT_COUNTERS,
        %% those are used be memcached buckets
        bytes_read,
        bytes_written,
        cas_badval,
        cas_hits,

        cas_misses,                             % Used by misses aggregation

        %% this is memcached-level stats
        % used by ops aggregation. Misses are used by misses aggregation
        cmd_get,
        cmd_lookup,
        cmd_set,
        decr_hits,
        decr_misses,
        delete_hits,
        delete_misses,
        incr_hits,
        incr_misses,

        %% XDCR related stats
        ep_num_ops_del_meta,
        ep_num_ops_get_meta,
        ep_num_ops_set_meta,
        ep_num_ops_set_ret_meta,
        ep_num_ops_del_ret_meta,

        %% this is memcached protocol hits & misses for get requests
        get_hits,
        get_misses,

        %% Total number of items persisted.
        %% ep_total_persisted,

        %% this is default bucket type evictions
        evictions,

        %% Number of times Not My VBucket exception happened during
        %% runtime
        %% ep_num_not_my_vbuckets,

        %% Number of times unrecoverable OOMs happened while
        %% processing operations
        %% ep_oom_errors,

        %% Number of times temporary OOMs happened while processing
        %% operations
        ep_tmp_oom_errors,

        %% Number of items fetched from disk.
        ep_bg_fetched,                          % needed by ep_cache_miss_rate

        %% Number of times replica item values got ejected from memory
        %% to disk
        %% ep_num_eject_replicas,

        %% Number of create operations
        vb_active_ops_create,
        %% Number of create operations
        vb_replica_ops_create,
        %% Number of create operations
        vb_pending_ops_create,
        %% ep_ops_create: aggregated by stats_collector

        %% Number of update operations
        vb_active_ops_update,
        %% Number of update operations
        vb_replica_ops_update,
        %% Number of update operations
        vb_pending_ops_update,
        %% ep_ops_update: aggregated by stats_collector

        %% Total enqueued items
        vb_active_queue_fill,
        %% Total enqueued items
        vb_replica_queue_fill,
        %% Total enqueued items
        vb_pending_queue_fill,
        %% vb_total_queue_fill: aggregated by stats_collector
        ep_diskqueue_fill,

        %% Number of times item values got ejected
        vb_active_eject,
        %% Number of times item values got ejected
        vb_replica_eject,
        %% Number of times item values got ejected
        vb_pending_eject,
        %% Number of times item values got ejected from memory to disk
        ep_num_value_ejects,

        %% Total drained items
        vb_active_queue_drain,
        %% Total drained items
        vb_replica_queue_drain,
        %% Total drained items
        vb_pending_queue_drain,
        %% vb_total_queue_drain: aggregated by stats_collector
        ep_diskqueue_drain,

        %% Total absolute drift for all active vBuckets.
        ep_active_hlc_drift,
        %% Number of updates applied to ep_active_hlc_drift.
        ep_active_hlc_drift_count,
        %% Total abosulte drift for all replica vBuckets.
        ep_replica_hlc_drift,
        %% Number of updates applied to ep_replica_hlc_drift.
        ep_replica_hlc_drift_count,
        %% Sum total of all active vBuckets' drift_ahead_threshold_exceeded counter.
        ep_active_ahead_exceptions,
        %% Sum total of all replica vBuckets' drift_ahead_threshold_exceeded counter.
        ep_replica_ahead_exceptions,

        %% Durable write statistics
        vb_active_sync_write_accepted_count,
        vb_active_sync_write_committed_count,
        vb_active_sync_write_aborted_count
).

-record(stat_entry, {timestamp :: integer(),
                     values :: [{binary() | atom(), number() | undefined}]}).

-record(dcp_stream_stats, {count = 0,
                           items_remaining = 0,
                           items_sent = 0,
                           producer_count = 0,
                           total_backlog_size = 0,
                           total_bytes = 0,
                           backoff = 0}).

-define(DCP_STAT_GAUGES,
        ep_dcp_replica_count,
        ep_dcp_replica_items_remaining,
        ep_dcp_replica_producer_count,
        ep_dcp_replica_total_backlog_size,

        ep_dcp_xdcr_count,
        ep_dcp_xdcr_items_remaining,
        ep_dcp_xdcr_producer_count,
        ep_dcp_xdcr_total_backlog_size,

        ep_dcp_views_count,
        ep_dcp_views_items_remaining,
        ep_dcp_views_producer_count,
        ep_dcp_views_total_backlog_size,

        ep_dcp_2i_count,
        ep_dcp_2i_items_remaining,
        ep_dcp_2i_producer_count,
        ep_dcp_2i_total_backlog_size,

        ep_dcp_fts_count,
        ep_dcp_fts_items_remaining,
        ep_dcp_fts_producer_count,
        ep_dcp_fts_total_backlog_size,

        ep_dcp_cbas_count,
        ep_dcp_cbas_items_remaining,
        ep_dcp_cbas_producer_count,
        ep_dcp_cbas_total_backlog_size,

        ep_dcp_eventing_count,
        ep_dcp_eventing_items_remaining,
        ep_dcp_eventing_producer_count,
        ep_dcp_eventing_total_backlog_size,

        ep_dcp_other_count,
        ep_dcp_other_items_remaining,
        ep_dcp_other_producer_count,
        ep_dcp_other_total_backlog_size).

-define(DCP_STAT_COUNTERS,
        ep_dcp_replica_items_sent,
        ep_dcp_replica_total_bytes,
        ep_dcp_replica_backoff,

        ep_dcp_xdcr_items_sent,
        ep_dcp_xdcr_total_bytes,
        ep_dcp_xdcr_backoff,

        ep_dcp_views_items_sent,
        ep_dcp_views_total_bytes,
        ep_dcp_views_backoff,

        ep_dcp_2i_items_sent,
        ep_dcp_2i_total_bytes,
        ep_dcp_2i_backoff,

        ep_dcp_fts_items_sent,
        ep_dcp_fts_total_bytes,
        ep_dcp_fts_backoff,

        ep_dcp_cbas_items_sent,
        ep_dcp_cbas_total_bytes,
        ep_dcp_cbas_backoff,

        ep_dcp_eventing_items_sent,
        ep_dcp_eventing_total_bytes,
        ep_dcp_eventing_backoff,

        ep_dcp_other_items_sent,
        ep_dcp_other_total_bytes,
        ep_dcp_other_backoff).

-ifdef(NEED_DCP_STREAM_STATS_CODE).

-define(DEFINE_EXTRACT(N), extract_agg_dcp_stat(<<??N>>, V, Acc) ->
               Acc#dcp_stream_stats{N = list_to_integer(binary_to_list(V))}).
?DEFINE_EXTRACT(count);
?DEFINE_EXTRACT(items_remaining);
?DEFINE_EXTRACT(items_sent);
?DEFINE_EXTRACT(producer_count);
?DEFINE_EXTRACT(total_backlog_size);
?DEFINE_EXTRACT(total_bytes);
?DEFINE_EXTRACT(backoff);
extract_agg_dcp_stat(_K, _V, Acc) -> Acc.
-undef(DEFINE_EXTRACT).

-define(DEFINE_TO_KVLIST(N), {<<Prefix/binary, ??N>>, list_to_binary(integer_to_list(Record#dcp_stream_stats.N))}).
dcp_stream_stats_to_kvlist(Prefix, Record) ->
    [?DEFINE_TO_KVLIST(count),
     ?DEFINE_TO_KVLIST(items_remaining),
     ?DEFINE_TO_KVLIST(items_sent),
     ?DEFINE_TO_KVLIST(producer_count),
     ?DEFINE_TO_KVLIST(total_backlog_size),
     ?DEFINE_TO_KVLIST(total_bytes),
     ?DEFINE_TO_KVLIST(backoff)].
-undef(DEFINE_TO_KVLIST).

-define(DEFINE_FORMULA(N), N = A#dcp_stream_stats.N + B#dcp_stream_stats.N).

add_dcp_stats(A, B) ->
    #dcp_stream_stats{?DEFINE_FORMULA(count),
                      ?DEFINE_FORMULA(items_remaining),
                      ?DEFINE_FORMULA(items_sent),
                      ?DEFINE_FORMULA(producer_count),
                      ?DEFINE_FORMULA(total_backlog_size),
                      ?DEFINE_FORMULA(total_bytes),
                      ?DEFINE_FORMULA(backoff)}.

-undef(DEFINE_FORMULA).

-define(DEFINE_FORMULA(N), N =
            H#dcp_stream_stats.N -
            (A#dcp_stream_stats.N + B#dcp_stream_stats.N +
                 C#dcp_stream_stats.N + D#dcp_stream_stats.N +
                 E#dcp_stream_stats.N + F#dcp_stream_stats.N +
                 G#dcp_stream_stats.N)).

calc_dcp_other_stats(A, B, C, D, E, F, G, H) ->
    #dcp_stream_stats{?DEFINE_FORMULA(count),
                      ?DEFINE_FORMULA(items_remaining),
                      ?DEFINE_FORMULA(items_sent),
                      ?DEFINE_FORMULA(producer_count),
                      ?DEFINE_FORMULA(total_backlog_size),
                      ?DEFINE_FORMULA(total_bytes),
                      ?DEFINE_FORMULA(backoff)}.
-undef(DEFINE_FORMULA).

-endif. % NEED_DCP_STREAM_STATS_CODE
