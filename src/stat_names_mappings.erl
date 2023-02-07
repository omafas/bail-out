%% @author Couchbase <info@couchbase.com>
%% @copyright 2018-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(stat_names_mappings).

-export([pre_70_stats_to_prom_query/3, prom_name_to_pre_70_name/2,
         handle_stats_mapping_get/3, default_stat_list/1]).

-include("ns_test.hrl").
-include("ns_stats.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(IRATE_INTERVAL, "1m").

-define(OR_GROUP_SIZE, 16).

handle_stats_mapping_get(Section, StatTokens, Req) ->
    StatName = lists:flatten(lists:join("/", StatTokens)),
    Stats = case StatName of
                "all" -> all;
                S -> [list_to_binary(S)]
            end,
    [Query] = pre_70_stats_to_prom_query(Section, undefined, Stats, #{}),
    menelaus_util:reply_text(Req, Query, 200).

pre_70_stats_to_prom_query(Section, Stats, Opts) ->
    pre_70_stats_to_prom_query(Section, ?OR_GROUP_SIZE, Stats, Opts).

pre_70_stats_to_prom_query("@system-processes" = Section, OrGroupSize, all,
                           Opts) ->
    SpecialMetrics =
        [<<"*/major_faults">>, <<"*/minor_faults">>, <<"*/page_faults">>],
    AstList =
        [{[{eq, <<"category">>, <<"system-processes">>}]}] ++
        [Ast || M <- SpecialMetrics,
                {ok, Ast} <- [pre_70_stat_to_prom_query(Section, M, Opts)]],
    [promQL:format_promql({union, SubGroup})
        || SubGroup <- misc:split(OrGroupSize, AstList)];
pre_70_stats_to_prom_query("@global", _, all, _Opts) ->
    [<<"{category=`audit`}">>];
pre_70_stats_to_prom_query(StatSection, OrGroupSize, all, Opts) ->
    pre_70_stats_to_prom_query(StatSection, OrGroupSize,
                               default_stat_list(StatSection), Opts);
pre_70_stats_to_prom_query(StatSection, OrGroupSize, List, Opts) ->
    AstList = lists:filtermap(
                fun (S) ->
                    case pre_70_stat_to_prom_query(StatSection, S, Opts) of
                        {ok, R} -> {true, R};
                        {error, not_found} -> false
                    end
                end, [bin(S) || S <- List]),

    [promQL:format_promql({union, SubGroup})
        || SubGroup <- misc:split(OrGroupSize, AstList)].

pre_70_stat_to_prom_query("@system", <<"rest_requests">>, Opts) ->
    {ok, promQL:rate({[{eq, <<"name">>, <<"cm_rest_request_leaves_total">>}]},
                     Opts)};
pre_70_stat_to_prom_query("@system", <<"hibernated_waked">>, Opts) ->
    {ok, promQL:rate({[{eq, <<"name">>, <<"cm_request_unhibernates_total">>}]},
                     Opts)};
pre_70_stat_to_prom_query("@system", <<"hibernated_requests">>, _Opts) ->
    {ok, promQL:named(
           <<"sys_hibernated_requests">>,
           {'-', [{ignoring, [<<"name">>]}],
            [{[{eq, <<"name">>, <<"cm_request_hibernates_total">>}]},
             {[{eq, <<"name">>, <<"cm_request_unhibernates_total">>}]}]})};
pre_70_stat_to_prom_query("@system", Stat, _Opts) ->
    case is_system_stat(Stat) of
        true -> {ok, {[{eq, <<"name">>, <<"sys_", Stat/binary>>}]}};
        false -> {error, not_found}
    end;

pre_70_stat_to_prom_query("@system-processes", Stat, Opts) ->
    case binary:split(Stat, <<"/">>) of
        [ProcName, Counter] when Counter == <<"major_faults">>;
                                 Counter == <<"minor_faults">>;
                                 Counter == <<"page_faults">> ->
            Name = <<"sysproc_", Counter/binary>>,
            Metric = {[{eq, <<"name">>, <<Name/binary, "_raw">>}] ++
                      [{eq, <<"proc">>, ProcName} || ProcName =/= <<"*">>]},
            {ok, promQL:named(Name, promQL:rate(Metric, Opts))};
        [ProcName, MetricName] ->
            case is_sysproc_stat(MetricName) of
                true ->
                    {ok, {[{eq, <<"name">>, <<"sysproc_", MetricName/binary>>},
                           {eq, <<"proc">>, ProcName}]}};
                false ->
                    {error, not_found}
            end;
        _ ->
            {error, not_found}
    end;

pre_70_stat_to_prom_query("@global", Stat, _Opts) ->
    {ok, {[{eq, <<"name">>, Stat}]}};

pre_70_stat_to_prom_query("@query", <<"query_", Stat/binary>>, Opts) ->
    Gauges = [<<"active_requests">>, <<"queued_requests">>],
    case lists:member(Stat, Gauges) of
        true -> {ok, {[{eq, <<"name">>, <<"n1ql_", Stat/binary>>}]}};
        false ->
            {ok, promQL:rate({[{eq, <<"name">>, <<"n1ql_", Stat/binary>>}]},
                             Opts)}
    end;

pre_70_stat_to_prom_query("@fts", <<"fts_", _/binary>> = Stat, _Opts) ->
    {ok, {[{eq, <<"name">>, Stat}]}};

pre_70_stat_to_prom_query("@fts-" ++ Bucket, <<"fts/", Stat/binary>>, Opts) ->
    map_index_stats(<<"fts">>, get_counters(fts), Bucket, Stat, Opts);

pre_70_stat_to_prom_query("@index", <<"index_ram_percent">>, _Opts) ->
    {ok, promQL:named(
           <<"index_ram_percent">>,
           {'*', [{'/', [{ignoring, [<<"name">>]}],
                   [promQL:metric(<<"index_memory_used_total">>),
                    promQL:metric(<<"index_memory_quota">>)]}, 100]})};
pre_70_stat_to_prom_query("@index", <<"index_remaining_ram">>, _Opts) ->
    {ok, promQL:named(<<"index_remaining_ram">>,
                      {'-', [{ignoring, [<<"name">>]}],
                       [promQL:metric(<<"index_memory_quota">>),
                        promQL:metric(<<"index_memory_used_total">>)]})};
pre_70_stat_to_prom_query("@index", <<"index_memory_used">>, _Opts) ->
    {ok, promQL:metric(<<"index_memory_used_total">>)};
pre_70_stat_to_prom_query("@index", <<"index_", _/binary>> = Stat, _Opts) ->
    {ok, promQL:metric(Stat)};

pre_70_stat_to_prom_query("@index-" ++ Bucket, <<"index/", Stat/binary>>,
                          Opts) ->
    map_index_stats(<<"index">>, get_counters(index), Bucket, Stat, Opts);

pre_70_stat_to_prom_query("@cbas", <<"cbas_disk_used">>, _Opts) ->
    {ok, promQL:metric(<<"cbas_disk_used_bytes_total">>)};
pre_70_stat_to_prom_query("@cbas", <<"cbas_gc_count">>, Opts) ->
    {ok, promQL:rate(promQL:metric(<<"cbas_gc_count_total">>), Opts)};
pre_70_stat_to_prom_query("@cbas", <<"cbas_gc_time">>, Opts) ->
    {ok, promQL:rate(promQL:metric(<<"cbas_gc_time_milliseconds_total">>),
                     Opts)};
pre_70_stat_to_prom_query("@cbas", <<"cbas_heap_used">>, _Opts) ->
    {ok, promQL:metric(<<"cbas_heap_memory_used_bytes">>)};
pre_70_stat_to_prom_query("@cbas", <<"cbas_system_load_average">>, _Opts) ->
    {ok, promQL:metric(<<"cbas_system_load_average">>)};
pre_70_stat_to_prom_query("@cbas", <<"cbas_thread_count">>, _Opts) ->
    {ok, promQL:metric(<<"cbas_thread_count">>)};
pre_70_stat_to_prom_query("@cbas", <<"cbas_io_reads">>, Opts) ->
    {ok, promQL:rate(promQL:metric(<<"cbas_io_reads_total">>), Opts)};
pre_70_stat_to_prom_query("@cbas", <<"cbas_io_writes">>, Opts) ->
    {ok, promQL:rate(promQL:metric(<<"cbas_io_writes_total">>), Opts)};

pre_70_stat_to_prom_query("@cbas-" ++ Bucket, <<"cbas/", Stat/binary>>, Opts) ->
    Incoming = {[{eq, <<"name">>, <<"cbas_incoming_records_count">>},
                 {eq, <<"bucket">>, Bucket},
                 {eq, <<"link">>, <<"Local">>}]},
    Failed = {[{eq, <<"name">>, <<"cbas_failed_to_parse_records_count">>},
               {eq, <<"bucket">>, Bucket},
               {eq, <<"link">>, <<"Local">>}]},
    case Stat of
        <<"incoming_records_count_total">> ->
            {ok, promQL:named(<<"cbas_incoming_records_count_total">>,
                              promQL:sum(Incoming))};
        <<"all/incoming_records_count_total">> ->
            {ok, promQL:named(<<"cbas_all_incoming_records_count_total">>,
                              promQL:sum(Incoming))};
        <<"failed_at_parser_records_count_total">> ->
            {ok, promQL:named(<<"cbas_failed_at_parser_records_count_total">>,
                              promQL:sum(Failed))};
        <<"all/failed_at_parser_records_count_total">> ->
            {ok, promQL:named(<<"cbas_all_failed_at_parser_records_count_total">>,
                              promQL:sum(Failed))};
        <<"incoming_records_count">> ->
            {ok, promQL:named(<<"cbas_incoming_records_count">>,
                              promQL:sum(promQL:rate(Incoming, Opts)))};
        <<"all/incoming_records_count">> ->
            {ok, promQL:named(<<"cbas_all_incoming_records_count">>,
                              promQL:sum(promQL:rate(Incoming, Opts)))};
        <<"failed_at_parser_records_count">> ->
            {ok, promQL:named(<<"cbas_failed_at_parser_records_count">>,
                              promQL:sum(promQL:rate(Failed, Opts)))};
        <<"all/failed_at_parser_records_count">> ->
            {ok, promQL:named(<<"cbas_all_failed_at_parser_records_count">>,
                              promQL:sum(promQL:rate(Failed, Opts)))};
        _ ->
            {error, not_found}
    end;

pre_70_stat_to_prom_query("@xdcr-" ++ Bucket, <<"replication_changes_left">>,
                          _Opts) ->
    M = {[{eq, <<"name">>, <<"xdcr_changes_left_total">>},
          {eq, <<"sourceBucketName">>, Bucket}]},
    {ok, promQL:sum_by([<<"name">>], M)};
pre_70_stat_to_prom_query("@xdcr-" ++ Bucket,
                          <<"replication_docs_rep_queue">>, _Opts) ->
    M = {[{eq, <<"name">>, <<"xdcr_docs_rep_queue_total">>},
          {eq, <<"sourceBucketName">>, Bucket}]},
    {ok, promQL:sum_by([<<"name">>], M)};
pre_70_stat_to_prom_query("@xdcr-" ++ Bucket,
                          <<"replications/", Stat/binary>>, Opts) ->
    BucketBin = list_to_binary(Bucket),
    [ReplId, Source, Target, Name] = binary:split(Stat, <<"/">>, [global]),
    Metric = fun (N) ->
                 {RId, Type} =
                    case ReplId of
                        <<"*">> -> {<<"*">>, <<"*">>};
                        <<"backfill_", Id/binary>> -> {Id, <<"Backfill">>};
                        Id -> {Id, <<"Main">>}
                    end,
                 {[{eq, <<"name">>, N}, {eq, <<"sourceBucketName">>, Bucket}] ++
                  [{eq, <<"pipelineType">>, Type} || Type =/= <<"*">>] ++
                  [{eq, <<"targetClusterUUID">>, RId} || RId =/= <<"*">>] ++
                  [{eq, <<"targetBucketName">>, Target} || Target =/= <<"*">>]}
             end,
    case Name of
        _ when Source =/= BucketBin ->
            {error, not_found};
        N when N =:= <<"time_committing">>;
               N =:= <<"wtavg_docs_latency">>;
               N =:= <<"wtavg_get_latency">>;
               N =:= <<"wtavg_meta_latency">>;
               N =:= <<"throughput_throttle_latency">>;
               N =:= <<"resp_wait_time">>;
               N =:= <<"throttle_latency">> ->
            M = Metric(<<"xdcr_", N/binary, "_seconds">>),
            {ok, promQL:convert_units(seconds, milliseconds, M)};
        <<"dcp_dispatch_time">> ->
            M = Metric(<<"xdcr_dcp_dispatch_time_seconds">>),
            {ok, promQL:convert_units(seconds, nanoseconds, M)};
        <<"bandwidth_usage">> ->
            M = promQL:rate(Metric(<<"xdcr_data_replicated_bytes">>), Opts),
            {ok, promQL:named(<<"xdcr_bandwidth_usage_bytes_per_second">>, M)};
        <<"rate_doc_checks">> ->
            {ok, promQL:named(
                   <<"xdcr_rate_doc_checks_docs_per_second">>,
                   promQL:idelta(Metric(<<"xdcr_docs_checked_total">>), Opts))};
        <<"rate_received_from_dcp">> ->
            M = promQL:rate(Metric(<<"xdcr_docs_received_from_dcp_total">>),
                            Opts),
            {ok, promQL:named(<<"xdcr_rate_received_from_dcp_docs_per_second">>,
                              M)};
        <<"rate_doc_opt_repd">> ->
            M = promQL:rate(Metric(<<"xdcr_docs_opt_repd_total">>), Opts),
            {ok, promQL:named(<<"xdcr_rate_doc_opt_repd_docs_per_second">>, M)};
        <<"rate_replicated">> ->
            M = promQL:rate(Metric(<<"xdcr_docs_written_total">>), Opts),
            {ok, promQL:named(<<"xdcr_rate_replicated_docs_per_second">>, M)};
        N when N =:= <<"deletion_filtered">>;
               N =:= <<"expiry_received_from_dcp">>;
               N =:= <<"docs_opt_repd">>;
               N =:= <<"deletion_failed_cr_source">>;
               N =:= <<"set_filtered">>;
               N =:= <<"datapool_failed_gets">>;
               N =:= <<"dcp_datach_length">>;
               N =:= <<"docs_filtered">>;
               N =:= <<"docs_checked">>;
               N =:= <<"set_received_from_dcp">>;
               N =:= <<"docs_unable_to_filter">>;
               N =:= <<"set_failed_cr_source">>;
               N =:= <<"set_docs_written">>;
               N =:= <<"docs_written">>;
               N =:= <<"deletion_docs_written">>;
               N =:= <<"expiry_docs_written">>;
               N =:= <<"num_failedckpts">>;
               N =:= <<"add_docs_written">>;
               N =:= <<"docs_rep_queue">>;
               N =:= <<"docs_failed_cr_source">>;
               N =:= <<"deletion_received_from_dcp">>;
               N =:= <<"num_checkpoints">>;
               N =:= <<"expiry_filtered">>;
               N =:= <<"expiry_stripped">>;
               N =:= <<"changes_left">>;
               N =:= <<"docs_processed">>;
               N =:= <<"docs_received_from_dcp">>;
               N =:= <<"expiry_failed_cr_source">> ->
            {ok, Metric(<<"xdcr_", N/binary, "_total">>)};
        N when N =:= <<"size_rep_queue">>;
               N =:= <<"data_replicated">> ->
            {ok, Metric(<<"xdcr_", N/binary, "_bytes">>)};
        _ ->
            {error, not_found}
    end;

pre_70_stat_to_prom_query("@eventing", <<"eventing/", Stat/binary>>, _Opts) ->
    case binary:split(Stat, <<"/">>, [global]) of
        [<<"failed_count">>] ->
            Metrics = [eventing_metric(bin(M), <<"*">>)
                          || M <- eventing_failures()],
            {ok, promQL:named(<<"eventing_failed_count">>,
                              promQL:sum({union, Metrics}))};
        [FunctionName, <<"failed_count">>] ->
            Metrics = [eventing_metric(bin(M), FunctionName)
                           || M <- eventing_failures()],
            {ok, promQL:named(<<"eventing_failed_count">>,
                              promQL:sum_by([<<"functionName">>],
                                            {union, Metrics}))};
        [<<"processed_count">>] ->
            Metrics = [eventing_metric(bin(M), <<"*">>)
                           || M <- eventing_successes()],
            {ok, promQL:named(<<"eventing_processed_count">>,
                              promQL:sum({union, Metrics}))};
        [FunctionName, <<"processed_count">>] ->
            Metrics = [eventing_metric(bin(M), FunctionName)
                           || M <- eventing_successes()],
            {ok, promQL:named(<<"eventing_processed_count">>,
                              promQL:sum_by([<<"functionName">>],
                                            {union, Metrics}))};
        [N] ->
            {ok, promQL:sum_by([<<"name">>], eventing_metric(N, <<"*">>))};
        [FunctionName, N] ->
            Metric = eventing_metric(N, FunctionName),
            {ok, promQL:sum_by([<<"name">>, <<"functionName">>], Metric)};
        _ ->
            {error, not_found}
    end;

%% Starting from Chesire-Cat eventing functions are not necessarily associated
%% with a bucket and the bucket label is removed from all metrics.
%% Because of that @eventing-bucket stats don't make any sense anymore.
pre_70_stat_to_prom_query("@eventing-" ++ _Bucket, _, _Opts) ->
    {error, not_found};

pre_70_stat_to_prom_query("@" ++ _, _, _Opts) ->
    {error, not_found};

%% "dcpagg :" stats
pre_70_stat_to_prom_query(Bucket, <<"ep_dcp_replica_", Stat/binary>>, Opts) ->
    TypeLabel = {eq, <<"connection_type">>, <<"replication">>},
    {ok, map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts)};
pre_70_stat_to_prom_query(Bucket, <<"ep_dcp_xdcr_", Stat/binary>>, Opts) ->
    TypeLabel = {eq, <<"connection_type">>, <<"xdcr">>},
    {ok, map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts)};
pre_70_stat_to_prom_query(Bucket, <<"ep_dcp_2i_", Stat/binary>>, Opts) ->
    TypeLabel = {eq, <<"connection_type">>, <<"secidx">>},
    {ok, map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts)};
pre_70_stat_to_prom_query(Bucket, <<"ep_dcp_fts_", Stat/binary>>, Opts) ->
    TypeLabel = {eq, <<"connection_type">>, <<"fts">>},
    {ok, map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts)};
pre_70_stat_to_prom_query(Bucket, <<"ep_dcp_eventing_", Stat/binary>>, Opts) ->
    TypeLabel = {eq, <<"connection_type">>, <<"eventing">>},
    {ok, map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts)};
pre_70_stat_to_prom_query(Bucket, <<"ep_dcp_cbas_", Stat/binary>>, Opts) ->
    TypeLabel = {eq, <<"connection_type">>, <<"cbas">>},
    {ok, map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts)};
pre_70_stat_to_prom_query(Bucket, <<"ep_dcp_views_", Stat/binary>>, Opts) ->
    TypeLabel = {eq_any, <<"connection_type">>, [<<"mapreduce_view">>,
                                                 <<"spatial_view">>]},
    ViewsStats = promQL:sum_by([<<"name">>],
                               map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts)),
    {ok, promQL:with_label(<<"connection_type">>, <<"views">>, ViewsStats)};
pre_70_stat_to_prom_query(Bucket, <<"ep_dcp_other_", Stat/binary>>, Opts) ->
    TypeLabel = {not_any, <<"connection_type">>,
                 [<<"replication">>, <<"xdcr">>, <<"fts">>, <<"cbas">>,
                  <<"eventing">>, <<"secidx">>, <<"mapreduce_view">>,
                  <<"spatial_view">>]},
    ViewsStats = promQL:sum_by([<<"name">>],
                               map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts)),
    {ok, promQL:with_label(<<"connection_type">>, <<"other">>, ViewsStats)};
%% Exceptions that are not handled by kv_stats_mappings for one reason
%% on another
pre_70_stat_to_prom_query(Bucket, <<"cmd_get">>, Opts) ->
    M = {[{eq, <<"name">>, <<"kv_ops">>},
          {eq, <<"bucket">>, list_to_binary(Bucket)},
          {eq, <<"op">>, <<"get">>}]},
    {ok, promQL:named(<<"kv_cmd_get">>, promQL:sum(promQL:rate(M, Opts)))};
pre_70_stat_to_prom_query(Bucket, <<"couch_docs_disk_size">>, _Opts) ->
    M = promQL:bucket_metric(<<"kv_ep_db_file_size_bytes">>, Bucket),
    {ok, promQL:named(<<"couch_docs_disk_size">>, promQL:sum(M))};

pre_70_stat_to_prom_query(Bucket, <<"couch_docs_data_size">>, _Opts) ->
    Config = ns_config:latest(),
    MinFileSize0 = ns_config:search_node_prop(Config, compaction_daemon,
                                              min_file_size, 131072),
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket),
    MinFileSize =
        MinFileSize0 * length(ns_bucket:all_node_vbuckets(BucketConfig)),

    FileSize = promQL:bucket_metric(<<"kv_ep_db_file_size_bytes">>, Bucket),
    DataSize = promQL:bucket_metric(<<"kv_ep_db_data_size_bytes">>, Bucket),

    {ok, promQL:named(
           <<"couch_docs_data_size">>,
           promQL:max(promQL:op('or', [{'<', [FileSize, MinFileSize]},
                                       DataSize])))};

pre_70_stat_to_prom_query(Bucket, <<"disk_write_queue">>, _Opts) ->
    M = {union, [promQL:bucket_metric(<<"kv_ep_queue_size">>, Bucket),
                 promQL:bucket_metric(<<"kv_ep_flusher_todo">>, Bucket)]},
    {ok, promQL:named(<<"kv_disk_write_queue">>, promQL:sum(M))};
pre_70_stat_to_prom_query(Bucket, <<"ep_ops_create">>, Opts) ->
    M = promQL:rate(promQL:bucket_metric(<<"kv_vb_ops_create">>, Bucket), Opts),
    {ok, promQL:named(<<"kv_ep_ops_create">>, promQL:sum_by([], M))};
pre_70_stat_to_prom_query(Bucket, <<"ep_ops_update">>, Opts) ->
    M = promQL:rate(promQL:bucket_metric(<<"kv_vb_ops_update">>, Bucket), Opts),
    {ok, promQL:named(<<"kv_ep_ops_update">>, promQL:sum_by([], M))};
pre_70_stat_to_prom_query(Bucket, <<"misses">>, Opts) ->
    M = {[{eq, <<"name">>, <<"kv_ops">>},
          {eq, <<"bucket">>, list_to_binary(Bucket)},
          {eq, <<"result">>, <<"miss">>}]},
    {ok, promQL:named(<<"kv_misses">>, promQL:sum(promQL:rate(M, Opts)))};
pre_70_stat_to_prom_query(Bucket, <<"ops">>, Opts) ->
    Metrics = [promQL:rate(promQL:bucket_metric(<<"kv_cmd_lookup">>, Bucket),
                           Opts),
               promQL:rate({[{eq, <<"name">>, <<"kv_ops">>},
                             {eq, <<"bucket">>, list_to_binary(Bucket)},
                             {eq_any, <<"op">>,
                              [<<"set">>, <<"incr">>, <<"decr">>, <<"delete">>,
                               <<"del_meta">>, <<"get_meta">>,<<"set_meta">>,
                               <<"set_ret_meta">>,<<"del_ret_meta">>]}]},
                           Opts)],
    {ok, promQL:named(<<"kv_old_ops">>, promQL:sum({union, Metrics}))};
pre_70_stat_to_prom_query(Bucket, <<"vb_total_queue_age">>, _Opts) ->
    M = promQL:bucket_metric(<<"kv_vb_queue_age_seconds">>,
                             list_to_binary(Bucket)),
    {ok, promQL:named(<<"kv_vb_total_queue_age">>,
                      promQL:convert_units(seconds, milliseconds,
                                           promQL:sum(M)))};
pre_70_stat_to_prom_query(Bucket, <<"vb_total_queue_size">>, _Opts) ->
    M = promQL:bucket_metric(<<"kv_vb_queue_size">>,
                             list_to_binary(Bucket)),
    {ok, promQL:named(<<"kv_vb_total_queue_size">>, promQL:sum(M))};
pre_70_stat_to_prom_query(Bucket, <<"xdc_ops">>, Opts) ->
    M = {[{eq, <<"name">>, <<"kv_ops">>},
          {eq, <<"bucket">>, list_to_binary(Bucket)},
          {eq_any, <<"op">>, [<<"del_meta">>, <<"get_meta">>,
                              <<"set_meta">>]}]},
    {ok, promQL:named(<<"kv_xdc_ops">>, promQL:sum(promQL:rate(M, Opts)))};
%% Timings metrics:
pre_70_stat_to_prom_query(Bucket, <<"bg_wait_count">>, Opts) ->
    {ok, promQL:rate(promQL:bucket_metric(<<"kv_bg_wait_seconds_count">>,
                                          Bucket), Opts)};
pre_70_stat_to_prom_query(Bucket, <<"bg_wait_total">>, Opts) ->
    M = promQL:rate(promQL:bucket_metric(<<"kv_bg_wait_seconds_sum">>,
                                         Bucket), Opts),
    {ok, promQL:convert_units(seconds, microseconds, M)};
pre_70_stat_to_prom_query(Bucket, <<"disk_commit_count">>, Opts) ->
    {ok, promQL:rate({[{eq, <<"name">>, <<"kv_disk_seconds_count">>},
                       {eq, <<"op">>, <<"commit">>},
                       {eq, <<"bucket">>, Bucket}]}, Opts)};
pre_70_stat_to_prom_query(Bucket, <<"disk_commit_total">>, Opts) ->
    M = promQL:rate({[{eq, <<"name">>, <<"kv_disk_seconds_sum">>},
                      {eq, <<"op">>, <<"commit">>},
                      {eq, <<"bucket">>, Bucket}]}, Opts),
    {ok, promQL:convert_units(seconds, microseconds, M)};
pre_70_stat_to_prom_query(Bucket, <<"disk_update_count">>, Opts) ->
    {ok, promQL:rate({[{eq, <<"name">>, <<"kv_disk_seconds_count">>},
                       {eq, <<"op">>, <<"update">>},
                       {eq, <<"bucket">>, Bucket}]}, Opts)};
pre_70_stat_to_prom_query(Bucket, <<"disk_update_total">>, Opts) ->
    M = promQL:rate({[{eq, <<"name">>, <<"kv_disk_seconds_sum">>},
                      {eq, <<"op">>, <<"update">>},
                      {eq, <<"bucket">>, Bucket}]}, Opts),
    {ok, promQL:convert_units(seconds, microseconds, M)};
%% Couchdb metrics:
pre_70_stat_to_prom_query(Bucket, N, Opts) when N =:= <<"couch_views_ops">>;
                                                N =:= <<"couch_spatial_ops">> ->
    {ok, promQL:sum_by([<<"name">>],
                       promQL:rate(promQL:bucket_metric(N, Bucket), Opts))};
pre_70_stat_to_prom_query(Bucket, <<"couch_", _/binary>> = N, _Opts) ->
    {ok, promQL:sum_by([<<"name">>], promQL:bucket_metric(N, Bucket))};
pre_70_stat_to_prom_query(Bucket, <<"spatial/", Name/binary>>, Opts) ->
    Metric = fun (MetricName, Id) ->
                 {[{eq, <<"name">>, <<"couch_spatial_", MetricName/binary>>},
                   {eq, <<"bucket">>, list_to_binary(Bucket)}] ++
                  [{eq, <<"signature">>, Id} || Id =/= <<"*">>]}
             end,
    case binary:split(Name, <<"/">>) of
        [Id, <<"accesses">>] ->
            {ok, promQL:rate(Metric(<<"ops">>, Id), Opts)};
        [Id, Stat] ->
            {ok, Metric(Stat, Id)};
        _ ->
            {error, not_found}
    end;
pre_70_stat_to_prom_query(Bucket, <<"views/", Name/binary>>, Opts) ->
    Metric = fun (MetricName, Id) ->
                 {[{eq, <<"name">>, <<"couch_views_", MetricName/binary>>},
                   {eq, <<"bucket">>, list_to_binary(Bucket)}] ++
                  [{eq, <<"signature">>, Id} || Id =/= <<"*">>]}
             end,
    case binary:split(Name, <<"/">>) of
        [Id, <<"accesses">>] ->
            {ok, promQL:rate(Metric(<<"ops">>, Id), Opts)};
        [Id, Stat] ->
            {ok, Metric(Stat, Id)};
        _ ->
            {error, not_found}
    end;
%% Memcached "empty key" metrics:
pre_70_stat_to_prom_query(_Bucket, <<"curr_connections">>, _Opts) ->
    %% curr_connections can't be handled like other metrics because
    %% it's not actually a "per-bucket" metric, but a global metric
    {ok, promQL:metric(<<"kv_curr_connections">>)};
pre_70_stat_to_prom_query(Bucket, Stat, Opts) ->
    case kv_stats_mappings:old_to_new(Stat) of
        {ok, {Type, {Metric, Labels}, {OldUnit, NewUnit}}} ->
            M = {[{eq, <<"name">>, Metric},
                  {eq, <<"bucket">>, list_to_binary(Bucket)}] ++
                 [{eq, K, V} || {K, V} <- Labels]},
            case Type of
                counter ->
                    {ok, promQL:convert_units(NewUnit, OldUnit,
                                              promQL:rate(M, Opts))};
                gauge ->
                    {ok, promQL:convert_units(NewUnit, OldUnit, M)}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

map_dcpagg_stat(TypeLabel, Stat, Bucket, Opts) ->
    Suffix = case Stat of
                <<"count">> -> <<"connection_count">>;
                <<"total_bytes">> -> <<"total_data_size_bytes">>;
                _ -> Stat
             end,
    Metric = {[{eq, <<"name">>, <<"kv_dcp_", Suffix/binary>>},
               {eq, <<"bucket">>, list_to_binary(Bucket)},
               TypeLabel]},
    IsCounter = case Stat of
               <<"items_sent">> -> true;
               <<"total_bytes">> -> true;
               <<"backoff">> -> true;
               _ -> false
           end,
    case IsCounter of
        true -> promQL:rate(Metric, Opts);
        false -> Metric
    end.

%% Works for fts and index, Prefix is the only difference
map_index_stats(Prefix, Counters, Bucket, Stat, Opts) ->
    IsCounter =
        fun (N) ->
            try
                lists:member(binary_to_existing_atom(N, latin1), Counters)
            catch
                _:_ -> false
            end
        end,
    case binary:split(Stat, <<"/">>, [global]) of
        [<<"disk_overhead_estimate">> = N] ->
              DiskSize = promQL:bucket_metric(<<Prefix/binary, "_disk_size">>,
                                              Bucket),
              FragPerc = promQL:bucket_metric(<<Prefix/binary,
                                              "_frag_percent">>,
                                              Bucket),
              Name = <<Prefix/binary, "_", N/binary>>,
              Product = {'*', [{ignoring, [<<"name">>]}], [DiskSize, FragPerc]},
              {ok, promQL:named(Name, {'/', [promQL:sum(Product), 100]})};
        [Index,  <<"disk_overhead_estimate">> = N] ->
              DiskSize = index_metric(<<Prefix/binary, "_disk_size">>,
                                      Bucket, Index),
              FragPerc = index_metric(<<Prefix/binary, "_frag_percent">>,
                                      Bucket, Index),
              Name = <<Prefix/binary, "_", N/binary>>,
              {ok, promQL:named(Name, {'/', [{'*', [{ignoring, [<<"name">>]}],
                                              [DiskSize, FragPerc]}, 100]})};
        [N] ->
            Name = <<Prefix/binary, "_", N/binary>>,
            case IsCounter(N) of
                true ->
                    {ok, promQL:sum_by(
                           [<<"name">>],
                           promQL:rate(promQL:bucket_metric(Name, Bucket),
                                       Opts))};
                false ->
                    {ok, promQL:sum_by([<<"name">>],
                                      promQL:bucket_metric(Name, Bucket))}
            end;
        [Index, N] ->
            Name = <<Prefix/binary, "_", N/binary>>,
            case IsCounter(N) of
                true ->
                    {ok, promQL:rate(index_metric(Name, Bucket, Index), Opts)};
                false ->
                    {ok, index_metric(Name, Bucket, Index)}
            end;
        _ ->
            {error, not_found}
    end.

index_metric(Name, Bucket, Index) ->
    {[{eq, <<"name">>, Name}, {eq, <<"bucket">>, Bucket}] ++
     [{eq, <<"index">>, Index} || Index =/= <<"*">>]}.

eventing_metric(Name, FunctionName) ->
    {[{eq, <<"name">>, <<"eventing_", (bin(Name))/binary>>}] ++
     [{eq, <<"functionName">>, FunctionName} || FunctionName =/= <<"*">>]}.


bin(A) when is_atom(A) -> atom_to_binary(A, latin1);
bin(L) when is_list(L) -> list_to_binary(L);
bin(B) when is_binary(B) -> B.

prom_name_to_pre_70_name(Bucket, {JSONProps}) ->
    Res =
        case proplists:get_value(<<"name">>, JSONProps) of
            <<"n1ql_", Name/binary>> ->
                {ok, <<"query_", Name/binary>>};
            <<"cm_rest_request_leaves_total">> ->
                {ok, <<"rest_requests">>};
            <<"cm_request_unhibernates_total">> ->
                {ok, <<"hibernated_waked">>};
            <<"sys_", Name/binary>> -> {ok, Name};
            <<"sysproc_", Name/binary>> ->
                Proc = proplists:get_value(<<"proc">>, JSONProps, <<>>),
                {ok, <<Proc/binary, "/", Name/binary>>};
            <<"audit_", _/binary>> = Name -> {ok, Name};
            <<"fts_", _/binary>> = Name when Bucket == "@fts" ->
                {ok, Name};
            <<"fts_", Name/binary>> -> %% for @fts-<bucket>
                case proplists:get_value(<<"index">>, JSONProps, <<>>) of
                    <<>> -> {ok, <<"fts/", Name/binary>>};
                    Index -> {ok, <<"fts/", Index/binary, "/", Name/binary>>}
                end;
            <<"index_memory_used_total">> when Bucket == "@index" ->
                {ok, <<"index_memory_used">>};
            <<"index_", _/binary>> = Name when Bucket == "@index" ->
                {ok, Name};
            <<"index_", Name/binary>> -> %% for @index-<bucket>
                case proplists:get_value(<<"index">>, JSONProps, <<>>) of
                    <<>> -> {ok, <<"index/", Name/binary>>};
                    Index -> {ok, <<"index/", Index/binary, "/", Name/binary>>}
                end;
            <<"cbas_disk_used_bytes_total">> ->
                {ok, <<"cbas_disk_used">>};
            <<"cbas_gc_count_total">> ->
                {ok, <<"cbas_gc_count">>};
            <<"cbas_gc_time_milliseconds_total">> ->
                {ok, <<"cbas_gc_time">>};
            <<"cbas_heap_memory_used_bytes">> ->
                {ok, <<"cbas_heap_used">>};
            <<"cbas_system_load_average">> ->
                {ok, <<"cbas_system_load_average">>};
            <<"cbas_thread_count">> ->
                {ok, <<"cbas_thread_count">>};
            <<"cbas_io_reads_total">> ->
                {ok, <<"cbas_io_reads">>};
            <<"cbas_io_writes_total">> ->
                {ok, <<"cbas_io_writes">>};
            <<"cbas_all_", Name/binary>> ->
                {ok, <<"cbas/all/", Name/binary>>};
            <<"cbas_", Name/binary>> ->
                {ok, <<"cbas/", Name/binary>>};
            <<"xdcr_", Name/binary>> ->
                build_pre_70_xdcr_name(Name, JSONProps);
            <<"eventing_", Name/binary>> ->
                case proplists:get_value(<<"functionName">>, JSONProps, <<>>) of
                    <<>> ->
                        {ok, <<"eventing/", Name/binary>>};
                    FName ->
                        {ok, <<"eventing/", FName/binary, "/", Name/binary>>}
                end;
            <<"kv_bg_wait_seconds_count">> ->
                {ok, <<"bg_wait_count">>};
            <<"kv_bg_wait_seconds_sum">> ->
                {ok, <<"bg_wait_total">>};
            <<"kv_disk_seconds_count">> ->
                case proplists:get_value(<<"op">>, JSONProps) of
                    <<"commit">> -> {ok, <<"disk_commit_count">>};
                    <<"update">> -> {ok, <<"disk_update_count">>};
                    _ -> {error, not_found}
                end;
            <<"kv_disk_seconds_sum">> ->
                case proplists:get_value(<<"op">>, JSONProps) of
                    <<"commit">> -> {ok, <<"disk_commit_total">>};
                    <<"update">> -> {ok, <<"disk_update_total">>};
                    _ -> {error, not_found}
                end;
            <<"kv_disk_write_queue">> ->
                {ok, <<"disk_write_queue">>};
            <<"kv_ep_ops_create">> ->
                {ok, <<"ep_ops_create">>};
            <<"kv_ep_ops_update">> ->
                {ok, <<"ep_ops_update">>};
            <<"kv_misses">> ->
                {ok, <<"misses">>};
            <<"kv_old_ops">> ->
                {ok, <<"ops">>};
            <<"kv_vb_total_queue_age">> ->
                {ok, <<"vb_total_queue_age">>};
            <<"kv_vb_total_queue_size">> ->
                {ok, <<"vb_total_queue_size">>};
            <<"kv_xdc_ops">> ->
                {ok, <<"xdc_ops">>};
            <<"kv_dcp_", Stat/binary>> ->
                case proplists:get_value(<<"connection_type">>, JSONProps) of
                    undefined -> {error, not_found};
                    Type ->
                        Type2 =
                            case Type of
                                <<"replication">> -> <<"replica">>;
                                <<"secidx">> -> <<"2i">>;
                                _ -> Type
                            end,
                        Suffix =
                            case Stat of
                                <<"connection_count">> -> <<"count">>;
                                <<"total_data_size_bytes">> -> <<"total_bytes">>;
                                _ -> Stat
                            end,
                        {ok, <<"ep_dcp_", Type2/binary, "_", Suffix/binary>>}
                end;
            <<"kv_", _/binary>> = Name ->
                DropLabels = [<<"name">>, <<"bucket">>, <<"job">>,
                              <<"category">>, <<"instance">>, <<"__name__">>],
                Filter = fun (L) -> not lists:member(L, DropLabels) end,
                Labels = misc:proplist_keyfilter(Filter, JSONProps),
                kv_stats_mappings:new_to_old({Name, lists:usort(Labels)});
            <<"couch_", _/binary>> = Name ->
                case proplists:get_value(<<"signature">>, JSONProps) of
                    undefined -> {ok, Name};
                    Sig ->
                        case Name of
                            <<"couch_spatial_ops">> ->
                                {ok, <<"spatial/", Sig/binary, "/accesses">>};
                            <<"couch_views_ops">> ->
                                {ok, <<"views/", Sig/binary, "/accesses">>};
                            <<"couch_spatial_", N/binary>> ->
                                {ok, <<"spatial/", Sig/binary, "/", N/binary>>};
                            <<"couch_views_", N/binary>> ->
                                {ok, <<"views/", Sig/binary, "/", N/binary>>}
                        end
                end;
            _ -> {error, not_found}
        end,
    case Res of
        {ok, <<"spatial/", _/binary>>} -> Res;
        {ok, <<"views/", _/binary>>} -> Res;
        {ok, BinName} ->
            %% Since pre-7.0 stats don't care much about stats name type,
            %% 7.0 stats have to convert names to correct types based on stat
            %% section.
            case key_type_by_stat_type(Bucket) of
                atom -> {ok, binary_to_atom(BinName, latin1)};
                binary -> {ok, BinName}
            end;
        {error, _} = Error ->
            Error
    end.

build_pre_70_xdcr_name(Name, Props) ->
    Suffixes = [<<"_total">>, <<"_seconds">>,
                <<"_bytes_per_second">>, <<"_docs_per_second">>,
                <<"_bytes">>],
    case drop_suffixes(Name, Suffixes) of
        {ok, Stripped} ->
            Id = proplists:get_value(<<"targetClusterUUID">>, Props),
            Source = proplists:get_value(<<"sourceBucketName">>, Props),
            Target = proplists:get_value(<<"targetBucketName">>, Props),
            Type = proplists:get_value(<<"pipelineType">>, Props),
            if
                Type   =:= <<"Backfill">>,
                Id     =/= undefined,
                Source =/= undefined,
                Target =/= undefined ->
                    {ok, <<"replications/backfill_", Id/binary, "/",
                           Source/binary, "/", Target/binary,"/",
                           Stripped/binary>>};
                Type   =:= <<"Main">>,
                Id     =/= undefined,
                Source =/= undefined,
                Target =/= undefined ->
                    {ok, <<"replications/", Id/binary, "/",
                           Source/binary, "/", Target/binary,"/",
                           Stripped/binary>>};
                (Stripped =:= <<"docs_rep_queue">>) or
                (Stripped =:= <<"changes_left">>),
                Id       =:= undefined,
                Source   =:= undefined,
                Target   =:= undefined ->
                    {ok, <<"replication_", Stripped/binary>>};
                true ->
                    {error, not_found}
            end;
        false ->
            {error, not_found}
    end.

drop_suffixes(Bin, Suffixes) ->
    Check = fun (Suffix) ->
                fun (NameToParse) ->
                    case misc:is_binary_ends_with(NameToParse, Suffix) of
                        true ->
                            L = byte_size(NameToParse) - byte_size(Suffix),
                            {ok, binary:part(NameToParse, {0, L})};
                        false ->
                            false
                    end
                end
            end,
    functools:alternative(Bin,[Check(S) || S <- Suffixes]).

key_type_by_stat_type("@query") -> atom;
key_type_by_stat_type("@global") -> atom;
key_type_by_stat_type("@system") -> atom;
key_type_by_stat_type("@system-processes") -> binary;
key_type_by_stat_type("@fts") -> binary;
key_type_by_stat_type("@fts-" ++ _) -> binary;
key_type_by_stat_type("@index") -> binary;
key_type_by_stat_type("@index-" ++ _) -> binary;
key_type_by_stat_type("@cbas") -> binary;
key_type_by_stat_type("@cbas-" ++ _) -> binary;
key_type_by_stat_type("@xdcr-" ++ _) -> binary;
key_type_by_stat_type("@eventing") -> binary;
key_type_by_stat_type("@eventing-" ++ _) -> binary;
key_type_by_stat_type(_) -> atom.


%% For @global stats it's simple, we can get all of them with a simple query
%% {category="audit"}. For most of other stats it's not always the case.
%% For example, for query we need to request rates for some stats, so we have
%% to know which stats should be rates and which stats should be plain. This
%% leads to the fact that when we need to get all of them we have to know
%% the real list of stats being requested. It can be achieved by various
%% means. I chose to just hardcode it (should be fine as it's for backward
%% compat only).
default_stat_list("@system") ->
    [swap_used, swap_total, rest_requests, odp_report_failed, mem_used_sys,
     mem_total, mem_limit, mem_free, mem_actual_used, mem_actual_free,
     hibernated_waked, hibernated_requests, cpu_utilization_rate,
     cpu_user_rate, cpu_sys_rate, cpu_stolen_rate, cpu_irq_rate,
     cpu_cores_available, allocstall];
default_stat_list("@query") ->
    [query_active_requests, query_queued_requests, query_errors,
     query_invalid_requests, query_request_time, query_requests,
     query_requests_500ms, query_requests_250ms, query_requests_1000ms,
     query_requests_5000ms, query_result_count, query_result_size,
     query_selects, query_service_time, query_warnings];
default_stat_list("@fts") ->
    Stats = get_service_gauges(fts),
    [<<"fts_", (bin(S))/binary>> || S <- Stats];
default_stat_list("@fts-" ++ _) ->
    Stats = get_gauges(fts) ++ get_counters(fts),
    [<<"fts/", (bin(S))/binary>> || S <- Stats] ++
    [<<"fts/*/", (bin(S))/binary>> || S <- Stats];
default_stat_list("@index") ->
    Stats = get_service_gauges(index) ++ [ram_percent, remaining_ram],
    [<<"index_", (bin(S))/binary>> || S <- Stats];
default_stat_list("@index-" ++ _) ->
    Stats = get_gauges(index) ++
            get_counters(index) ++
            get_computed(index),
    [<<"index/", (bin(S))/binary>> || S <- Stats] ++
    [<<"index/*/", (bin(S))/binary>> || S <- Stats];
default_stat_list("@cbas") ->
    Stats = get_service_gauges(cbas) ++
            get_service_counters(cbas),
    [<<"cbas_", (bin(S))/binary>> || S <- Stats];
default_stat_list("@cbas-" ++ _) ->
    Stats = get_gauges(cbas) ++ get_counters(cbas),
    [<<"cbas/", (bin(S))/binary>> || S <- Stats] ++
    [<<"cbas/all/", (bin(S))/binary>> || S <- Stats];
default_stat_list("@xdcr-" ++ B) ->
    Bucket = list_to_binary(B),
    Stats = [
        <<"add_docs_written">>, <<"bandwidth_usage">>, <<"changes_left">>,
        <<"data_replicated">>, <<"datapool_failed_gets">>,
        <<"dcp_datach_length">>, <<"dcp_dispatch_time">>,
        <<"deletion_docs_written">>, <<"deletion_failed_cr_source">>,
        <<"deletion_filtered">>, <<"deletion_received_from_dcp">>,
        <<"docs_checked">>, <<"docs_failed_cr_source">>, <<"docs_filtered">>,
        <<"docs_opt_repd">>, <<"docs_processed">>, <<"docs_received_from_dcp">>,
        <<"docs_rep_queue">>, <<"docs_unable_to_filter">>, <<"docs_written">>,
        <<"expiry_docs_written">>, <<"expiry_failed_cr_source">>,
        <<"expiry_filtered">>, <<"expiry_received_from_dcp">>,
        <<"expiry_stripped">>, <<"num_checkpoints">>, <<"num_failedckpts">>,
        <<"rate_doc_checks">>, <<"rate_doc_opt_repd">>,
        <<"rate_received_from_dcp">>, <<"rate_replicated">>,
        <<"resp_wait_time">>, <<"set_docs_written">>,
        <<"set_failed_cr_source">>, <<"set_filtered">>,
        <<"set_received_from_dcp">>, <<"size_rep_queue">>,
        <<"throttle_latency">>, <<"throughput_throttle_latency">>,
        <<"time_committing">>, <<"wtavg_docs_latency">>,
        <<"wtavg_get_latency">>, <<"wtavg_meta_latency">>
    ],
    [<<"replication_changes_left">>, <<"replication_docs_rep_queue">>] ++
    [<<"replications/*/", Bucket/binary, "/*/", S/binary>> || S <- Stats];
default_stat_list("@eventing") ->
    Stats = get_service_gauges(eventing) ++
            get_computed(eventing),
    [<<"eventing/", (bin(S))/binary>> || S <- Stats] ++
    [<<"eventing/*/", (bin(S))/binary>> || S <- Stats];
default_stat_list("@eventing-" ++ _) ->
    [];
default_stat_list(_Bucket) ->
    [?STAT_GAUGES, ?STAT_COUNTERS, ?DCP_STAT_GAUGES, ?DCP_STAT_COUNTERS,
     couch_docs_actual_disk_size, couch_views_actual_disk_size,
     couch_spatial_data_size, couch_spatial_disk_size, couch_spatial_ops,
     couch_views_data_size, couch_views_disk_size, couch_views_ops,
     bg_wait_count, bg_wait_total, disk_commit_count, disk_commit_total,
     disk_update_count, disk_update_total, couch_docs_disk_size,
     couch_docs_data_size, disk_write_queue, ep_ops_create, ep_ops_update,
     misses, evictions, ops, vb_total_queue_age, vb_total_queue_size, xdc_ops,
     <<"spatial/*/accesses">>, <<"spatial/*/data_size">>,
     <<"spatial/*/disk_size">>, <<"views/*/accesses">>, <<"views/*/data_size">>,
     <<"views/*/disk_size">>].

is_system_stat(<<"cpu_", _/binary>>) -> true;
is_system_stat(<<"swap_", _/binary>>) -> true;
is_system_stat(<<"mem_", _/binary>>) -> true;
is_system_stat(<<"rest_requests">>) -> true;
is_system_stat(<<"hibernated_", _/binary>>) -> true;
is_system_stat(<<"odp_report_failed">>) -> true;
is_system_stat(<<"allocstall">>) -> true;
is_system_stat(_) -> false.

is_sysproc_stat(<<"major_faults">>) -> true;
is_sysproc_stat(<<"minor_faults">>) -> true;
is_sysproc_stat(<<"page_faults">>) -> true;
is_sysproc_stat(<<"mem_", _/binary>>) -> true;
is_sysproc_stat(<<"cpu_utilization">>) -> true;
is_sysproc_stat(<<"minor_faults_raw">>) -> true;
is_sysproc_stat(<<"major_faults_raw">>) -> true;
is_sysproc_stat(<<"page_faults_raw">>) -> true;
is_sysproc_stat(_) -> false.

get_gauges(fts) ->
    [num_mutations_to_index, doc_count, num_recs_to_persist,
     num_bytes_used_disk, num_pindexes_actual, num_pindexes_target,
     num_files_on_disk, num_root_memorysegments, num_root_filesegments];
get_gauges(index) ->
    [disk_size, data_size, memory_used, num_docs_pending, num_docs_queued,
     items_count, frag_percent, recs_in_mem, recs_on_disk, data_size_on_disk,
     log_space_on_disk, raw_data_size];
get_gauges(cbas) ->
    ['incoming_records_count_total', 'failed_at_parser_records_count_total'].

get_counters(fts) ->
    [total_bytes_indexed, total_compaction_written_bytes, total_queries,
     total_queries_slow, total_queries_timeout, total_queries_error,
     total_bytes_query_results, total_term_searchers, total_request_time];
get_counters(index) ->
    [num_requests, num_rows_returned, num_docs_indexed,
     scan_bytes_read, total_scan_duration, cache_misses, cache_hits];
get_counters(cbas) ->
    ['incoming_records_count', 'failed_at_parser_records_count'].

get_service_gauges(fts) ->
    [num_bytes_used_ram, total_queries_rejected_by_herder,
     curr_batches_blocked_by_herder];
get_service_gauges(index) ->
    [memory_quota, memory_used];
get_service_gauges(cbas) ->
    ['heap_used', 'system_load_average', 'thread_count', 'disk_used'];
get_service_gauges(eventing) ->
    [dcp_backlog | eventing_successes() ++ eventing_failures()].

get_service_counters(cbas) ->
    ['gc_count', 'gc_time', 'io_reads', 'io_writes'].

get_computed(index) ->
    [disk_overhead_estimate];
get_computed(eventing) ->
    [processed_count, failed_count].

eventing_successes() ->
    [on_update_success,
     on_delete_success,
     timer_callback_success].

eventing_failures() ->
    [bucket_op_exception_count,
     checkpoint_failure_count,
     n1ql_op_exception_count,
     timeout_count,
     doc_timer_create_failure,
     non_doc_timer_create_failure,
     on_update_failure,
     on_delete_failure,
     timer_callback_failure].

-ifdef(TEST).

pre_70_to_prom_query_test_() ->
    Test = fun (Section, Stats, ExpectedQuery) ->
               Name = lists:flatten(io_lib:format("~s: ~p", [Section, Stats])),
               Opts = #{range_interval => ?IRATE_INTERVAL},
               {Name, ?_assertBinStringsEqual(
                         list_to_binary(ExpectedQuery),
                         hd(pre_70_stats_to_prom_query(Section, undefined,
                                                       Stats, Opts)))}
           end,
    [Test("@system", all,
          "{name=~`sys_allocstall|sys_cpu_cores_available|sys_cpu_irq_rate|"
                  "sys_cpu_stolen_rate|sys_cpu_sys_rate|sys_cpu_user_rate|"
                  "sys_cpu_utilization_rate|sys_mem_actual_free|"
                  "sys_mem_actual_used|sys_mem_free|sys_mem_limit|"
                  "sys_mem_total|sys_mem_used_sys|sys_odp_report_failed|"
                  "sys_swap_total|sys_swap_used`} or "
          "irate({name=~`cm_request_unhibernates_total|"
                        "cm_rest_request_leaves_total`}[1m]) or "
          "label_replace({name=`cm_request_hibernates_total`} - ignoring(name) "
                        "{name=`cm_request_unhibernates_total`},"
                        "`name`,`sys_hibernated_requests`,``,``)"),
     Test("@system", [], ""),
     Test("@system-processes", all,
          "{category=`system-processes`} or "
          "label_replace(irate({name=`sysproc_major_faults_raw`}[1m]),"
                        "`name`,`sysproc_major_faults`,``,``) or "
          "label_replace(irate({name=`sysproc_minor_faults_raw`}[1m]),"
                        "`name`,`sysproc_minor_faults`,``,``) or "
          "label_replace(irate({name=`sysproc_page_faults_raw`}[1m]),"
                        "`name`,`sysproc_page_faults`,``,``)"),
     Test("@system-processes", [], ""),
     Test("@system-processes", [<<"ns_server/cpu_utilization">>,
                                <<"ns_server/mem_resident">>,
                                <<"couchdb/cpu_utilization">>],
          "{name=`sysproc_cpu_utilization`,proc=`couchdb`} or "
          "{name=~`sysproc_cpu_utilization|sysproc_mem_resident`,"
           "proc=`ns_server`}"),
     Test("@query", all,
          "{name=~`n1ql_active_requests|n1ql_queued_requests`} or "
          "irate({name=~`n1ql_errors|n1ql_invalid_requests|n1ql_request_time|"
                        "n1ql_requests|n1ql_requests_1000ms|"
                        "n1ql_requests_250ms|n1ql_requests_5000ms|"
                        "n1ql_requests_500ms|n1ql_result_count|"
                        "n1ql_result_size|n1ql_selects|n1ql_service_time|"
                        "n1ql_warnings`}["?IRATE_INTERVAL"])"),
     Test("@query", [], ""),
     Test("@query", [query_errors, query_active_requests, query_request_time],
          "{name=`n1ql_active_requests`} or "
          "irate({name=~`n1ql_errors|n1ql_request_time`}["?IRATE_INTERVAL"])"),
     Test("@fts", all, "{name=~`fts_curr_batches_blocked_by_herder|"
                               "fts_num_bytes_used_ram|"
                               "fts_total_queries_rejected_by_herder`}"),
     Test("@fts", [], ""),
     Test("@fts", [<<"fts_num_bytes_used_ram">>,
                   <<"fts_curr_batches_blocked_by_herder">>],
          "{name=~`fts_curr_batches_blocked_by_herder|"
                  "fts_num_bytes_used_ram`}"),
     Test("@fts-test", all,
          "{name=~`fts_doc_count|"
                  "fts_num_bytes_used_disk|"
                  "fts_num_files_on_disk|"
                  "fts_num_mutations_to_index|"
                  "fts_num_pindexes_actual|"
                  "fts_num_pindexes_target|"
                  "fts_num_recs_to_persist|"
                  "fts_num_root_filesegments|"
                  "fts_num_root_memorysegments`,bucket=`test`} or "
          "irate({name=~`fts_total_bytes_indexed|"
                        "fts_total_bytes_query_results|"
                        "fts_total_compaction_written_bytes|"
                        "fts_total_queries|"
                        "fts_total_queries_error|"
                        "fts_total_queries_slow|"
                        "fts_total_queries_timeout|"
                        "fts_total_request_time|"
                        "fts_total_term_searchers`,bucket=`test`}[1m]) or "
          "sum by (name) ({name=~`fts_doc_count|"
                                 "fts_num_bytes_used_disk|"
                                 "fts_num_files_on_disk|"
                                 "fts_num_mutations_to_index|"
                                 "fts_num_pindexes_actual|"
                                 "fts_num_pindexes_target|"
                                 "fts_num_recs_to_persist|"
                                 "fts_num_root_filesegments|"
                                 "fts_num_root_memorysegments`,"
                          "bucket=`test`}) or "
          "sum by (name) (irate({name=~`fts_total_bytes_indexed|"
                                       "fts_total_bytes_query_results|"
                                       "fts_total_compaction_written_bytes|"
                                       "fts_total_queries|"
                                       "fts_total_queries_error|"
                                       "fts_total_queries_slow|"
                                       "fts_total_queries_timeout|"
                                       "fts_total_request_time|"
                                       "fts_total_term_searchers`,"
                                "bucket=`test`}[1m]))"
                                      ),
     Test("@fts-test", [], ""),
     Test("@fts-test", [<<"fts/num_files_on_disk">>,
                        <<"fts/num_pindexes_target">>,
                        <<"fts/doc_count">>,
                        <<"fts/ind1/doc_count">>,
                        <<"fts/ind1/num_pindexes_target">>,
                        <<"fts/ind2/num_files_on_disk">>,
                        <<"fts/ind2/total_queries">>],
          "{name=~`fts_doc_count|fts_num_pindexes_target`,"
           "bucket=`test`,index=`ind1`} or "
          "{name=`fts_num_files_on_disk`,bucket=`test`,index=`ind2`} or "
          "irate({name=`fts_total_queries`,bucket=`test`,index=`ind2`}[1m]) or "
          "sum by (name) ({name=~`fts_doc_count|"
                                 "fts_num_files_on_disk|"
                                 "fts_num_pindexes_target`,bucket=`test`})"),
     Test("@index", all,
          "{name=~`index_memory_quota|index_memory_used_total`} or "
          "label_replace(({name=`index_memory_used_total`} / ignoring(name)"
                        " {name=`index_memory_quota`}) * 100,"
                        "`name`,`index_ram_percent`,``,``) or "
          "label_replace({name=`index_memory_quota`} - ignoring(name) "
                        "{name=`index_memory_used_total`},"
                        "`name`,`index_remaining_ram`,``,``)"),
     Test("@index", [], ""),
     Test("@index", [<<"index_memory_quota">>, <<"index_remaining_ram">>],
          "{name=`index_memory_quota`} or "
          "label_replace({name=`index_memory_quota`} - ignoring(name) "
                        "{name=`index_memory_used_total`},"
                        "`name`,`index_remaining_ram`,``,``)"),
     Test("@index-test", all,
          "{name=~`index_data_size|"
                  "index_data_size_on_disk|"
                  "index_disk_size|"
                  "index_frag_percent|"
                  "index_items_count|"
                  "index_log_space_on_disk|"
                  "index_memory_used|"
                  "index_num_docs_pending|"
                  "index_num_docs_queued|"
                  "index_raw_data_size|"
                  "index_recs_in_mem|"
                  "index_recs_on_disk`,bucket=`test`} or "
          "irate({name=~`index_cache_hits|"
                        "index_cache_misses|"
                        "index_num_docs_indexed|"
                        "index_num_requests|"
                        "index_num_rows_returned|"
                        "index_scan_bytes_read|"
                        "index_total_scan_duration`,"
                 "bucket=`test`}[1m]) or "
          "label_replace(({name=`index_disk_size`,bucket=`test`} "
                          "* ignoring(name) "
                          "{name=`index_frag_percent`,bucket=`test`}) / 100,"
                         "`name`,`index_disk_overhead_estimate`,``,``) or "
          "label_replace(sum({name=`index_disk_size`,bucket=`test`} "
                            "* ignoring(name) "
                            "{name=`index_frag_percent`,bucket=`test`}) "
                        "/ 100,`name`,`index_disk_overhead_estimate`,``,``) or "
          "sum by (name) ({name=~`index_data_size|index_data_size_on_disk|"
                                 "index_disk_size|index_frag_percent|"
                                 "index_items_count|index_log_space_on_disk|"
                                 "index_memory_used|index_num_docs_pending|"
                                 "index_num_docs_queued|index_raw_data_size|"
                                 "index_recs_in_mem|index_recs_on_disk`,"
                          "bucket=`test`}) or "
          "sum by (name) (irate({name=~`index_cache_hits|index_cache_misses|"
                                       "index_num_docs_indexed|"
                                       "index_num_requests|"
                                       "index_num_rows_returned|"
                                       "index_scan_bytes_read|"
                                       "index_total_scan_duration`,"
                                "bucket=`test`}[1m]))"),
     Test("@index-test", [], ""),
     Test("@index-test", [<<"index/cache_hits">>,
                          <<"index/i1/num_requests">>,
                          <<"index/i1/disk_overhead_estimate">>],
          "irate({name=`index_num_requests`,"
                 "bucket=`test`,index=`i1`}[1m]) or "
          "label_replace(({name=`index_disk_size`,bucket=`test`,index=`i1`} "
                         "* ignoring(name) "
                         "{name=`index_frag_percent`,bucket=`test`,index=`i1`})"
                         " / 100,"
                         "`name`,`index_disk_overhead_estimate`,``,``) or "
          "sum by (name) (irate({name=`index_cache_hits`,"
                                 "bucket=`test`}[1m]))"),
     Test("@cbas", all, "{name=~`cbas_disk_used_bytes_total|"
                                "cbas_heap_memory_used_bytes|"
                                "cbas_system_load_average|"
                                "cbas_thread_count`} or "
                        "irate({name=~`cbas_gc_count_total|"
                                      "cbas_gc_time_milliseconds_total|"
                                      "cbas_io_reads_total|"
                                      "cbas_io_writes_total`}[1m])"),
     Test("@cbas", [], ""),
     Test("@cbas", [<<"cbas_disk_used">>, <<"cbas_gc_count">>],
          "{name=`cbas_disk_used_bytes_total`} or "
          "irate({name=`cbas_gc_count_total`}[1m])"),
     Test("@cbas-test", all,
          "label_replace(sum({name=`cbas_failed_to_parse_records_count`,"
                             "bucket=`test`,link=`Local`}),"
                        "`name`,`cbas_all_failed_at_parser_records_count_total`"
                        ",``,``) or "
          "label_replace(sum({name=`cbas_incoming_records_count`,"
                             "bucket=`test`,link=`Local`}),"
                        "`name`,`cbas_all_incoming_records_count_total`,"
                        "``,``) or "
          "label_replace(sum({name=`cbas_failed_to_parse_records_count`,"
                             "bucket=`test`,link=`Local`}),"
                        "`name`,`cbas_failed_at_parser_records_count_total`,"
                        "``,``) or "
          "label_replace(sum({name=`cbas_incoming_records_count`,"
                             "bucket=`test`,link=`Local`}),"
                        "`name`,`cbas_incoming_records_count_total`,``,``) or "
          "label_replace(sum(irate({name=`cbas_failed_to_parse_records_"
                                   "count`,"
                                   "bucket=`test`,link=`Local`}[1m])),"
                        "`name`,`cbas_all_failed_at_parser_records_count`,"
                        "``,``) or "
          "label_replace(sum(irate({name=`cbas_incoming_records_count`,"
                                   "bucket=`test`,link=`Local`}[1m])),"
                        "`name`,`cbas_all_incoming_records_count`,``,``) or "
          "label_replace(sum(irate({name=`cbas_failed_to_parse_records_count`,"
                                   "bucket=`test`,link=`Local`}[1m])),"
                        "`name`,`cbas_failed_at_parser_records_count`,"
                        "``,``) or "
          "label_replace(sum(irate({name=`cbas_incoming_records_count`,"
                                    "bucket=`test`,link=`Local`}[1m])),"
                        "`name`,`cbas_incoming_records_count`,``,``)"),
     Test("@cbas-test", [], ""),
     Test("@xdcr-test", all,
          "{name=~`xdcr_add_docs_written_total|xdcr_changes_left_total|"
                  "xdcr_data_replicated_bytes|xdcr_datapool_failed_gets_total|"
                  "xdcr_dcp_datach_length_total|"
                  "xdcr_deletion_docs_written_total|"
                  "xdcr_deletion_failed_cr_source_total|"
                  "xdcr_deletion_filtered_total|"
                  "xdcr_deletion_received_from_dcp_total|"
                  "xdcr_docs_checked_total|xdcr_docs_failed_cr_source_total|"
                  "xdcr_docs_filtered_total|xdcr_docs_opt_repd_total|"
                  "xdcr_docs_processed_total|xdcr_docs_received_from_dcp_total|"
                  "xdcr_docs_rep_queue_total|xdcr_docs_unable_to_filter_total|"
                  "xdcr_docs_written_total|xdcr_expiry_docs_written_total|"
                  "xdcr_expiry_failed_cr_source_total|"
                  "xdcr_expiry_filtered_total|"
                  "xdcr_expiry_received_from_dcp_total|"
                  "xdcr_expiry_stripped_total|xdcr_num_checkpoints_total|"
                  "xdcr_num_failedckpts_total|xdcr_set_docs_written_total|"
                  "xdcr_set_failed_cr_source_total|xdcr_set_filtered_total|"
                  "xdcr_set_received_from_dcp_total|xdcr_size_rep_queue_bytes`,"
           "sourceBucketName=`test`} or "
          "({name=~`xdcr_resp_wait_time_seconds|xdcr_throttle_latency_seconds|"
                   "xdcr_throughput_throttle_latency_seconds|"
                   "xdcr_time_committing_seconds|"
                   "xdcr_wtavg_docs_latency_seconds|"
                   "xdcr_wtavg_get_latency_seconds|"
                   "xdcr_wtavg_meta_latency_seconds`,"
            "sourceBucketName=`test`} * 1000) or "
          "({name=`xdcr_dcp_dispatch_time_seconds`,"
            "sourceBucketName=`test`} * 1000000000) or "
          "label_replace(idelta({name=`xdcr_docs_checked_total`,"
                                "sourceBucketName=`test`}[1m]),`name`,"
                        "`xdcr_rate_doc_checks_docs_per_second`,``,``) or "
          "label_replace(irate({name=`xdcr_data_replicated_bytes`,"
                               "sourceBucketName=`test`}[1m]),`name`,"
                        "`xdcr_bandwidth_usage_bytes_per_second`,``,``) or "
          "label_replace(irate({name=`xdcr_docs_opt_repd_total`,"
                               "sourceBucketName=`test`}[1m]),`name`,"
                        "`xdcr_rate_doc_opt_repd_docs_per_second`,``,``) or "
          "label_replace(irate({name=`xdcr_docs_received_from_dcp_total`,"
                               "sourceBucketName=`test`}[1m]),`name`,"
                        "`xdcr_rate_received_from_dcp_docs_per_second`,``,``)"
          " or "
          "label_replace(irate({name=`xdcr_docs_written_total`,"
                               "sourceBucketName=`test`}[1m]),`name`,"
                        "`xdcr_rate_replicated_docs_per_second`,``,``) or "
          "sum by (name) ({name=~`xdcr_changes_left_total|"
                                 "xdcr_docs_rep_queue_total`,"
                          "sourceBucketName=`test`})"),
     Test("@xdcr-test", [], ""),
     Test("@xdcr-test",
          [<<"replications/id1/test/test2/changes_left">>,
           <<"replications/backfill_id1/test/test2/changes_left">>,
           <<"replications/id1/test/test2/docs_processed">>,
           <<"replications/backfill_id1/test/test2/docs_processed">>,
           <<"replications/id1/test/test2/bandwidth_usage">>,
           <<"replications/backfill_id1/test/test2/bandwidth_usage">>,
           <<"replications/id1/test/test2/time_committing">>,
           <<"replications/backfill_id1/test/test2/time_committing">>],
          "{name=~`xdcr_changes_left_total|xdcr_docs_processed_total`,"
           "sourceBucketName=`test`,pipelineType=`Backfill`,"
           "targetClusterUUID=`id1`,targetBucketName=`test2`} or "
          "{name=~`xdcr_changes_left_total|xdcr_docs_processed_total`,"
           "sourceBucketName=`test`,pipelineType=`Main`,"
           "targetClusterUUID=`id1`,targetBucketName=`test2`} or "
          "({name=`xdcr_time_committing_seconds`,sourceBucketName=`test`,"
            "pipelineType=`Backfill`,targetClusterUUID=`id1`,"
            "targetBucketName=`test2`} * 1000) or "
          "({name=`xdcr_time_committing_seconds`,sourceBucketName=`test`,"
            "pipelineType=`Main`,targetClusterUUID=`id1`,"
            "targetBucketName=`test2`} * 1000) or "
          "label_replace(irate({name=`xdcr_data_replicated_bytes`,"
                               "sourceBucketName=`test`,"
                               "pipelineType=`Backfill`,"
                               "targetClusterUUID=`id1`,"
                               "targetBucketName=`test2`}[1m]),`name`,"
                        "`xdcr_bandwidth_usage_bytes_per_second`,``,``) or "
          "label_replace(irate({name=`xdcr_data_replicated_bytes`,"
                               "sourceBucketName=`test`,"
                               "pipelineType=`Main`,"
                               "targetClusterUUID=`id1`,"
                               "targetBucketName=`test2`}[1m]),`name`,"
                        "`xdcr_bandwidth_usage_bytes_per_second`,``,``)"),
     Test("@eventing", [], ""),
     Test("@eventing", all,
          "label_replace(sum({name=~`eventing_on_delete_success|"
                                    "eventing_on_update_success|"
                                    "eventing_timer_callback_success`}),"
                        "`name`,`eventing_processed_count`,``,``) or "
          "label_replace(sum("
                          "{name=~`eventing_bucket_op_exception_count|"
                                  "eventing_checkpoint_failure_count|"
                                  "eventing_doc_timer_create_failure|"
                                  "eventing_n1ql_op_exception_count|"
                                  "eventing_non_doc_timer_create_failure|"
                                  "eventing_on_delete_failure|"
                                  "eventing_on_update_failure|"
                                  "eventing_timeout_count|"
                                  "eventing_timer_callback_failure`}),"
                         "`name`,`eventing_failed_count`,``,``) or "
          "label_replace(sum by (functionName) ("
                          "{name=~`eventing_on_delete_success|"
                                  "eventing_on_update_success|"
                                  "eventing_timer_callback_success`}),"
                        "`name`,`eventing_processed_count`,``,``) or "
          "label_replace(sum by (functionName) ("
                          "{name=~`eventing_bucket_op_exception_count|"
                                  "eventing_checkpoint_failure_count|"
                                  "eventing_doc_timer_create_failure|"
                                  "eventing_n1ql_op_exception_count|"
                                  "eventing_non_doc_timer_create_failure|"
                                  "eventing_on_delete_failure|"
                                  "eventing_on_update_failure|"
                                  "eventing_timeout_count|"
                                  "eventing_timer_callback_failure`}),"
                        "`name`,`eventing_failed_count`,``,``) or "
          "sum by (name) ({name=~`eventing_bucket_op_exception_count|"
                                 "eventing_checkpoint_failure_count|"
                                 "eventing_dcp_backlog|"
                                 "eventing_doc_timer_create_failure|"
                                 "eventing_n1ql_op_exception_count|"
                                 "eventing_non_doc_timer_create_failure|"
                                 "eventing_on_delete_failure|"
                                 "eventing_on_delete_success|"
                                 "eventing_on_update_failure|"
                                 "eventing_on_update_success|"
                                 "eventing_timeout_count|"
                                 "eventing_timer_callback_failure|"
                                 "eventing_timer_callback_success`}) or "
          "sum by (name,functionName) ("
            "{name=~`eventing_bucket_op_exception_count|"
                    "eventing_checkpoint_failure_count|"
                    "eventing_dcp_backlog|"
                    "eventing_doc_timer_create_failure|"
                    "eventing_n1ql_op_exception_count|"
                    "eventing_non_doc_timer_create_failure|"
                    "eventing_on_delete_failure|"
                    "eventing_on_delete_success|"
                    "eventing_on_update_failure|"
                    "eventing_on_update_success|"
                    "eventing_timeout_count|"
                    "eventing_timer_callback_failure|"
                    "eventing_timer_callback_success`})"),
     Test("@eventing", [<<"eventing/test/failed_count">>,
                        <<"eventing/test/processed_count">>,
                        <<"eventing/bucket_op_exception_count">>,
                        <<"eventing/test/bucket_op_exception_count">>],
          "label_replace(sum by (functionName) ("
                          "{name=~`eventing_on_delete_success|"
                                  "eventing_on_update_success|"
                                  "eventing_timer_callback_success`,"
                           "functionName=`test`}),"
                        "`name`,`eventing_processed_count`,``,``) or "
          "label_replace(sum by (functionName) ("
                          "{name=~`eventing_bucket_op_exception_count|"
                                  "eventing_checkpoint_failure_count|"
                                  "eventing_doc_timer_create_failure|"
                                  "eventing_n1ql_op_exception_count|"
                                  "eventing_non_doc_timer_create_failure|"
                                  "eventing_on_delete_failure|"
                                  "eventing_on_update_failure|"
                                  "eventing_timeout_count|"
                                  "eventing_timer_callback_failure`,"
                           "functionName=`test`}),"
                        "`name`,`eventing_failed_count`,``,``) or "
          "sum by (name) ({name=`eventing_bucket_op_exception_count`}) or "
          "sum by (name,functionName) ("
            "{name=`eventing_bucket_op_exception_count`,functionName=`test`})"),
     Test("@eventing-test", [], ""),
     Test("@eventing-test", all, "")].

prom_name_to_pre_70_name_test_() ->
    Test = fun (Section, Json, ExpectedRes) ->
               Name = lists:flatten(io_lib:format("~s: ~s", [Section, Json])),
               Props = ejson:decode(Json),
               {Name,
                fun () ->
                    ?assertEqual(prom_name_to_pre_70_name(Section, Props),
                                 ExpectedRes)
                end}
           end,
    [Test("@system", "{\"name\": \"sys_cpu_user_rate\"}",
          {ok, cpu_user_rate}),
     Test("@system-processes",
          "{\"name\": \"sysproc_cpu_utilization\",\"proc\": \"ns_server\"}",
          {ok, <<"ns_server/cpu_utilization">>}),
     Test("@query", "{\"name\": \"n1ql_active_requests\"}",
          {ok, query_active_requests}),
     Test("@query", "{}",
          {error, not_found}),
     Test("@query", "{\"name\": \"unknown\"}",
          {error, not_found}),
     Test("@query", "{\"proc\": \"ns_server\"}",
          {error, not_found}),
     Test("@fts", "{\"name\": \"fts_num_bytes_used_ram\"}",
          {ok, <<"fts_num_bytes_used_ram">>}),
     Test("@fts-test", "{\"name\": \"fts_doc_count\"}",
          {ok, <<"fts/doc_count">>}),
     Test("@fts-test", "{\"name\": \"fts_doc_count\", \"index\": \"ind1\"}",
          {ok, <<"fts/ind1/doc_count">>}),
     Test("@index", "{\"name\": \"index_memory_used_total\"}",
          {ok, <<"index_memory_used">>}),
     Test("@index", "{\"name\": \"index_remaining_ram\"}",
          {ok, <<"index_remaining_ram">>}),
     Test("@index-test", "{\"name\": \"index_disk_size\"}",
          {ok, <<"index/disk_size">>}),
     Test("@index-test", "{\"name\": \"index_disk_size\", \"index\": \"ind1\"}",
          {ok, <<"index/ind1/disk_size">>}),
     Test("@cbas", "{\"name\": \"cbas_gc_time_milliseconds_total\"}",
          {ok, <<"cbas_gc_time">>}),
     Test("@cbas-test",
          "{\"name\": \"cbas_failed_at_parser_records_count_total\"}",
          {ok, <<"cbas/failed_at_parser_records_count_total">>}),
     Test("@cbas-test",
          "{\"name\": \"cbas_all_failed_at_parser_records_count_total\"}",
          {ok, <<"cbas/all/failed_at_parser_records_count_total">>}),
     Test("@xdcr-test",
          "{\"name\": \"xdcr_docs_processed_total\","
           "\"sourceBucketName\": \"b1\","
           "\"pipelineType\": \"Backfill\","
           "\"targetClusterUUID\": \"id1\","
           "\"targetBucketName\":\"b2\"}",
          {ok, <<"replications/backfill_id1/b1/b2/docs_processed">>}),
     Test("@xdcr-test",
          "{\"name\": \"xdcr_bandwidth_usage_bytes_per_second\","
           "\"sourceBucketName\": \"b1\","
           "\"pipelineType\": \"Main\","
           "\"targetClusterUUID\": \"id1\","
           "\"targetBucketName\":\"b2\"}",
          {ok, <<"replications/id1/b1/b2/bandwidth_usage">>}),
     Test("@xdcr-test",
          "{\"name\": \"xdcr_changes_left_total\"}",
          {ok, <<"replication_changes_left">>}),
     Test("@eventing",
          "{\"name\": \"eventing_bucket_op_exception_count\"}",
          {ok, <<"eventing/bucket_op_exception_count">>}),
     Test("@eventing",
          "{\"name\": \"eventing_bucket_op_exception_count\","
           "\"functionName\": \"test\"}",
          {ok, <<"eventing/test/bucket_op_exception_count">>})].

-endif.
