%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(stats_collector).

-include("ns_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(LOG_FREQ, 100).     % Dump every n collections to the log
-define(WIDTH, 30).         % Width of the key part of the formatted logs

%% API
-export([start_link/1]).

-export([format_stats/1, log_stats/3]).

%% callbacks
-export([init/1, handle_info/2, grab_stats/1, process_stats/5]).

-record(state, {bucket,
                logged_ts = 0,
                min_files_size = undefined}).

-define(NEED_DCP_STREAM_STATS_CODE, 1).
-include("ns_stats.hrl").

start_link(Bucket) ->
    base_stats_collector:start_link(?MODULE, Bucket).

init(Bucket) ->
    Self = self(),
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({{node, Node, compaction_daemon}, _}) when node() =:= Node ->
              Self ! config_changed;
          ({buckets, _}) ->
              Self ! config_changed;
          (_) ->
              ok
      end),

    {ok, #state{bucket=Bucket}}.

interesting_timing_key(<<"disk_update_", _/binary>>) -> true;
interesting_timing_key(<<"disk_commit_", _/binary>>) -> true;
interesting_timing_key(<<"bg_wait_", _/binary>>) -> true;
interesting_timing_key(_) -> false.

%% drops irrelevant keys from proplist for performance
prefilter_timings(RawTimings) ->
    [KV || {K, _} = KV <- RawTimings,
           interesting_timing_key(K)].

grab_stats(#state{bucket=Bucket}) ->
    {ok, PlainStats} = ns_memcached:stats(Bucket),
    CouchStats = case (catch ns_couchdb_api:fetch_couch_stats(Bucket)) of
                     {ok, CS} -> CS;
                     Crap ->
                         ?log_info("Failed to fetch couch stats:~n~p", [Crap]),
                         []
                 end,
    DcpStats = case ns_memcached:stats(Bucket, <<"dcpagg :">>) of
                   {ok, ValuesDcp} -> ValuesDcp;
                   {memcached_error, key_enoent, _} -> []
               end,
    Timings = case ns_memcached:stats(Bucket, <<"timings">>) of
                  {ok, ValuesK} ->
                      prefilter_timings(ValuesK);
                  {memcached_error, key_enoent, _} -> []
              end,
    {PlainStats, DcpStats, Timings, CouchStats}.

handle_info(config_changed, State) ->
    {noreply, State#state{min_files_size = undefined}};
handle_info(_Msg, State) -> % Don't crash on delayed responses to calls
    {noreply, State}.

maybe_log_stats(TS, #state{logged_ts = LoggedTS,
                           bucket = Bucket} = State, RawStats) ->
    case TS - LoggedTS >= ?LOG_FREQ * 1000 of
        true ->
            case misc:get_env_default(dont_log_stats, false) of
                false ->
                    log_stats(TS, Bucket, RawStats);
                _ -> ok
            end,
            State#state{logged_ts = TS};
        false ->
            State
    end.

log_stats(TS, Bucket, RawStats) ->
    %% TS is epoch _milli_seconds
    TSMicros = (TS rem 1000) * 1000,
    TSSec0 = TS div 1000,
    TSMega = TSSec0 div 1000000,
    TSSec = TSSec0 rem 1000000,
    ?stats_debug("(at ~p (~p)) Stats for bucket ~p:~n~s",
                 [calendar:now_to_local_time({TSMega, TSSec, TSMicros}),
                  TS,
                  Bucket, format_stats(RawStats)]).

get_min_files_size(Bucket) ->
    Config = ns_config:get(),
    MinFileSize = ns_config:search_node_prop(Config,
                                             compaction_daemon, min_file_size, 131072),
    {ok, BucketConfig} = ns_bucket:get_bucket(Bucket, Config),

    MinFileSize * length(ns_bucket:all_node_vbuckets(BucketConfig)).


process_stats(TS, Stats, undefined, LastTS, State) ->
    process_stats(TS, Stats, {undefined, undefined, undefined}, LastTS, State);
process_stats(TS,
              {PlainStats, DcpStats, Timings, CouchStats},
              {LastPlainCounters, LastDcpCounters, LastTimingsCounters},
              LastTS,
              #state{bucket = Bucket,
                     min_files_size = MinFilesSize0} = State) ->
    MinFilesSize = case MinFilesSize0 of
                       undefined ->
                           get_min_files_size(Bucket);
                       _ ->
                           MinFilesSize0
                   end,
    {PlainValues, PlainCounters} = parse_plain_stats(TS, PlainStats, LastTS,
                                                     LastPlainCounters, MinFilesSize),
    {DcpValues, DcpCounters} = parse_dcpagg_stats(TS, DcpStats, LastTS, LastDcpCounters),
    {TimingValues, TimingsCounters} = parse_timings(TS, Timings, LastTS, LastTimingsCounters),
    %% Don't send event with undefined values
    Stats =
        case lists:member(undefined,
                          [LastPlainCounters, LastDcpCounters, LastTimingsCounters]) of
            false ->
                Values = lists:merge(
                           [PlainValues, DcpValues, TimingValues, CouchStats]),
                [{Bucket, Values}];
            true ->
                []
        end,

    StatsToBeLogged = DcpValues ++ PlainStats,
    NewState = maybe_log_stats(TS, State, StatsToBeLogged),

    {Stats,
     {PlainCounters, DcpCounters, TimingsCounters},
     NewState#state{min_files_size = MinFilesSize}}.

%% Internal functions
format_stats(Stats) ->
    erlang:list_to_binary(
      [case couch_util:to_binary(K0) of
           K -> [K, lists:duplicate(erlang:max(1, ?WIDTH - byte_size(K)), $\s),
                 couch_util:to_binary(V), $\n]
       end || {K0, V} <- lists:sort(Stats)]).

translate_stat(bytes) -> % memcached calls it bytes
    mem_used;
translate_stat(Stat) ->
    Stat.

orddict_fetch(Name, Dict) ->
    element(2, lists:keyfind(Name, 1, Dict)).

sum_stat_values(Dict, [FirstName | RestNames]) ->
    lists:foldl(fun (Name, Acc) ->
                        orddict_fetch(Name, Dict) + Acc
                end, orddict_fetch(FirstName, Dict), RestNames).

extract_agg_dcp_stats(KVs) ->
    lists:foldl(fun ({K, V}, Acc) -> extract_agg_dcp_stat(K, V, Acc) end,
                #dcp_stream_stats{}, KVs).

stats_dict_get(Stat, Stats) ->
    case lists:keyfind(Stat, 1, Stats) of
        false ->
            0;
        {Stat, V} ->
            list_to_integer(binary_to_list(V))
    end.

mk_stats_dict_get(Stats) ->
    fun (Stat) ->
            stats_dict_get(Stat, Stats)
    end.

parse_stats_raw(TS, Stats, LastCounters, LastTS, KnownGauges, KnownCounters) ->
    Dict = lists:sort([{translate_stat(binary_to_atom(K, latin1)), V}
                        || {K, V} <- Stats]),
    diff_stats_counters(TS, LastCounters, LastTS, KnownGauges, KnownCounters,
                        mk_stats_dict_get(Dict)).

diff_stats_counters(TS, LastCounters, LastTS, KnownGauges, KnownCounters, GetValue) ->
    Gauges = [GetValue(K) || K <- KnownGauges],
    Counters = [GetValue(K) || K <- KnownCounters],
    Deltas = case LastCounters of
                 undefined ->
                     lists:duplicate(length(KnownCounters), 0);
                 _ ->
                     Delta = TS - LastTS,
                     if Delta > 0 ->
                             lists:zipwith(
                               fun (A, B) ->
                                       Res = (A - B) * 1000.0/Delta,
                                       if Res < 0 -> 0;
                                          true -> Res
                                       end
                               end, Counters, LastCounters);
                        true ->
                             lists:duplicate(length(KnownCounters),
                                             0)
                     end
             end,
    Values0 = orddict:merge(fun (_, _, _) -> erlang:error(cannot_happen) end,
                            lists:sort(lists:zip(KnownGauges, Gauges)),
                            lists:sort(lists:zip(KnownCounters, Deltas))),
    {Values0, Counters}.

parse_aggregate_dcp_stats(AggDcp) ->
    ReplicaStats = extract_agg_dcp_stats([{K, V} || {<<"replication:", K/binary>>, V} <- AggDcp]),
    XdcrStats = extract_agg_dcp_stats([{K, V} || {<<"xdcr:", K/binary>>, V} <- AggDcp]),
    MapreduceStats = extract_agg_dcp_stats([{K, V} || {<<"mapreduce_view:", K/binary>>, V} <- AggDcp]),
    SpatialStats = extract_agg_dcp_stats([{K, V} || {<<"spatial_view:", K/binary>>, V} <- AggDcp]),
    IndexStats = extract_agg_dcp_stats([{K, V} || {<<"secidx:", K/binary>>, V} <- AggDcp]),
    FtsStats = extract_agg_dcp_stats([{K, V} || {<<"fts:", K/binary>>, V} <- AggDcp]),
    CbasStats = extract_agg_dcp_stats([{K, V} || {<<"cbas:", K/binary>>, V} <- AggDcp]),
    EventingStats = extract_agg_dcp_stats([{K, V} || {<<"eventing:", K/binary>>, V} <- AggDcp]),
    TotalStats = extract_agg_dcp_stats([{K, V} || {<<":total:", K/binary>>, V} <- AggDcp]),

    ViewsStats = add_dcp_stats(MapreduceStats, SpatialStats),

    OtherStats = calc_dcp_other_stats(ReplicaStats, XdcrStats, ViewsStats,
                                      IndexStats, FtsStats, CbasStats,
                                      EventingStats, TotalStats),

    lists:append([dcp_stream_stats_to_kvlist(<<"ep_dcp_replica_">>, ReplicaStats),
                  dcp_stream_stats_to_kvlist(<<"ep_dcp_xdcr_">>, XdcrStats),
                  dcp_stream_stats_to_kvlist(<<"ep_dcp_views_">>, ViewsStats),
                  dcp_stream_stats_to_kvlist(<<"ep_dcp_2i_">>, IndexStats),
                  dcp_stream_stats_to_kvlist(<<"ep_dcp_fts_">>, FtsStats),
                  dcp_stream_stats_to_kvlist(<<"ep_dcp_cbas_">>, CbasStats),
                  dcp_stream_stats_to_kvlist(<<"ep_dcp_eventing_">>, EventingStats),
                  dcp_stream_stats_to_kvlist(<<"ep_dcp_other_">>, OtherStats)]).

maybe_adjust_data_size(DataSize, DiskSize, MinFileSize) ->
    case DiskSize < MinFileSize of
        true ->
            DiskSize;
        false ->
            DataSize
    end.

parse_plain_stats(TS, PlainStats, LastTS, LastPlainCounters, MinFilesSize) ->
    {Values0, Counters} = parse_stats_raw(TS, PlainStats, LastPlainCounters, LastTS,
                                          [?STAT_GAUGES], [?STAT_COUNTERS]),
    DiskSize = stats_dict_get(<<"ep_db_file_size">>, PlainStats),
    AggregateValues = [{ops, sum_stat_values(Values0, [cmd_lookup, cmd_set,
                                                       incr_misses, incr_hits,
                                                       decr_misses, decr_hits,
                                                       delete_misses, delete_hits,
                                                       ep_num_ops_del_meta,
                                                       ep_num_ops_get_meta,
                                                       ep_num_ops_set_meta,
                                                       ep_num_ops_set_ret_meta,
                                                       ep_num_ops_del_ret_meta])},
                       {xdc_ops, sum_stat_values(Values0, [ep_num_ops_del_meta,
                                                           ep_num_ops_get_meta,
                                                           ep_num_ops_set_meta])},
                       {cmd_set, sum_stat_values(Values0, [cmd_set,
                                                           ep_num_ops_set_ret_meta])},
                       {delete_hits, sum_stat_values(Values0, [delete_hits,
                                                               ep_num_ops_del_ret_meta])},
                       {misses, sum_stat_values(Values0, [get_misses, delete_misses,
                                                          incr_misses, decr_misses,
                                                          cas_misses])},
                       {disk_write_queue, sum_stat_values(Values0, [ep_queue_size, ep_flusher_todo])},
                       {ep_ops_create, sum_stat_values(Values0, [vb_active_ops_create,
                                                                 vb_replica_ops_create,
                                                                 vb_pending_ops_create])},
                       {ep_ops_update, sum_stat_values(Values0, [vb_active_ops_update,
                                                                 vb_replica_ops_update,
                                                                 vb_pending_ops_update])},
                       {vb_total_queue_age, sum_stat_values(Values0, [vb_active_queue_age,
                                                                      vb_replica_queue_age,
                                                                      vb_pending_queue_age])},
                       {couch_docs_disk_size, DiskSize},
                       {couch_docs_data_size, maybe_adjust_data_size(
                                                stats_dict_get(<<"ep_db_data_size">>,
                                                               PlainStats),
                                                DiskSize, MinFilesSize)}
                      ],
    Values = orddict:merge(fun (_K, _V1, V2) ->
                                   V2           % prefer aggregated value
                           end,
                           Values0,
                           lists:sort(AggregateValues)),
    {Values, Counters}.

parse_dcpagg_stats(TS, DcpStats, LastTS, LastDcpCounters) ->
    parse_stats_raw(TS, parse_aggregate_dcp_stats(DcpStats),
                    LastDcpCounters, LastTS,
                    [?DCP_STAT_GAUGES], [?DCP_STAT_COUNTERS]).

parse_timing_range(Rest) ->
    {Begin, End} = misc:split_binary_at_char(Rest, $,),
    {list_to_integer(binary_to_list(Begin)),
     list_to_integer(binary_to_list(End))}.


parse_timing_key(Prefix, Key) ->
    case (catch erlang:split_binary(Key, erlang:size(Prefix))) of
        {Prefix, Suffix} ->
            parse_timing_range(Suffix);
        _ ->
            nothing
    end.

aggregate_timings(_Prefix, [], TotalCount, TotalAggregate) ->
    {TotalCount, TotalAggregate};
aggregate_timings(Prefix, [{K, V} | TimingsRest], TotalCount, TotalAggregate) ->
    case parse_timing_key(Prefix, K) of
        {Start, End} ->
            ThisSeeks = list_to_integer(binary_to_list(V)),
            NewSeeks = TotalCount + ThisSeeks,
            NewDistance = TotalAggregate + (End + Start) * 0.5 * ThisSeeks,
            aggregate_timings(Prefix, TimingsRest, NewSeeks, NewDistance);
        nothing ->
            aggregate_timings(Prefix, TimingsRest, TotalCount, TotalAggregate)
    end.

parse_timings(TS, Timings, LastTS, LastTimingCounters) ->
    {BGWaitCount, BGWaitTotal} = aggregate_timings(<<"bg_wait_">>, Timings, 0, 0),
    {DiskCommitCount, DiskCommitTotal} = aggregate_timings(<<"disk_commit_">>, Timings, 0, 0),
    {DiskUpdateCount, DiskUpdateTotal} = aggregate_timings(<<"disk_update_">>, Timings, 0, 0),
    diff_stats_counters(TS, LastTimingCounters, LastTS,
                        [], [bg_wait_count, bg_wait_total,
                             disk_commit_count, disk_commit_total,
                             disk_update_count, disk_update_total],
                        fun (bg_wait_count) -> BGWaitCount;
                            (bg_wait_total) -> BGWaitTotal;
                            (disk_commit_count) -> DiskCommitCount;
                            (disk_commit_total) -> DiskCommitTotal;
                            (disk_update_count) -> DiskUpdateCount;
                            (disk_update_total) -> DiskUpdateTotal
                        end).


-ifdef(TEST).
parse_stats_test() ->
    Now = erlang:monotonic_time(millisecond),
    Input =
        [{<<"conn_yields">>,<<"0">>},
         {<<"threads">>,<<"4">>},
         {<<"rejected_conns">>,<<"0">>},
         {<<"limit_maxbytes">>,<<"67108864">>},
         {<<"bytes_written">>,<<"823281">>},
         {<<"bytes_read">>,<<"37610">>},
         {<<"cas_badval">>,<<"0">>},
         {<<"cas_hits">>,<<"0">>},
         {<<"cas_misses">>,<<"0">>},
         {<<"decr_hits">>,<<"0">>},
         {<<"decr_misses">>,<<"0">>},
         {<<"incr_hits">>,<<"0">>},
         {<<"incr_misses">>,<<"0">>},
         {<<"delete_hits">>,<<"0">>},
         {<<"delete_misses">>,<<"0">>},
         {<<"get_misses">>,<<"0">>},
         {<<"get_hits">>,<<"0">>},
         {<<"auth_errors">>,<<"0">>},
         {<<"auth_cmds">>,<<"0">>},
         {<<"cmd_flush">>,<<"0">>},
         {<<"cmd_set">>,<<"0">>},
         {<<"cmd_get">>,<<"0">>},
         {<<"cmd_lookup">>,<<"0">>},
         {<<"connection_structures">>,<<"11">>},
         {<<"total_connections">>,<<"11">>},
         {<<"curr_connections">>,<<"11">>},
         {<<"daemon_connections">>,<<"10">>},
         {<<"rusage_system">>,<<"0.067674">>},
         {<<"rusage_user">>,<<"0.149256">>},
         {<<"pointer_size">>,<<"64">>},
         {<<"libevent">>,<<"1.4.14b-stable">>},
         {<<"version">>,<<"1.4.4_209_g7b9e75f">>},
         {<<"time">>,<<"1285969960">>},
         {<<"uptime">>,<<"103">>},
         {<<"pid">>,<<"56368">>},
         {<<"bucket_conns">>,<<"1">>},
         {<<"ep_num_non_resident">>,<<"0">>},
         {<<"ep_pending_ops_max_duration">>,<<"0">>},
         {<<"ep_pending_ops_max">>,<<"0">>},
         {<<"ep_pending_ops_total">>,<<"0">>},
         {<<"ep_pending_ops">>,<<"0">>},
         {<<"ep_warmup">>,<<"true">>},
         {<<"ep_dbinit">>,<<"0">>},
         {<<"ep_dbname">>,<<"./data/n_0/default">>},
         {<<"ep_warmup_time">>,<<"0">>},
         {<<"ep_warmup_oom">>,<<"0">>},
         {<<"ep_warmup_dups">>,<<"0">>},
         {<<"ep_warmed_up">>,<<"0">>},
         {<<"ep_warmup_thread">>,<<"complete">>},
         {<<"ep_num_not_my_vbuckets">>,<<"0">>},
         {<<"ep_num_eject_failures">>,<<"0">>},
         {<<"ep_num_value_ejects">>,<<"5">>},
         {<<"ep_num_expiry_pager_runs">>,<<"0">>},
         {<<"ep_num_pager_runs">>,<<"0">>},
         {<<"ep_bg_fetched">>,<<"0">>},
         {<<"ep_storage_type">>,<<"featured">>},
         {<<"ep_tmp_oom_errors">>,<<"0">>},
         {<<"ep_oom_errors">>,<<"0">>},
         {<<"ep_total_cache_size">>,<<"0">>},
         {<<"ep_mem_high_wat">>,<<"2394685440">>},
         {<<"ep_mem_low_wat">>,<<"1915748352">>},
         {<<"ep_max_size">>,<<"3192913920">>},
         {<<"ep_overhead">>,<<"25937168">>},
         {<<"ep_kv_size">>,<<"0">>},
         {<<"mem_used">>,<<"25937168">>},
         {<<"curr_items_tot">>,<<"0">>},
         {<<"curr_items">>,<<"0">>},
         {<<"ep_flush_duration_highwat">>,<<"0">>},
         {<<"ep_flush_duration_total">>,<<"0">>},
         {<<"ep_flush_duration">>,<<"0">>},
         {<<"ep_flush_preempts">>,<<"0">>},
         {<<"ep_vbucket_del_fail">>,<<"0">>},
         {<<"ep_vbucket_del">>,<<"0">>},
         {<<"ep_commit_time_total">>,<<"0">>},
         {<<"ep_commit_time">>,<<"0">>},
         {<<"ep_commit_num">>,<<"0">>},
         {<<"ep_flusher_state">>,<<"running">>},
         {<<"ep_flusher_todo">>,<<"0">>},
         {<<"ep_queue_size">>,<<"0">>},
         {<<"ep_item_flush_expired">>,<<"0">>},
         {<<"ep_expired">>,<<"0">>},
         {<<"ep_item_commit_failed">>,<<"0">>},
         {<<"ep_item_flush_failed">>,<<"0">>},
         {<<"ep_total_persisted">>,<<"0">>},
         {<<"ep_total_del_items">>,<<"0">>},
         {<<"ep_total_new_items">>,<<"0">>},
         {<<"ep_total_enqueued">>,<<"0">>},
         {<<"ep_too_old">>,<<"0">>},
         {<<"ep_too_young">>,<<"0">>},
         {<<"ep_data_age_highwat">>,<<"0">>},
         {<<"ep_data_age">>,<<"0">>},
         {<<"ep_queue_age_cap">>,<<"900">>},
         {<<"ep_min_data_age">>,<<"0">>},
         {<<"ep_storage_age_highwat">>,<<"0">>},
         {<<"ep_storage_age">>,<<"0">>},
         {<<"ep_num_not_my_vbuckets">>,<<"0">>},
         {<<"ep_oom_errors">>,<<"0">>},
         {<<"ep_tmp_oom_errors">>,<<"0">>},
         {<<"ep_version">>,<<"1.6.0beta3a_166_g24a1637">>}],

    TestGauges = [curr_connections,
                  curr_items,
                  ep_flusher_todo,
                  ep_queue_size,
                  mem_used],

    TestCounters = [bytes_read,
                    bytes_written,
                    cas_badval,
                    cas_hits,
                    cas_misses,
                    cmd_get,
                    cmd_lookup,
                    cmd_set,
                    decr_hits,
                    decr_misses,
                    delete_hits,
                    delete_misses,
                    get_hits,
                    get_misses,
                    incr_hits,
                    incr_misses,
                    ep_total_persisted,
                    ep_num_value_ejects,
                    ep_num_not_my_vbuckets,
                    ep_oom_errors,
                    ep_tmp_oom_errors],

    ExpectedPropList = [{bytes_read,37610},
                        {bytes_written,823281},
                        {cas_badval,0},
                        {cas_hits,0},
                        {cas_misses,0},
                        {cmd_get,0},
                        {cmd_lookup,0},
                        {cmd_set,0},
                        {curr_connections,11},
                        {curr_items,0},
                        {decr_hits,0},
                        {decr_misses,0},
                        {delete_hits,0},
                        {delete_misses,0},
                        {ep_flusher_todo,0},
                        {ep_num_not_my_vbuckets,0},
                        {ep_oom_errors,0},
                        {ep_queue_size,0},
                        {ep_tmp_oom_errors,0},
                        {ep_total_persisted,0},
                        {ep_num_value_ejects,5},
                        {get_hits,0},
                        {get_misses,0},
                        {incr_hits,0},
                        {incr_misses,0},
                        {mem_used,25937168}],

    {ActualValues,
     ActualCounters} = parse_stats_raw(
                         Now, Input,
                         lists:duplicate(length(TestCounters), 0),
                         Now - 1000,
                         TestGauges, TestCounters),

    ?assertEqual([37610,823281,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0,0,0],
                 ActualCounters),

    E = lists:keysort(1, ExpectedPropList),
    A = lists:keysort(1, ActualValues),
    ?log_debug("Expected: ~p~nActual:~p", [E, A]),
    true = (E == A).
-endif.
