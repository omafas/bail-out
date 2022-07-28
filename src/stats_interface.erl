%% @author Couchbase <info@couchbase.com>
%% @copyright 2020-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
-module(stats_interface).

-include("ns_common.hrl").

-export([system/0,
         sysproc/0,
         buckets_interesting/0,
         buckets_interesting/1,
         for_alerts/0,
         current_items_total/1,
         current_items_total/2,
         failover_safeness_level/1,
         latest/2]).

-define(DEFAULT_TIMEOUT, 5000).
-define(IRATE_INTERVAL, "1m").

current_items_total(Bucket) when is_list(Bucket) ->
    current_items_total(list_to_binary(Bucket));
current_items_total(BucketBin) when is_binary(BucketBin) ->
    case latest(<<"kv_curr_items_tot{bucket=`", BucketBin/binary, "`}">>,
                fun (_Props) -> {true, curr_items_total} end) of
        [{curr_items_total, N}] -> N;
        _ -> undefined
    end.

current_items_total(Bucket, Node) ->
    Res = from_nodes([Node], current_items_total, [Bucket], ?DEFAULT_TIMEOUT),
    proplists:get_value(Node, Res).

system() ->
    latest(<<"{category=`system`,"
              "name=~`sys_cpu_utilization_rate|"
                     "sys_cpu_stolen_rate|sys_swap_total|sys_swap_used|"
                     "sys_mem_total|sys_mem_free|sys_mem_limit|"
                     "sys_cpu_cores_available|sys_allocstall`}">>,
           fun (Props) ->
               <<"sys_", N/binary>> = proplists:get_value(<<"name">>, Props),
               {true, binary_to_atom(N, latin1)}
           end).

sysproc() ->
    Res = latest(
            <<"{category=`system-processes`,"
               "name=~`sysproc_mem_resident|sysproc_mem_size|"
                      "sysproc_cpu_utilization|sysproc_major_faults_raw`}">>,
            fun (Props) ->
                <<"sysproc_", N/binary>> = proplists:get_value(<<"name">>,
                                                               Props),
                P = proplists:get_value(<<"proc">>, Props),
                {true, {binary_to_atom(P, latin1), binary_to_atom(N, latin1)}}
            end),
    misc:groupby_map(fun ({{Proc, Name}, Value}) ->
                         {Proc, {Name, Value}}
                     end, Res).

buckets_interesting(Nodes) ->
    from_nodes(Nodes, buckets_interesting, [], ?DEFAULT_TIMEOUT).

buckets_interesting() ->
    Q = <<"{name=~`kv_curr_items|"
                  "kv_curr_items_tot|"
                  "kv_mem_used_bytes|"
                  "couch_docs_actual_disk_size|"
                  "couch_views_actual_disk_size|"
                  "kv_ep_db_data_size_bytes|"
                  "kv_ep_bg_fetched`} or "
          "kv_vb_curr_items{state='replica'} or "
          "kv_vb_num_non_resident{state='active'} or "
          "label_replace(sum by (bucket, name) ("
                          "irate(kv_ops{op=`get`}["?IRATE_INTERVAL"])), `name`,"
                          "`cmd_get`, ``, ``) or "
          "label_replace(irate(kv_ops{op=`get`,result=`hit`}"
                              "["?IRATE_INTERVAL"]),"
                        "`name`,`get_hits`,``,``) or "
          "label_replace("
            "sum by (bucket) ("
              "irate(kv_cmd_lookup["?IRATE_INTERVAL"]) or "
              "irate(kv_ops{op=~`set|incr|decr|delete|del_meta|"
                                "get_meta|set_meta|set_ret_meta|"
                                "del_ret_meta`}["?IRATE_INTERVAL"])), "
            "`name`, `ops`, ``, ``) or "
          "sum by (bucket, name) ({name=~`index_data_size|index_disk_size|"
                                         "couch_spatial_data_size|"
                                         "couch_spatial_disk_size|"
                                         "couch_views_data_size`})">>,
    Res = latest(Q, fun (Props) ->
                        N = proplists:get_value(<<"name">>, Props),
                        B = proplists:get_value(<<"bucket">>, Props),
                        {true, {binary_to_list(B), binary_to_atom(N, latin1)}}
                    end),
    BucketStats = interesting_stats_backward_compat_mapping(
                    misc:groupby_map(fun ({{Bucket, Name}, Value}) ->
                                         {Bucket, {Name, Value}}
                                     end, Res)),

    BucketInterestingStats =
        [curr_items, curr_items_tot, vb_replica_curr_items, mem_used,
         couch_docs_actual_disk_size, couch_views_actual_disk_size,
         couch_spatial_disk_size, couch_docs_data_size, couch_views_data_size,
         couch_spatial_data_size, vb_active_num_non_resident, cmd_get, get_hits,
         ep_bg_fetched, ops],

    BucketNames = ns_bucket:get_bucket_names(),
    [{Bucket, zero_not_existing_stats(Stats, BucketInterestingStats)}
     || {Bucket, Stats} <- BucketStats, lists:member(Bucket, BucketNames)].

from_nodes(Nodes, Function, Args, Timeout) ->
    {GoodRes, BadRes} = misc:multi_call(Nodes, ns_server_stats,
                                        {stats_interface, Function, Args},
                                        Timeout),
    case BadRes of
        [] -> ok;
        _ ->
            ?log_error("Failed to get ~p~p stats from ~p with reason:~n~p",
                       [Function, Args, [N || {N, _} <- BadRes], BadRes])
    end,
    GoodRes.

%% Return current metrics values required for alert conditions checks
%%
%% Note that this function also maps real metrics names to metric
%% names expected by alert system. If metrics names in prometheus change,
%% metrics names returned by this functions should stay the same.
-spec for_alerts() -> [{Section, [{MetricName, Value}]}]
            when Section :: string(),
                 MetricName :: atom(),
                 Value :: number().
for_alerts() ->
    Q = <<"{name=~`kv_ep_meta_data_memory_bytes|"
                  "kv_ep_max_size|"
                  "kv_ep_oom_errors|"
                  "kv_ep_item_commit_failed|"
                  "kv_ep_clock_cas_drift_threshold_exceeded`} or "
          "label_replace(sum(kv_audit_dropped_events),"
                        "`name`, `audit_dropped_events`,``,``) or "
          "label_replace(({name=`index_memory_used_total`} / ignoring(name) "
                         "{name=`index_memory_quota`}) * 100,"
                        "`name`,`index_ram_percent`,``,``) or "
         "{name=~`sys_mem_actual_free|sys_mem_actual_used|"
                 "sys_mem_cgroup_limit|sys_mem_cgroup_used`}">>,

    Res = latest(Q, fun (Props) ->
                        case proplists:get_value(<<"name">>, Props) of
                            <<"audit_", _/binary>> = N ->
                                {true, {"@global", binary_to_atom(N, latin1)}};
                            <<"index_", _/binary>> = N ->
                                {true, {"@index", binary_to_atom(N, latin1)}};
                            <<"kv_", N/binary>> ->
                                B = proplists:get_value(<<"bucket">>, Props),
                                {true, {binary_to_list(B),
                                        binary_to_atom(N, latin1)}};
                            <<"sys_", N/binary>> ->
                                {true, {"@system",
                                        binary_to_atom(N, latin1)}};
                            _ ->
                                false
                        end
                    end),
    misc:groupby_map(fun ({{Bucket, Name}, Value}) ->
                         {Bucket, {Name, Value}}
                     end, Res).

failover_safeness_level(Bucket) ->
    Settings = prometheus_cfg:settings(),
    Interval = prometheus_cfg:derived_metrics_interval(Settings),
    % Requesting range-vector in order to retrieve real timestamps
    % for datapoints, instead of just "now"
    QueryIOL = io_lib:format("cm_failover_safeness_level{bucket=`~s`}[~bs]",
                             [Bucket, Interval * 2]),
    Query = lists:flatten(QueryIOL),
    case prometheus:query(Query, undefined, ?DEFAULT_TIMEOUT, Settings) of
        {ok, [{Props} | _]} ->
            Values = proplists:get_value(<<"values">>, Props),
            [Timestamp, Val] = lists:last(Values),
            case promQL:parse_value(Val) of
                N when is_number(N) -> {ok, {Timestamp, Interval, round(N)}};
                _ -> {error, invalid_value}
            end;
        {ok, []} -> {error, not_available};
        {error, _} -> {error, stats_request_failed}
    end.

latest(Query, NameParser) ->
    latest(Query, NameParser, ?DEFAULT_TIMEOUT).
latest(Query, NameParser, Timeout) ->
    Settings = prometheus_cfg:settings(),
    case prometheus:query(Query, undefined, Timeout, Settings) of
        {ok, JsonArray} ->
            lists:filtermap(
              fun ({Props}) ->
                  {MetricProps} = proplists:get_value(<<"metric">>, Props),
                  [_, Val] = proplists:get_value(<<"value">>, Props),
                  case NameParser(MetricProps) of
                      {true, Name} ->
                          {true, {Name, promQL:parse_value(Val)}};
                      false ->
                          false
                  end
              end, JsonArray);
        {error, _} -> []
    end.

%% Stat names changed in Chesire-Cat
%% There are 2 reasons why we need to convert interesting stats names to
%% old names:
%% 1) Compatibility between nodes during upgrade;
%% 2) Compatibility of GET /pools/nodes/
interesting_stats_backward_compat_mapping(BucketStats) ->
   Map = fun (kv_mem_used_bytes) -> mem_used;
             (kv_curr_items) -> curr_items;
             (kv_curr_items_tot) -> curr_items_tot;
             (kv_vb_curr_items) -> vb_replica_curr_items;
             (kv_ep_db_data_size_bytes) -> couch_docs_data_size;
             (kv_vb_num_non_resident) -> vb_active_num_non_resident;
             (kv_ep_bg_fetched) -> ep_bg_fetched;
             (N) -> N
         end,
    lists:map(
      fun ({Bucket, Stats}) ->
          {Bucket, [{Map(N), V} || {N, V} <- Stats]}
      end, BucketStats).

zero_not_existing_stats(StatsData, StatsNames) ->
    misc:update_proplist([{N, 0} || N <- StatsNames], StatsData).
