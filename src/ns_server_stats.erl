%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2011-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.
%%
%% @doc grabs system-level stats portsigar
%%
-module(ns_server_stats).

-behaviour(gen_server).

-include_lib("stdlib/include/assert.hrl").
-include("ns_common.hrl").
-include("ns_stats.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(ETS_LOG_INTVL, 180).

-define(DEFAULT_HIST_MAX, 10000). %%  10^4, 4 buckets
-define(DEFAULT_HIST_UNIT, millisecond).
-define(METRIC_PREFIX, <<"cm_">>).
-define(POPULATE_STATS_INTERVAL, ?get_timeout(populate_stats_interval, 5000)).
%% For heavyweight stats we populate them infrequently
-define(HEAVYWEIGHT_STATS_SKIP_COUNT, ?get_param(skip_count, 6)).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([init_stats/0, notify_counter/1, notify_counter/2, notify_gauge/2,
         notify_gauge/3, notify_histogram/2, notify_histogram/4, notify_max/2]).

-export([increment_counter/2,
         get_ns_server_stats/0,
         add_histo/2,
         delete_bucket_stats/1,
         stale_histo_epoch_cleaner/0,
         report_derived_stats/2,
         report_prom_stats/3,
         report_prom_stats/4]).

-type os_pid() :: integer().

-type metric() :: atom() | binary() | {atom() | binary(), [label()]}.
-type label() :: {atom() | binary() | iolist(),
                  integer() | float() | atom() | binary() | iolist()}.

-type gauge_value() :: undefined | infinity | neg_infinity | binary()
                       | number() | boolean().

-type units() :: second | millisecond | microsecond.

-record(state, {
          process_stats_timer :: reference() | undefined,
          cleanup_stats_timer :: reference() | undefined,
          populate_stats_timer :: reference() | undefined,
          populate_stats_pid :: pid() | undefined,
          populate_stats_start_time = 0 :: non_neg_integer(),
          populate_stats_ref :: reference() | undefined,
          populate_stats_count = 0 :: non_neg_integer(),
          pid_names :: [{os_pid(), binary()}]
         }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec notify_counter(metric()) -> ok.
notify_counter(Metric) ->
    notify_counter(Metric, 1).

-spec notify_counter(metric(), pos_integer()) -> ok.
notify_counter(Metric, Val) when Val > 0, is_integer(Val) ->
    Key = {c, normalized_metric(Metric)},
    catch ets:update_counter(?MODULE, Key, Val, {Key, 0}),
    ok.

-spec notify_gauge(metric(), gauge_value()) -> ok.
notify_gauge(Metric, Val) ->
    notify_gauge(Metric, Val, #{}).

%% Store a value for a metric that we don't wish to fetch at scrape time.
%% Stale values will be cleaned up after an expiration time, which is by default
%% 3 minutes (the maximum scrape interval that we permit for the prometheus
%% config).
%% The expiration can be set to infinity to avoid the gauge ever getting cleaned
%% up. However, we should only use infinity when updates to the metric could be
%% arbitrarily far apart, and the metric is expected to be valid indefinitely.
%% If the metric is associated with an entity which could disappear (for
%% instance bucket deletion, or potentially in future, the removal of a
%% service), then the gauge should get cleaned up at that point.
-spec notify_gauge(metric(), gauge_value(),
                   #{expiration_s => pos_integer() | infinity}) -> ok.
notify_gauge(Metric, Val, Opts) ->
    Key = {g, normalized_metric(Metric)},
    Expiration = maps:get(expiration_s, Opts,
                          prometheus_cfg:max_scrape_int()),

    ExpiryTime =
        case Expiration of
            infinity -> infinity;
            _ ->  erlang:monotonic_time(millisecond) + Expiration * 1000
        end,
    catch ets:insert(?MODULE, {Key, {ExpiryTime, Val}}),
    ok.

-spec notify_histogram(metric(), integer()) -> ok.
notify_histogram(Metric, Val) ->
    notify_histogram(Metric, ?DEFAULT_HIST_MAX, ?DEFAULT_HIST_UNIT, Val).

-spec notify_histogram(metric(), pos_integer(), units(), integer()) -> ok.
notify_histogram(Metric, Max, Units, Val) when Max > 0, Val >= 0,
                                               is_integer(Val) ->
    BucketN = get_histogram_bucket(Val, Max),
    Key = {h, normalized_metric(Metric), Max, Units},
            %% Update sum | Update counter for a particular bucket
    Updates = [{2, Val},    {BucketN + 4, 1}],
    try ets:update_counter(?MODULE, Key, Updates) of
        _ -> ok
    catch
        error:badarg -> %% missing stat or no ets table
            N = get_histogram_bucket(Max, Max) + 1,
            %%                      Sum | Inf | Other Buckets
            V = list_to_tuple([Key,   0,    0 | lists:duplicate(N, 0)]),
            %% verify units
            ?assert(is_binary(to_seconds_bin(1, Units))),
            catch ets:insert_new(?MODULE, V),
            catch ets:update_counter(?MODULE, Key, Updates),
            ok
    end;
%% Ignoring negative values. It might happen in case of time change
%% when os:system_time is used for measurements. It is also possible to
%% see that in case of monotonic time usage, though (seen it one time,
%% most likely a bug in vm specific to mac os).
notify_histogram(Metric, _Max, _Units, Val) when Val < 0 ->
    ?log_warning("Ignoring negative histogram value (~p) for ~p",
                 [Val, Metric]),
    ok.

%% It is unsafe to use this function from multiple processes with the same
%% metric
-spec notify_max({metric(), pos_integer(), pos_integer()}, number()) -> ok.
notify_max({Metric, Window, BucketSize}, Val) ->
    Now = erlang:monotonic_time(millisecond),
    notify_moving_window(max, Metric, Window, BucketSize, Now, ?MODULE, Val).

-spec report_prom_stats(
        fun (({atom() | binary(), [label()], gauge_value()}) -> ok),
        fun (({atom() | binary(), [label()], gauge_value()}) -> ok),
        boolean(), undefined | pos_integer()) -> ok.
report_prom_stats(ReportMetricFun, ReportMetaFun, IsHighCard, undefined) ->
    report_prom_stats(ReportMetricFun, ReportMetaFun, IsHighCard);
report_prom_stats(ReportMetricFun, ReportMetaFun, IsHighCard, Timeout) ->
    case async:run_with_timeout(
           fun () ->
                   report_prom_stats(ReportMetricFun, ReportMetaFun, IsHighCard)
           end, Timeout) of
        {ok, Res} -> Res;
        {error, timeout} ->
            ?log_debug("Metrics collection timed out (~p)", [Timeout]),
            {error, timeout}
    end.

-spec report_prom_stats(
        fun (({atom() | binary(), [label()], gauge_value()}) -> ok),
        fun (({atom() | binary(), [label()], gauge_value()}) -> ok),
        boolean()) -> ok.
report_prom_stats(ReportMetricFun, ReportMetaFun, IsHighCard) ->
    Try = fun (Name, F) ->
              try F()
              catch C:E:ST ->
                  ?log_error("~p stats reporting exception: ~p:~p~n~p",
                             [Name, C, E, ST])
              end
          end,
    case IsHighCard of
        true ->
            Try(ns_server, fun () -> report_ns_server_hc_stats(
                                       ReportMetricFun,
                                       ReportMetaFun)
                           end),
            Try(cluster, fun () -> report_cluster_stats(
                                     ReportMetricFun,
                                     ReportMetaFun)
                         end),
            Try(erlang, fun () -> report_erlang_stats(
                                    ReportMetricFun,
                                    ReportMetaFun)
                        end);
        false ->
            Try(ns_server, fun () -> report_ns_server_lc_stats(
                                       ReportMetricFun,
                                       ReportMetaFun)
                           end),
            Try(audit, fun () -> report_audit_stats(
                                   ReportMetricFun,
                                   ReportMetaFun)
                       end),
            Try(system, fun () -> report_system_stats(
                                    ReportMetricFun,
                                    ReportMetaFun)
                        end),
            Try(couchdb, fun () -> report_couchdb_stats(
                                     ReportMetricFun,
                                     ReportMetaFun)
                         end),
            Try(cbauth, fun () -> report_cbauth_stats(
                                    ReportMetricFun,
                                    ReportMetaFun)
                        end)
    end,
    ok.

report_derived_stats(ReportFun, ReportMetaFun) ->
    try report_ns_server_derived_stats(ReportFun, ReportMetaFun)
    catch C:E:ST ->
              ?log_error("Derived stats reporting exception: ~p:~p~n~p",
                         [C, E, ST])
    end.

get_pressure_name_labels(PsiKey) ->
    [Level, PsiKey0] = binary:split(PsiKey, <<"/">>),
    [Resource, PsiKey1] = binary:split(PsiKey0, <<"/">>),
    [SomeOrAll, PsiKey2] = binary:split(PsiKey1, <<"/">>),
    PressureLabels = [{<<"level">>, Level}, {<<"resource">>, Resource},
                      {<<"quantifier">>, SomeOrAll}],
    {Name, Labels} =
        case binary:split(PsiKey2, <<"/">>) of
            [Key] ->
                {Key, PressureLabels};
            [Key, Interval] ->
                {Key, [{<<"interval">>, Interval} | PressureLabels]}
        end,
    {<<"pressure_", Name/binary>>, Labels}.

-ifdef(TEST).

validate_psi(<<"pressure/", PsiKey/binary>>, {EName, ELabels}) ->
    {RName, RLabels} = get_pressure_name_labels(PsiKey),
    ?assertEqual(EName, RName),
    ?assertEqual(lists:sort(ELabels), lists:sort(RLabels)).

psi_test() ->
    PsiKeyAvg = <<"pressure/cgroup/io/some/share_time_stalled/10">>,
    PsiKeyStall = <<"pressure/host/cpu/full/total_stall_time_usec">>,
    AvgExpect = {<<"pressure_share_time_stalled">>,
                 [{<<"interval">>, <<"10">>}, {<<"level">>, <<"cgroup">>},
                  {<<"resource">>, <<"io">>}, {<<"quantifier">>, <<"some">>}]},
    StallExpect = {<<"pressure_total_stall_time_usec">>,
                   [{<<"level">>, <<"host">>}, {<<"resource">>, <<"cpu">>},
                    {<<"quantifier">>, <<"full">>}]},
    validate_psi(PsiKeyAvg, AvgExpect),
    validate_psi(PsiKeyStall, StallExpect).

-endif.

key_to_binary(Key) when is_atom(Key) ->
    atom_to_binary(Key);
key_to_binary(Key) when is_binary(Key) ->
    Key;
key_to_binary(Key) when is_list(Key) ->
    list_to_binary(Key).

report_system_stats(ReportMetricFun, ReportMetaFun) ->
    Stats = gen_server:call(?MODULE, get_stats),
    SystemStats = proplists:get_value("@system", Stats, []),
    lists:foreach(
      fun ({Key, Val}) ->
              KeyBin = key_to_binary(Key),
              {StatName, Labels0} =
                case KeyBin of
                    <<"cpu_host_seconds_total_", Mode/binary>> ->
                        {<<"cpu_host_seconds_total">>,
                         [{<<"mode">>, Mode}]};
                    <<"pressure/", PsiKey/binary>> ->
                        get_pressure_name_labels(PsiKey);
                    <<"cpu_cgroup_seconds_total_", Mode/binary>> ->
                        {<<"cpu_cgroup_seconds_total">>,
                         [{<<"mode">>, Mode}]};
                    _ ->
                        {KeyBin, []}
                end,
              ReportMetaFun([<<"sys_">>, StatName]),
              Labels = Labels0 ++ [{<<"category">>, <<"system">>}],
              ReportMetricFun({<<"sys">>, StatName, Labels, Val})
      end, SystemStats),

    SysProcStats = proplists:get_value("@system-processes", Stats, []),
    lists:foreach(
        fun ({KeyBin, Val}) ->
            [Proc, Name0] = binary:split(KeyBin, <<"/">>),
            {Name, Labels0} =
                case Name0 of
                    <<"cpu_seconds_total_", Mode/binary>> ->
                        {<<"cpu_seconds_total">>,
                         [{<<"mode">>, Mode}]};
                    _ ->
                        {Name0, []}
                end,
            ReportMetaFun([<<"sysproc_">>, Name]),
            Labels = Labels0 ++ [{<<"proc">>, Proc},
                                 {<<"category">>, <<"system-processes">>}],
            ReportMetricFun({<<"sysproc">>, Name, Labels, Val})
        end, SysProcStats),

    DiskStats = proplists:get_value("@system-disks", Stats, []),
    lists:foreach(
      fun({{Disk, Name}, Val}) ->
              {MappedName, MappedValue} =
                  case binary:split(Name, <<"_ms">>) of
                      [Start, <<>>] ->
                          {<<Start/binary, "_seconds">>,
                           to_seconds_bin(Val, millisecond)};
                      _ -> {Name, Val}
                  end,
              ReportMetaFun([<<"sys_disk_">>, MappedName]),
              ReportMetricFun({<<"sys_disk">>, MappedName, [{<<"disk">>, Disk}],
                               MappedValue})
      end,
      DiskStats),
    Mounts = ns_disksup:get_disk_data(),
    ReportMetaFun(<<"sys_disk_usage_ratio">>),
    lists:foreach(
      fun ({Disk, _Size, Usage}) ->
              %% Prometheus recommends using the unit "ratio" with a value range
              %% of 0 - 1, so we divide by 100
              ReportMetricFun({<<"sys_disk_usage_ratio">>,
                               [{<<"disk">>, Disk}], Usage / 100})
      end, Mounts).

report_audit_stats(ReportMetricFun, ReportMetaFun) ->
    {ok, Stats} = ns_audit:stats(),
    AuditQueueLen = proplists:get_value(queue_length, Stats, 0),
    AuditRetries = proplists:get_value(unsuccessful_retries, Stats, 0),
    ReportMetaFun(<<"audit_queue_length">>),
    ReportMetricFun({<<"audit">>, <<"queue_length">>,
                     [{<<"category">>, <<"audit">>}], AuditQueueLen}),
    ReportMetaFun(<<"audit_unsuccessful_retries">>),
    ReportMetricFun({<<"audit">>, <<"unsuccessful_retries">>,
                     [{<<"category">>, <<"audit">>}], AuditRetries}).

report_couchdb_stats(ReportMetricFun, ReportMetaFun) ->
    ThisNodeBuckets = ns_bucket:node_bucket_names_of_type(node(), membase),
    [report_couch_stats(B, ReportMetricFun, ReportMetaFun) ||
     B <- ThisNodeBuckets].

report_couch_stats(Bucket, ReportMetricFun, ReportMetaFun) ->
    Stats = try
                ns_couchdb_api:fetch_raw_stats(Bucket)
            catch
                _:E:ST ->
                    ?log_info("Failed to fetch couch stats:~p~n~p", [E, ST]),
                    []
            end,
    ViewsStats = proplists:get_value(views_per_ddoc_stats, Stats, []),
    SpatialStats = proplists:get_value(spatial_per_ddoc_stats, Stats, []),
    ViewsDiskSize = proplists:get_value(couch_views_actual_disk_size, Stats),

    Labels = [{<<"bucket">>, Bucket}],
    case ViewsDiskSize of
        undefined -> ok;
        _ ->
            ReportMetaFun(couch_views_actual_disk_size),
            ReportMetricFun({couch_views_actual_disk_size, Labels,
                             ViewsDiskSize})
    end,
    lists:foreach(
      fun ({Sig, Disk, Data, Ops}) ->
            L = [{<<"signature">>, Sig} | Labels],
            ReportMetaFun(couch_views_disk_size),
            ReportMetricFun({couch_views_disk_size, L, Disk}),
            ReportMetaFun(couch_views_data_size),
            ReportMetricFun({couch_views_data_size, L, Data}),
            ReportMetaFun(couch_views_ops),
            ReportMetricFun({couch_views_ops, L, Ops})
      end, ViewsStats),
    lists:foreach(
      fun ({Sig, Disk, Data, Ops}) ->
            L = [{<<"signature">>, Sig} | Labels],
            ReportMetaFun(couch_spatial_disk_size),
            ReportMetricFun({couch_spatial_disk_size, L, Disk}),
            ReportMetaFun(couch_spatial_data_size),
            ReportMetricFun({couch_spatial_data_size, L, Data}),
            ReportMetaFun(couch_spatial_ops),
            ReportMetricFun({couch_spatial_ops, L, Ops})
      end, SpatialStats).

report_cbauth_stats(ReportMetricFun, ReportMetaFun) ->
    Stats = menelaus_cbauth:stats(),
    lists:foreach(
        fun ({ServiceName, {<<"cacheStats">>, CacheStatsList}}) ->
            lists:foreach(
                fun ({CacheStats}) ->
                    CacheName = proplists:get_value(<<"name">>,
                                                    CacheStats, undefined),
                    report_cbauth_cache_stats(ReportMetricFun, ReportMetaFun,
                                              ServiceName,
                                              CacheStats, CacheName)
                end, CacheStatsList)
        end, Stats).

report_cbauth_cache_stats(_ReportMetricFun, _ReportMetaFun, ServiceName,
                          _CacheStats, undefined) ->
    ?log_error("Found empty cache name for service ~p. Ignoring the stats.",
               [ServiceName]);
report_cbauth_cache_stats(ReportMetricFun, ReportMetaFun, ServiceName,
                          CacheStats, CacheName) ->
    lists:foreach(
        fun ({Name, ReportingName}) ->
            case proplists:get_value(Name, CacheStats, undefined) of
                undefined ->
                    ?log_error("Expected to find ~p value in the cbauth stats "
                               "report of service ~p, but couldn't find it. "
                               "Ignoring the stats.",
                               [Name, ServiceName]);
                Val ->
                    StatName = [<<CacheName/binary, <<"_">>/binary,
                                  ReportingName/binary>>],
                    FullName = [?METRIC_PREFIX, StatName],
                    ReportMetaFun(FullName),
                    ReportMetricFun({[?METRIC_PREFIX, CacheName],
                                     ReportingName,
                                     [{<<"category">>, <<"cbauth">>},
                                      {<<"service">>, ServiceName}],
                                     Val})
            end
        end,
        [{<<"maxSize">>, <<"max_items">>},
         {<<"size">>, <<"current_items">>},
         {<<"hit">>, <<"hit_total">>},
         {<<"miss">>, <<"miss_total">>}]).

report_ns_server_lc_stats(ReportMetricFun, ReportMetaFun) ->
    lists:foreach(
      fun (Key) ->
          case ets:lookup(?MODULE, Key) of
              [] -> ok;
              [M] -> report_stat(M, ReportMetricFun, ReportMetaFun)
          end
      end, low_cardinality_stats()).

report_ns_server_hc_stats(ReportMetricFun, ReportMetaFun) ->
    ets:foldl(
      fun (M, _) ->
              case lists:member(element(1, M), low_cardinality_stats()) of
                  true -> ok;
                  false ->
                      report_stat(M, ReportMetricFun, ReportMetaFun)
              end,
              ok
      end, [], ?MODULE),
    ok.

convert_to_reported_event(<<"start">>) -> <<"initiated">>;
convert_to_reported_event(<<"success">>) -> <<"completed">>;
convert_to_reported_event(<<"fail">>) -> <<"failed">>;
convert_to_reported_event(<<"stop">>) -> <<"stopped">>;
%% We only want the orchestrator counters which use the above suffixes to
%% 'failover_'. The counters generated by the failover module also start
%% with 'failover_' but also include graceful failovers. Fortunately the
%% trailing portions of the 'failover_' stats don't overlap between the
%% two modules.
convert_to_reported_event(_) -> skip.

%% Report cluster-wide stats (stored in chronicle).
report_cluster_stats(ReportMetricFun, ReportMetaFun) ->
    Counters = ns_cluster:counters(),
    lists:foreach(
      fun ({Key, Val}) ->
              KeyBin = key_to_binary(Key),
              {Event, StatName} =
                case KeyBin of
                    <<"rebalance_", Event0/binary>> ->
                        {convert_to_reported_event(Event0),
                         <<"rebalance_total">>};
                    <<"failover_", Event0/binary>> ->
                        {convert_to_reported_event(Event0),
                         <<"failover_total">>};
                    <<"graceful_failover_", Event0/binary>>  ->
                        {convert_to_reported_event(Event0),
                         <<"graceful_failover_total">>};
                    _ ->
                        {skip, undefined}
                end,
            case Event of
                skip ->
                    ok;
                _ ->
                    ReportMetaFun([?METRIC_PREFIX, StatName]),
                    Label = [{<<"event">>, Event}],
                    ReportMetricFun({<<"cm">>, StatName, Label, Val})
            end
      end, Counters).

%% Delete stats for the specified bucket.
delete_bucket_stats(Bucket) when is_list(Bucket) ->
    ets:foldl(
      fun (M, _) ->
              Key = element(1, M),
              case Key of
                  {h, {Metric, Labels}, _Max, _Units} ->
                      maybe_delete_stat(Bucket, Metric, Key, Labels);
                  {c, {Metric, Labels}} ->
                      maybe_delete_stat(Bucket, Metric, Key, Labels);
                  {g, {Metric, Labels}} ->
                      maybe_delete_stat(Bucket, Metric, Key, Labels);
                  {mw, _F, _Window, {Metric, Labels}} ->
                      maybe_delete_stat(Bucket, Metric, Key, Labels);
                  _ ->
                      ok
              end
      end, [], ?MODULE),
    ok.

maybe_delete_stat(Bucket, Metric, Key, Labels) ->
    case lists:member({<<"bucket">>, list_to_binary(Bucket)}, Labels) of
        true ->
            ?log_debug("Deleting ~p for ~p from ets table",
                       [Metric, Bucket]),
            ets:delete(?MODULE, Key);
        false ->
            ok
    end.

report_erlang_stats(ReportMetricFun, ReportMetaFun) ->
    InterestingErlangStats = [
        port_count,
        port_limit,
        process_count,
        process_limit
    ],
    lists:foreach(
      fun(Stat) ->
              StatName = [<<"erlang_">>, atom_to_binary(Stat)],
              FullName = [?METRIC_PREFIX, StatName],
              ReportMetaFun(FullName),
              ReportMetricFun({FullName,
                               [], erlang:system_info(Stat)})
      end, InterestingErlangStats).

%% Derived stats are those where ns_server has instructed prometheus to
%% do the calculations. The result of this is the stat resides in the local
%% prometheus instance. In order to report the stat in ns_server's REST
%% results we have to query prometheus for the stat and then report it.
report_ns_server_derived_stats(ReportFun, ReportMetaFun) ->
    Settings = prometheus_cfg:settings(),
    Derived = [N || {N, _} <- prometheus_cfg:derived_metrics(ns_server,
                                                             Settings)],
    Query = promQL:format_promql({'or', [promQL:metric(M) || M <- Derived]}),
    Timeout = prometheus:determine_timeout(undefined, Settings,
                                           query_derived_request_timeout),
    case prometheus:query(Query, undefined, Timeout, Settings) of
        {ok, []} ->
            ok;
        {ok, Metrics} ->
            lists:map(
              fun ({[{<<"metric">>, {Props}}, {<<"value">>, [_, Value]}]}) ->
                      report_derived_stats(Props, Value, ReportFun,
                                           ReportMetaFun);
                  ({[{<<"value">>, [_, Value]}, {<<"metric">>, {Props}}]}) ->
                      report_derived_stats(Props, Value, ReportFun,
                                           ReportMetaFun)
              end, Metrics);
        {error, Err} ->
            ?log_error("Failed to get derived ns_server stats: ~p", [Err])
    end.

report_derived_stats(Props, Value, ReportFun, ReportMetaFun) ->
    Name = proplists:get_value(<<"__name__">>, Props),
    Props2 = proplists:delete(<<"__name__">>, Props),
    Props3 = proplists:delete(<<"name">>, Props2),
    ReportMetaFun(Name),
    ReportFun({Name, Props3, Value}).

low_cardinality_stats() ->
    [{c, {<<"rest_request_enters">>, []}},
     {c, {<<"rest_request_leaves">>, []}}].

report_stat({{g, {BinName, Labels}}, {_TS, Value}}, ReportMetricFun,
            ReportMetaFun) ->
    FullName = [?METRIC_PREFIX, BinName],
    ReportMetaFun(FullName),
    ReportMetricFun({FullName, Labels, Value});
report_stat({{c, {BinName, Labels}}, Value}, ReportMetricFun,
           ReportMetaFun) ->
    FullName = [[?METRIC_PREFIX, BinName], <<"_total">>],
    ReportMetaFun(FullName),
    ReportMetricFun({FullName, Labels, Value});
report_stat({{mw, F, Window, {BinName, Labels}}, BucketsQ}, ReportMetricFun,
           ReportMetaFun) ->
    Now = erlang:monotonic_time(millisecond),
    PrunedBucketsQ = prune_buckets(Now - Window, BucketsQ),
    Values = [V || {_, V} <- queue:to_list(PrunedBucketsQ)],
    Value = aggregate_moving_window_buckets(F, Values),
    FullName = [?METRIC_PREFIX, BinName],
    ReportMetaFun(FullName),
    ReportMetricFun({FullName, Labels, Value});
report_stat(Histogram, ReportMetricFun, ReportMetaFun) ->
    [{h, {Name, Labels}, _Max, Units}, Sum, Inf | Buckets] =
        tuple_to_list(Histogram),
    BinName = iolist_to_binary([Name, <<"_seconds">>]),

    ReportMetaFun([?METRIC_PREFIX, BinName]),

    BucketName = [?METRIC_PREFIX, BinName, <<"_bucket">>],
    {_, BucketsTotal} =
        lists:foldl(
          fun (Val, {Le, CurTotal}) ->
              BucketValue = CurTotal + Val,
              LeBin = to_seconds_bin(Le, Units),
              ReportMetricFun({BucketName, [{le, LeBin} | Labels],
                               BucketValue}),
              {Le * 10, BucketValue}
          end, {1, 0}, Buckets),
    Total = BucketsTotal + Inf,
    ReportMetricFun({BucketName, [{le, <<"+Inf">>}| Labels], Total}),
    ReportMetricFun({[?METRIC_PREFIX, BinName, <<"_count">>], Labels, Total}),
    ReportMetricFun({[?METRIC_PREFIX, BinName, <<"_sum">>], Labels,
                     to_seconds_bin(Sum, Units)}).

init([]) ->
    init_stats(),
    increment_counter({request_leaves, rest}, 0),
    increment_counter({request_enters, hibernate}, 0),
    increment_counter({request_leaves, hibernate}, 0),
    increment_counter(log_counter, 0),
    increment_counter(odp_report_failed, 0),
    _ = spawn_link(fun stale_histo_epoch_cleaner/0),

    spawn_ale_stats_collector(),

    cb_stats_info:init_info(),

    {ok, restart_populate_stats_timer(0,
           restart_cleanup_stats_timer(
             restart_process_stats_timer(
               #state{pid_names = grab_pid_names()})))}.

init_stats() ->
    ets:new(?MODULE, [public, named_table, set]),
    %% Deprecated table, will be removed:
    ets:new(ns_server_system_stats, [public, named_table, set]).

handle_call(get_stats, _From, State) ->
    {Stats, NewState} = process_stats(State),
    {reply, Stats, NewState};

%% Can be called from another node. Introduced in 7.0
handle_call({stats_interface, Function, Args}, From, State) ->
    _ = proc_lib:spawn_link(
          fun () ->
              Res = erlang:apply(stats_interface, Function, Args),
              gen_server:reply(From, Res)
          end),
    {noreply, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

%% Can be called from another node. Introduced in 7.0
handle_cast({extract, {From, Ref}, Query, Start, End, Step, Timeout}, State) ->
    Settings = prometheus_cfg:settings(),
    Reply = fun (Res) -> From ! {Ref, Res} end,
    prometheus:query_range_async(Query, Start, End, Step, Timeout,
                                 Settings, Reply),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(process_ns_server_stats, State) ->
    case ets:update_counter(ns_server_system_stats, log_counter,
                            {2, 1, ?ETS_LOG_INTVL, 0}) of
        0 ->
            log_system_stats(os:system_time(millisecond));
        _ ->
            ok
    end,
    update_merger_rates(),
    sample_ns_memcached_queues(),
    {noreply, restart_process_stats_timer(State)};
handle_info(cleanup_ns_server_stats, State) ->
    misc:flush(cleanup_ns_server_stats),
    Now = erlang:monotonic_time(millisecond),

    NumDeleted = ets:select_delete(?MODULE,
                                   [{{{g, '_'}, {'$1', '_'}},
                                     [{'=<', '$1', Now}],
                                     [true]}]),
    NumDeleted > 0
        andalso ?log_debug("Abandoned ~p ns_server stats", [NumDeleted]),
    {noreply, restart_cleanup_stats_timer(State)};
handle_info(populate_ns_server_stats,
            #state{populate_stats_pid = undefined,
                   populate_stats_count = Count} = State) ->
    %% Populating stats can be time-consuming so we spin off a process so
    %% we don't block other work.
    {Pid, MRef} = spawn_opt(
                    fun () ->
                            do_populate_stats(Count)
                    end,
                    [link, monitor]),
    Now = erlang:monotonic_time(millisecond),
    %% Timer gets restarted when process exits.
    {noreply, State#state{populate_stats_pid = Pid,
                          populate_stats_start_time = Now,
                          populate_stats_ref = MRef,
                          populate_stats_count = Count + 1}};
handle_info(populate_ns_server_stats,
            #state{populate_stats_pid = Pid,
                   populate_stats_start_time = StartTime} = State) ->
    Now = erlang:monotonic_time(millisecond),
    ?log_debug("populate_stats process ~p is still running (~p msecs)",
               [Pid, Now - StartTime]),
    %% Still running. Check again in 1 second
    {noreply, restart_populate_stats_timer(1000, State)};
handle_info({'DOWN', MRef, process, Pid, normal},
            #state{populate_stats_ref = MRef,
                   populate_stats_pid = Pid,
                   populate_stats_start_time = StartTime} = State) ->
    %% Adjust interval to account for the amount of time taken
    %% to do the last stats population. The goal is to have stats
    %% populated at as close to the stats interval as possible
    %% even though the amount of time to process a single
    %% population might vary.
    Now = erlang:monotonic_time(millisecond),
    ElapsedTime = Now - StartTime,
    NextInterval =
        case ElapsedTime < ?POPULATE_STATS_INTERVAL of
            true ->
                ?POPULATE_STATS_INTERVAL - ElapsedTime;
            false ->
               ?log_debug("Populating stats took ~p msecs which is "
                          "~p msecs over the desired interval",
                          [ElapsedTime,
                           ElapsedTime - ?POPULATE_STATS_INTERVAL]),
               %% Last took longer than our target interval so kick
               %% off the next immediately.
               0
        end,
    {noreply,
     restart_populate_stats_timer(NextInterval,
                                  State#state{populate_stats_pid = undefined,
                                              populate_stats_ref = undefined})};
handle_info(Info, State) ->
    ?log_warning("Unhandled info: ~p", [Info]),
    {noreply, State}.

%% Gather stats (which could be time consuming) and populate the ets
%% table
do_populate_stats(Count) ->
    case mb_master:master_node() =:= node() of
        true ->
            %% Stats populated only on orchestrator.
            case Count rem ?HEAVYWEIGHT_STATS_SKIP_COUNT =:= 0 of
                true ->
                    %% Heavyweight stat populated less frequently
                    Value = case ns_cluster_membership:is_balanced() of
                                true -> 1;
                                false -> 0
                            end,
                    ns_server_stats:notify_gauge(is_balanced, Value);
                false ->
                    %% Shouldn't be any stats reported here.
                    ok
            end,
            %% Is rebalance running? Only report on the orchestrator node
            %% to be consistent with other rebalance progress stats.
            ns_server_stats:notify_gauge(rebalance_in_progress,
                                         rebalance:running());
        false ->
            %% Shouldn't be any stats reported here.
            ok
    end,

    %% Stats populated on all nodes.
    %% Auto-failover information
    AutoFailoverStats = menelaus_web_auto_failover:get_stats(),
    lists:foreach(
      fun ({Key, Val}) ->
              KeyBin0 = key_to_binary(Key),
              KeyBin = <<"auto_failover_", KeyBin0/binary>>,
              ns_server_stats:notify_gauge(KeyBin, Val)
      end, AutoFailoverStats).

log_system_stats(TS) ->
    NSServerStats = lists:sort(ets:tab2list(ns_server_system_stats)),
    NSCouchDbStats = ns_couchdb_api:fetch_stats(),

    log_stats(TS, "@system", lists:keymerge(1, NSServerStats, NSCouchDbStats)).

process_stats(#state{pid_names = PidNames} = State) ->
    {Counters, Gauges, ProcStats, DiskStats} =
        sigar:get_all(PidNames),
    RetStats = [{"@system", Counters ++ Gauges},
                {"@system-processes", ProcStats},
                {"@system-disks", DiskStats}],
    {RetStats, State}.

increment_counter(Name, By) ->
    try
        do_increment_counter(Name, By)
    catch
        _:_ ->
            ok
    end.

do_increment_counter(Name, By) ->
    ets:insert_new(ns_server_system_stats, {Name, 0}),
    ets:update_counter(ns_server_system_stats, Name, By).

set_counter(Name, Value) ->
    (catch do_set_counter(Name, Value)).

do_set_counter(Name, Value) ->
    case ets:insert_new(ns_server_system_stats, {Name, Value}) of
        false ->
            ets:update_element(ns_server_system_stats, Name, {2, Value});
        true ->
            ok
    end.

get_ns_server_stats() ->
    ets:tab2list(ns_server_system_stats).

%% those constants are used to average config merger rates
%% exponentially. See
%% http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
-define(TEN_SEC_ALPHA, 0.0951625819640405).
-define(MIN_ALPHA, 0.0165285461783825).
-define(FIVE_MIN_ALPHA, 0.0799555853706767).

combine_avg_key(Key, Prefix) ->
    case is_tuple(Key) of
        true ->
            list_to_tuple([Prefix | tuple_to_list(Key)]);
        false ->
            {Prefix, Key}
    end.

update_avgs(Key, Value) ->
    [update_avg(combine_avg_key(Key, Prefix), Value, Alpha)
     || {Prefix, Alpha} <- [{avg_10s, ?TEN_SEC_ALPHA},
                            {avg_1m, ?MIN_ALPHA},
                            {avg_5m, ?FIVE_MIN_ALPHA}]],
    ok.

update_avg(Key, Value, Alpha) ->
    OldValue = case ets:lookup(ns_server_system_stats, Key) of
                   [] ->
                       0;
                   [{_, V}] ->
                       V
               end,
    NewValue = OldValue + (Value - OldValue) * Alpha,
    set_counter(Key, NewValue).

read_counter(Key) ->
    ets:insert_new(ns_server_system_stats, {Key, 0}),
    [{_, V}] = ets:lookup(ns_server_system_stats, Key),
    V.

read_and_dec_counter(Key) ->
    V = read_counter(Key),
    increment_counter(Key, -V),
    V.

update_merger_rates() ->
    SleepTime = read_and_dec_counter(total_config_merger_sleep_time),
    update_avgs(config_merger_sleep_time, SleepTime),

    RunTime = read_and_dec_counter(total_config_merger_run_time),
    update_avgs(config_merger_run_time, RunTime),

    Runs = read_and_dec_counter(total_config_merger_runs),
    update_avgs(config_merger_runs_rate, Runs),

    QL = read_counter(config_merger_queue_len),
    update_avgs(config_merger_queue_len, QL).

just_avg_counter(RawKey, AvgKey) ->
    V = read_and_dec_counter(RawKey),
    update_avgs(AvgKey, V).

just_avg_counter(RawKey) ->
    just_avg_counter(RawKey, RawKey).

sample_ns_memcached_queues() ->
    KnownsServices = case ets:lookup(ns_server_system_stats,
                                     tracked_ns_memcacheds) of
                         [] -> [];
                         [{_, V}] -> V
                     end,
    Registered = [atom_to_list(Name) || Name <- registered()],
    ActualServices = [ServiceName ||
                      ("ns_memcached-" ++ _) = ServiceName <- Registered],
    ets:insert(ns_server_system_stats, {tracked_ns_memcacheds, ActualServices}),
    [begin
         [ets:delete(ns_server_system_stats, {Prefix, S, Stat})
          || Prefix <- [avg_10s, avg_1m, avg_5m]],
         ets:delete(ns_server_system_stats, {S, Stat})
     end
     || S <- KnownsServices -- ActualServices,
        Stat <- [qlen, call_time, calls, calls_rate,
                 long_call_time, long_calls, long_calls_rate,
                 e2e_call_time, e2e_calls, e2e_calls_rate]],
    [begin
         case (catch erlang:process_info(whereis(list_to_atom(S)),
                                         message_queue_len)) of
             {message_queue_len, QL} ->
                 QLenKey = {S, qlen},
                 update_avgs(QLenKey, QL),
                 set_counter(QLenKey, QL);
             _ -> ok
         end,

         just_avg_counter({S, call_time}),
         just_avg_counter({S, calls}, {S, calls_rate}),

         just_avg_counter({S, long_call_time}),
         just_avg_counter({S, long_calls}, {S, long_calls_rate}),

         just_avg_counter({S, e2e_call_time}),
         just_avg_counter({S, e2e_calls}, {S, e2e_calls_rate})
     end || S <- ["unknown" | ActualServices]],
    ok.

get_histo_bin(Value) when Value =< 0 -> 0;
get_histo_bin(Value) when Value > 64000000 -> infinity;
get_histo_bin(Value) when Value > 32000000 -> 64000000;
get_histo_bin(Value) when Value > 16000000 -> 32000000;
get_histo_bin(Value) when Value > 8000000 -> 16000000;
get_histo_bin(Value) when Value > 4000000 -> 8000000;
get_histo_bin(Value) when Value > 2000000 -> 4000000;
get_histo_bin(Value) when Value > 1000000 -> 2000000;
get_histo_bin(Value) ->
    Step = if
               Value < 100 -> 10;
               Value < 1000 -> 100;
               Value < 10000 -> 1000;
               Value =< 1000000 -> 10000
           end,
    ((Value + Step - 1) div Step) * Step.


-define(EPOCH_DURATION, 30).
-define(EPOCH_PRESERVE_COUNT, 5).

add_histo(Type, Value) ->
    BinV = get_histo_bin(Value),
    Epoch = erlang:monotonic_time(second) div ?EPOCH_DURATION,
    K = {h, Type, Epoch, BinV},
    increment_counter(K, 1),
    increment_counter({hg, Type, BinV}, 1).

cleanup_stale_epoch_histos() ->
    NowEpoch = erlang:monotonic_time(second) div ?EPOCH_DURATION,
    FirstStaleEpoch = NowEpoch - ?EPOCH_PRESERVE_COUNT,
    RV = ets:select_delete(ns_server_system_stats,
                           [{{{h, '_', '$1', '_'}, '_'},
                             [{'=<', '$1', {const, FirstStaleEpoch}}],
                             [true]}]),
    RV.

stale_histo_epoch_cleaner() ->
    erlang:register(system_stats_collector_stale_epoch_cleaner, self()),
    stale_histo_epoch_cleaner_loop().

stale_histo_epoch_cleaner_loop() ->
    cleanup_stale_epoch_histos(),
    timer:sleep(?EPOCH_DURATION * ?EPOCH_PRESERVE_COUNT * 1100),
    stale_histo_epoch_cleaner_loop().

spawn_ale_stats_collector() ->
    ns_pubsub:subscribe_link(
      ale_stats_events,
      fun ({{ale_disk_sink, Name}, StatName, Value}) ->
              add_histo({Name, StatName}, Value);
          (_) ->
              ok
      end).

grab_pid_names() ->
    OurPid = list_to_integer(os:getpid()),
    BabysitterPid = ns_server:get_babysitter_pid(),
    CouchdbPid = ns_couchdb_api:get_pid(),

    [{OurPid, <<"ns_server">>},
     {BabysitterPid, <<"babysitter">>},
     {CouchdbPid, <<"couchdb">>}].


-define(WIDTH, 30).

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

format_stats(Stats) ->
    erlang:list_to_binary(
      [case couch_util:to_binary(K0) of
           K -> [K, lists:duplicate(erlang:max(1, ?WIDTH - byte_size(K)), $\s),
                 couch_util:to_binary(V), $\n]
       end || {K0, V} <- lists:sort(Stats)]).

get_histogram_bucket(V, Max) when V > Max -> -1;
get_histogram_bucket(0, _Max) -> 0;
get_histogram_bucket(V, _Max) when is_integer(V) -> ceil(math:log10(V)).

to_seconds_bin(Value, second) -> integer_to_binary(Value);
to_seconds_bin(Value, millisecond) ->
    float_to_binary(Value / 1000, [compact, {decimals, 3}]);
to_seconds_bin(Value, microsecond) ->
    float_to_binary(Value / 1000000, [compact, {decimals, 6}]).

-ifdef(TEST).

to_seconds_bin_test() ->
    ?assertEqual(<<"420">>, to_seconds_bin(420, second)),
    ?assertEqual(<<"0.42">>, to_seconds_bin(420, millisecond)),
    ?assertEqual(<<"0.00042">>, to_seconds_bin(420, microsecond)).

-endif.

normalized_metric(N) when is_atom(N); is_binary(N) ->
    normalized_metric({N, []});
normalized_metric({N, L}) when is_atom(N) ->
    normalized_metric({atom_to_binary(N, latin1), L});
normalized_metric({N, L}) when is_binary(N), is_list(L) ->
    {N, lists:usort(normalized_labels(L))}.

normalized_labels([]) ->
    [];
normalized_labels([{Key, Value} | Rest]) ->
    BinaryKey = normalize_label_element(Key),
    BinaryValue = normalize_label_element(Value),
    [{BinaryKey, BinaryValue} | normalized_labels(Rest)].

normalize_label_element(Item) when is_atom(Item) ->
    atom_to_binary(Item);
normalize_label_element(Item) when is_list(Item) ->
    list_to_binary(Item);
normalize_label_element(Item) ->
    Item.

-ifdef(TEST).

normalized_test() ->
    ?assertEqual({<<"abc">>, []}, normalized_metric(abc)),
    ?assertEqual({<<"abc">>, []}, normalized_metric({abc, []})),
    ?assertEqual({<<"abc">>, [{<<"key">>, <<"value">>}]},
                 normalized_metric({abc, [{key, value}]})),
    ?assertEqual({<<"abc">>, [{<<"key">>, <<"value">>}]},
                 normalized_metric({<<"abc">>, [{<<"key">>, <<"value">>}]})),
    ?assertEqual({<<"abc">>, [{<<"key">>, <<"value">>}]},
                 normalized_metric({abc, [{"key", "value"}]})),
    ?assertEqual({<<"abc">>, [{<<"key">>, 503}]},
                 normalized_metric({abc, [{"key", 503}]})).

-endif.

notify_moving_window(F, Metric, Window, BucketSize, Now, Table, Val) ->
    Key = {mw, F, Window, normalized_metric(Metric)},
    Bucket = (Now div BucketSize) * BucketSize,
    try ets:lookup(Table, Key) of
        [] ->
            Q = queue:from_list([{Bucket, new_moving_window_bucket(F, Val)}]),
            catch ets:insert(Table, {Key, Q});
        [{Key, Q}] ->
            Q2 = prune_buckets(Now - Window, Q),
            Q3 = case queue:out(Q2) of
                     {{value, {Bucket, PrevVal}}, TmpQ} ->
                         NewVal = update_moving_window_bucket(F, PrevVal, Val),
                         queue:in_r({Bucket, NewVal}, TmpQ);
                     {_, _} ->
                         queue:in_r({Bucket, Val}, Q2)
                 end,
            catch ets:insert(Table, {Key, Q3}),
            ok
    catch
        error:badarg -> ok
    end.

new_moving_window_bucket(max, Val) -> Val.

update_moving_window_bucket(max, PrevVal, NewVal) ->
    case NewVal > PrevVal of
        true -> NewVal;
        false -> PrevVal
    end.

aggregate_moving_window_buckets(max, []) -> undefined;
aggregate_moving_window_buckets(max, Values) -> lists:max(Values).

prune_buckets(Deadline, Q) ->
    case queue:out_r(Q) of
        {{value, {TS, _}}, Q2} when TS < Deadline ->
            prune_buckets(Deadline, Q2);
        {_, _}  -> Q
    end.

-ifdef(TEST).

notify_moving_window_test() ->
    Tid = ets:new(test_table, [public, set]),
    try
        Buckets =
            fun () ->
                [{_, Q}] = ets:lookup(Tid, {mw, max, 1000, {<<"m">>, []}}),
                queue:to_list(Q)
            end,
        notify_moving_window(max, m, 1000, 100, 200, Tid, 3),
        notify_moving_window(max, m, 1000, 100, 250, Tid, 4),
        notify_moving_window(max, m, 1000, 100, 250, Tid, 2),
        notify_moving_window(max, m, 1000, 100, 255, Tid, 1),
        ?assertEqual([{200, 4}], Buckets()),
        notify_moving_window(max, m, 1000, 100, 400, Tid, 30),
        notify_moving_window(max, m, 1000, 100, 450, Tid, 40),
        notify_moving_window(max, m, 1000, 100, 455, Tid, 10),
        ?assertEqual([{400, 40}, {200, 4}], Buckets()),
        notify_moving_window(max, m, 1000, 100, 1201, Tid, 30),
        notify_moving_window(max, m, 1000, 100, 1300, Tid, 40),
        notify_moving_window(max, m, 1000, 100, 1350, Tid, 10),
        ?assertEqual([{1300, 40}, {1200, 30}, {400, 40}], Buckets()),
        notify_moving_window(max, m, 1000, 100, 2380, Tid, 11),
        ?assertEqual([{2300, 11}], Buckets())
    after
        ets:delete(Tid)
    end.

-endif.

restart_process_stats_timer(#state{process_stats_timer = undefined} = State) ->
    misc:flush(process_ns_server_stats),
    Ref = erlang:send_after(?get_timeout(process_stats, 1000), self(),
                            process_ns_server_stats),
    State#state{process_stats_timer = Ref};
restart_process_stats_timer(#state{process_stats_timer = Ref} = State) ->
    catch erlang:cancel_timer(Ref),
    restart_process_stats_timer(State#state{process_stats_timer = undefined}).

restart_cleanup_stats_timer(#state{cleanup_stats_timer = undefined} = State) ->
    Ref = erlang:send_after(?get_timeout(cleanup_stats, 30000), self(),
                            cleanup_ns_server_stats),
    State#state{cleanup_stats_timer = Ref};
restart_cleanup_stats_timer(#state{cleanup_stats_timer = Ref} = State)
                                                    when is_reference(Ref) ->
    catch erlang:cancel_timer(Ref),
    restart_cleanup_stats_timer(State#state{cleanup_stats_timer = undefined}).

restart_populate_stats_timer(Interval,
  #state{populate_stats_timer = undefined} = State) ->
    Ref = erlang:send_after(Interval, self(), populate_ns_server_stats),
    State#state{populate_stats_timer = Ref};
restart_populate_stats_timer(Interval,
                             #state{populate_stats_timer = Ref} = State)
  when is_reference(Ref) ->
    catch erlang:cancel_timer(Ref),
    restart_populate_stats_timer(Interval,
                                 State#state{populate_stats_timer = undefined}).

-ifdef(TEST).
cleanup_stats_test_() ->
    State = #state{},
    {foreach,
     fun () ->
             meck:expect(ns_config, get_timeout,
                         fun({?MODULE, cleanup_stats}, _Default) ->
                                 %% Large value to avoid unexpected cleanup
                                 1000000
                         end),
             ets:new(?MODULE, [public, named_table, set])
     end,
     fun  (_) ->
             meck:unload(),
             ets:delete(?MODULE)
     end,
     [{"no-op cleanup",
       fun () ->
               ?assertEqual(0, ets:info(?MODULE, size)),
               handle_info(cleanup_ns_server_stats, State),
               ?assertEqual(0, ets:info(?MODULE, size))
       end},
      {"old value cleanup",
       fun () ->
               ?assertEqual(0, ets:info(?MODULE, size)),
               ns_server_stats:notify_gauge(test, 0, #{expiration_s => 0}),
               ?assertEqual(1, ets:info(?MODULE, size)),
               handle_info(cleanup_ns_server_stats, State),
               ?assertEqual(0, ets:info(?MODULE, size))
       end},
      {"new value no cleanup",
       fun () ->
               ?assertEqual(0, ets:info(?MODULE, size)),
               ns_server_stats:notify_gauge(test, 0, #{expiration_s => 1000}),
               ?assertEqual(1, ets:info(?MODULE, size)),
               handle_info(cleanup_ns_server_stats, State),
               ?assertEqual(1, ets:info(?MODULE, size))
       end},
      {"no expiration no cleanup",
       fun () ->
               ?assertEqual(0, ets:info(?MODULE, size)),
               ns_server_stats:notify_gauge(test, 0,
                                            #{expiration_s => infinity}),
               ?assertEqual(1, ets:info(?MODULE, size)),
               handle_info(cleanup_ns_server_stats, State),
               ?assertEqual(1, ets:info(?MODULE, size))
       end}]}.

%% Add each type of stat to the ns_server_stats table, delete them, and
%% verify they are gone.
stats_deletion_test() ->
    ets:new(?MODULE, [public, named_table, set]),
    Bucket = "testBucket",
    Bucket2 = "testBucket2",

    %% This group "resolves" to the same stat (same ets table key).
    ns_server_stats:notify_histogram(
      {<<"test_histogram">>, [{bucket, Bucket}]}, rand:uniform(1000)),
    ns_server_stats:notify_histogram(
      {test_histogram, [{bucket, Bucket}]}, rand:uniform(1000)),
    ns_server_stats:notify_histogram(
      {<<"test_histogram">>, [{<<"bucket">>, list_to_binary(Bucket)}]},
      rand:uniform(1000)),
    ns_server_stats:notify_histogram(
      {test_histogram, [{<<"bucket">>, list_to_binary(Bucket)}]},
      rand:uniform(1000)),
    ns_server_stats:notify_histogram(
      {<<"test_histogram">>, [{"bucket", Bucket}]},
      rand:uniform(1000)),
    ns_server_stats:notify_histogram(
      {test_histogram, [{"bucket", Bucket}]},
      rand:uniform(1000)),
    ns_server_stats:notify_histogram(
      {<<"test_histogram">>, [{bucket, list_to_atom(Bucket)}]},
      rand:uniform(1000)),
    ns_server_stats:notify_histogram(
      {test_histogram, [{bucket, list_to_atom(Bucket)}]},
      rand:uniform(1000)),

    ns_server_stats:notify_histogram(
      {<<"test_histogram">>, [{bucket, Bucket2 }]}, rand:uniform(1000)),
    ns_server_stats:notify_counter(
      {<<"test_counter">>, [{bucket, Bucket}]}),
    ns_server_stats:notify_counter(
      {<<"test_counter">>, [{bucket, Bucket2}]}),
    ns_server_stats:notify_gauge(
      {<<"test_gauge">>, [{bucket, Bucket}]}, rand:uniform(100)),
    ns_server_stats:notify_gauge(
      {<<"test_gauge">>, [{bucket, Bucket2}]}, rand:uniform(100)),
    ns_server_stats:notify_max(
      {{<<"test_window">>, [{bucket, Bucket}]}, 600, 10}, rand:uniform(1000)),
    ns_server_stats:notify_max(
      {{<<"test_window">>, [{bucket, Bucket2}]}, 600, 10}, rand:uniform(1000)),

    ?assertEqual(8, ets:info(?MODULE, size)),
    ns_server_stats:delete_bucket_stats(Bucket),
    ?assertEqual(4, ets:info(?MODULE, size)),
    ns_server_stats:delete_bucket_stats(Bucket2),
    ?assertEqual(0, ets:info(?MODULE, size)),

    ets:delete(?MODULE).

-endif.
